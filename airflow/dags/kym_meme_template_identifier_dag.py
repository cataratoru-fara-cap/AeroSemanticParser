"""
Airflow DAG: KnowYourMeme Template Identifier (Hybrid BS4 + LLM fallback)
=======================================================================
- Scans recently scraped memes
- Uses BeautifulSoup rules for known templates
- Marks unknown ones for LLM processing in a later DAG
"""

from __future__ import annotations

import logging
import requests
from datetime import datetime, timedelta
from typing import Optional, List, Dict

from pydantic import BaseModel
from bs4 import BeautifulSoup

from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "gabi",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

KYM_CONN_ID = "postgres_kym"
KYM_TABLE = "memes"


# ─── Schema Extensions ───────────────────────────────────────────────────────

class TemplateRecord(BaseModel):
    """Record for template identification result."""
    url: str
    template_name: Optional[str] = None
    is_known: bool = False
    confidence: float = 0.0
    notes: Optional[str] = None


# Known templates with BS4 extraction rules
KNOWN_TEMPLATES = {
    "distracted_boyfriend": {
        "name": "Distracted Boyfriend",
        "selectors": ["Distracted Boyfriend", "distracted boyfriend"],
        "extra_fields": ["top_text", "bottom_text"]  # example
    },
    "drake": {
        "name": "Drake Hotline Bling",
        "selectors": ["Drake", "Hotline Bling"],
    },
    "expanding_brain": {
        "name": "Expanding Brain",
        "selectors": ["Expanding Brain"],
    },
    "change_my_mind": {
        "name": "Change My Mind",
        "selectors": ["Change My Mind"],
    },
    # Add more as you discover them
}

# ─── DAG ─────────────────────────────────────────────────────────────────────

@dag(
    dag_id="kym_meme_template_identifier",
    description="Hybrid template detection: BS4 for known + flag unknown for LLM",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["scraping", "memes", "kym", "template"],
)
def kym_template_identifier_dag():

    @task()
    def get_recent_memes() -> List[Dict]:
        """Fetch memes scraped after cutoff date that haven't been template-processed yet."""
        cutoff_days = int(Variable.get("KYM_TEMPLATE_CUTOFF_DAYS", default_var=7))
        cutoff = datetime.now() - timedelta(days=cutoff_days)

        hook = PostgresHook(postgres_conn_id=KYM_CONN_ID)
        
        sql = f"""
            SELECT url, name, about, tags, type 
            FROM {KYM_TABLE}
            WHERE scraped_at >= %s
              AND (template_name IS NULL OR template_name = '')
            ORDER BY scraped_at DESC
            LIMIT %s
        """
        limit = int(Variable.get("KYM_TEMPLATE_BATCH_SIZE", default_var=200))
        
        records = hook.get_records(sql, parameters=(cutoff, limit))
        log.info(f"Found {len(records)} recent memes without template info")
        return [dict(zip([col[0] for col in hook.get_connection().cursor().description], row)) 
                for row in records]  # better column mapping

    @task()
    def identify_templates(memes: List[Dict]) -> List[Dict]:
        """Hybrid identification: BS4 for known templates, flag others."""
        headers = {"User-Agent": "Mozilla/5.0 (compatible; KYMTemplateIdentifier/1.0; research)"}
        results: List[Dict] = []

        for meme in memes:
            url = meme["url"]
            try:
                resp = requests.get(url, headers=headers, timeout=15)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "html.parser")
                
                page_text = soup.get_text().lower()
                title = (meme.get("name") or "").lower()
                about = (meme.get("about") or "").lower()
                tags = [t.lower() for t in meme.get("tags", [])]

                template_match = None
                confidence = 0.0

                # Try known templates
                for key, info in KNOWN_TEMPLATES.items():
                    for selector in info["selectors"]:
                        selector_lower = selector.lower()
                        if (selector_lower in page_text or 
                            selector_lower in title or 
                            selector_lower in about or 
                            any(selector_lower in tag for tag in tags)):
                            
                            template_match = info["name"]
                            confidence = 0.85 if selector_lower in title else 0.65
                            break
                    if template_match:
                        break

                if template_match:
                    # Known template → extract with BS4
                    result = {
                        "url": url,
                        "template_name": template_match,
                        "is_known": True,
                        "confidence": confidence,
                        "notes": "Detected via known template rules"
                    }
                    log.info(f"✓ Known template: {template_match} → {url}")
                else:
                    # Unknown → flag for LLM DAG
                    result = {
                        "url": url,
                        "template_name": None,
                        "is_known": False,
                        "confidence": 0.0,
                        "notes": "Unknown template - queued for LLM"
                    }
                    log.info(f"⚠ Unknown template → {url}")

                results.append(result)

            except Exception as exc:
                log.warning(f"Failed to process {url}: {exc}")
                results.append({
                    "url": url,
                    "template_name": None,
                    "is_known": False,
                    "confidence": 0.0,
                    "notes": f"Processing error: {str(exc)}"
                })

        return results

    @task.sql(conn_id=KYM_CONN_ID)
    def update_template_info(records: List[Dict]):
        """Bulk update template information using jsonb_to_recordset."""
        sql = """
        WITH source AS (
            SELECT *
            FROM jsonb_to_recordset(%(records)s::jsonb) AS x(
                url            TEXT,
                template_name  TEXT,
                is_known       BOOLEAN,
                confidence     FLOAT,
                notes          TEXT
            )
        )
        UPDATE {{ params.table }} m
        SET 
            template_name     = COALESCE(s.template_name, m.template_name),
            template_confidence = s.confidence,
            template_notes    = s.notes,
            template_processed_at = NOW(),
            is_template_known = s.is_known
        FROM source s
        WHERE m.url = s.url;
        """
        return {"updated": len(records)}

    # ── Optional: Count stats ───────────────────────────────────────────────
    @task()
    def log_summary(records: List[Dict]):
        known = sum(1 for r in records if r.get("is_known"))
        unknown = len(records) - known
        log.info(f"Template identification summary: {known} known | {unknown} unknown")
        return {"known": known, "unknown": unknown, "total": len(records)}

    # ── Wire the DAG ─────────────────────────────────────────────────────────
    recent_memes = get_recent_memes()
    identified = identify_templates(recent_memes)
    update = update_template_info(identified)
    summary = log_summary(identified)

    # Set dependencies
    recent_memes >> identified >> [update, summary]


# Create DAG instance
dag_instance = kym_template_identifier_dag()