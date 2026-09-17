# The entry-type semantics artifacts don't match each other or the current prompt

**Status:** CLOSED 2026-09-17 — embeddings and report regenerated from the current definitions and a fresh census. The definitions were kept as they are, by decision. Kept for the decision trail.

## Resolution

**Decision:** the 119 definitions are fine as they are — 110 generated under prompt v1, plus the 9 manual overrides. They were not regenerated.

**What was done:**
1. **Archived** the July census, embeddings and report, with their original dates, to
   `data/archive/2026-07-24_type_semantics/`.
2. **Fresh census** from the current corpus (`python -m modules.kg.census`):
   - 23,882 entries, 19,379 with an entry type;
   - 119 types, 580 pairs;
   - was 22,915 entries and 572 pairs;
   - same filter (min pair count 3).
3. **Re-embedded** all 119 with `qwen3-embedding:0.6b`
   (digest `ac6da0dfba84…`, 1024 dims). The file now records the digest,
   dimension, prompt version (`1`) and a text hash per slug.
4. **Re-analyzed.** The report now carries the embedding provenance and
   a coverage section, which is empty: every census type has a vector and
   every vector has a count.

**What the cause turned out to be, confirmed:**
- For the 110 generated definitions, the July and September vectors are
  identical (cosine ≥ 0.9992): same weights, same text.
- Only the 9 manual overrides differ (cosine 0.57–0.84). The July
  embeddings were computed from their text before it was written by hand.
- The "different weights" explanation is ruled out.

**Effect on the report:**
- Substitution candidates went from 54 to 8, 7 of them kept. Nearly all
  of the lost pairs involve the overridden types (character, song,
  reaction, product, film, campaign, artist, tv-show). The generic v1
  definitions had made those types look alike; the overrides were written
  to fix exactly that.
- Complementary pairs went from 4 to 14, all 4 kept.
- Top-5 neighbours per type are unchanged for 27 of 119 types; the mean
  overlap is 3.55 of 5.

**Effect on the curated taxonomy** (`entry_type_taxonomy.yaml`, not edited):
- No decision flips.
- The 13 encoded edges keep their co-occurrence and PMI support; counts
  grew slightly with the corpus, and PMI is unchanged to one decimal.
- Cosine changed only where a manual override is involved:
  - `song → music` (encoded, semantic-only): 0.78 → 0.62, still a close
    pair;
  - `cartoon → tv-show` (demoted): 0.68 → 0.53, which supports the
    demotion;
  - `song → album` (do not encode): 0.51 → 0.59.

**Still true, for later:**
- **Prompt mismatch.** `PROMPT_VERSION` in code is `2` and the file is
  `1`. `describe` warns when the file is complete, and refuses to add v2
  definitions to it if a new entry type appears. Decide then: regenerate
  with `--force`, or restore the v1 prompt.
- **Filler text.** Five generated definitions still use the filler the v2
  prompt forbids ("gained significant popularity…"): comedian, manga,
  book, historical-figure, religion. They could be hand-overridden like
  the other 9 if they ever matter.
- **ollama-ui unreliability.** It dropped inference requests after exactly
  50 s for about 20 minutes during this work, while GPU-contended. Embed
  with `--batch-size 8` and `OPENWEBUI_MAX_ATTEMPTS=6` on a busy day; see
  the note in `openwebui_client.py`.

---

*Original write-up:*

## What

The three files behind "approach 1" of the taxonomy work (definitions →
embeddings → report) are out of sync with each other and with the code:

1. **The definitions are prompt v1, the code's prompt is v2.**
   `data/kg_type_definitions.json` has `"prompt_version": "1"`, but
   `PROMPT_VERSION` in `dags/modules/kg/semantics.py` is `"2"`. The v2 prompt
   adds the "don't use generic filler like 'gained significant popularity'"
   guidance. So 110 of the 119 definitions came from the older prompt the
   code no longer uses; the other 9 are manual overrides.

2. **The embeddings were not computed from the current definitions.**
   `data/kg_type_embeddings.json` dates from 2026-07-24;
   `data/kg_type_definitions.json` was last modified 2026-09-09. For the 9
   manual overrides, whose text is identical in both old and new files, a
   fresh embedding with the same model (`qwen3-embedding:0.6b`) has cosine
   **0.57–0.84** with the stored vector.

   The server was shown to be deterministic and batch-invariant for the same
   text: cosine ≥ 0.9998 alone, in batches of 3, 6 and 32, and on repeat. So
   the stored vectors embed different text, most likely the definitions as
   they were before the manual overrides were written. They could also come
   from different weights: the old file recorded only a model name, so there
   is no way to prove which.

3. **So `data/kg_type_semantics_report.json` (2026-07-24) reflects
   neither the current definitions nor the current prompt.** Its
   neighbours, clusters, substitution candidates and complementary pairs are
   evidence for a version of the definitions that no longer exists. That
   report is one of the approaches the curated taxonomy
   (`dags/kg_config/entry_type_taxonomy.yaml`) cites as supporting evidence.

## Why it matters

If the paper or the taxonomy YAML cites the semantic-similarity evidence,
that evidence isn't reproducible from the files in `data/`:
- re-running `analyze` gives the same report only because it reads the same
  stale vectors;
- re-running `embed` on the current definitions gives different vectors,
  hence a different report.

The rewritten module now makes this state visible instead of silent:
- definitions record prompt version + model digest;
- embeddings record per-slug text hashes + model digest + dimension;
- a resume onto a stale-prompt file is refused;
- vectors are reused only when their text hash matches.

But the existing files predate all of that.

## TODO

- [ ] Decide whether the v2 prompt is the one you want (read the two
      `SYSTEM_PROMPT` versions in git history if unsure). If yes:
      ```bash
      docker compose exec airflow-worker sh -c 'cd /opt/airflow/dags && \
        python -m modules.kg.semantics describe --force \
          --census /opt/airflow/data/kg_census_entry_type.json \
          --out /opt/airflow/data/kg_type_definitions.json'
      ```
      About 110 × ~20 s ≈ 35–40 min of shared GPU on ollama-ui
      (mistral-small3.2:24b). The 9 manual overrides are kept. Back up the
      current file first; it's the only copy of the v1 text.
- [ ] Re-embed (`embed`, no `--force` needed: old vectors have no text hashes,
      so all 119 are re-embedded — a few seconds on the 0.6B model).
- [ ] Re-run `analyze`, then compare the new report's substitution candidates
      and complementary pairs with the ones the taxonomy YAML's rationales
      cite. Any taxonomy decision that leaned on a pair that no longer shows
      up deserves a second look.
- [ ] If you'd rather keep v1: set `PROMPT_VERSION = "1"` and restore the v1
      `SYSTEM_PROMPT` in `semantics.py` so code and data agree. Either way,
      re-embed and re-analyze, because point 2 holds regardless of prompt.
