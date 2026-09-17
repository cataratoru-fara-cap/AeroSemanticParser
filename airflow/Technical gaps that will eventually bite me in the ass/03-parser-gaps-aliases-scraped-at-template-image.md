# Three parser fields are structurally empty or wrong — the KG now faithfully reproduces the gap

**Status:** open — parser fix needed; KG modeling is already correct and will pick it up automatically once fixed.

## What

Confirmed against the full 23,882-entry corpus and against the parser source
(`airflow/dags/modules/kym_parse.py`):

1. **`aliases` is 0% populated across the whole corpus.** The parser looks
   for a sidebar row labeled "also known as" or "aka"
   (`dd_text("also known as") or dd_text("aka")` at
   `kym_parse.py:452`) and splits it on commas. Either that label never
   appears on any scraped page in this corpus, or the selector/label text is
   wrong for how KYM actually renders it.

2. **`scraped_at` is 0% populated across the whole corpus.** It's set from
   `_timestamps(soup)`'s `added` value at parse time
   (`kym_parse.py:462`, `"scraped_at": fetched_at`) — worth checking whether
   `fetched_at` itself is ever being passed a real value from the scrape
   layer, or whether it's silently `None` all the way through.

3. **`template_image_url` is *always* a byte-for-byte copy of `og_image`.**
   This isn't a data coincidence — it's in the code:
   ```python
   # kym_parse.py:463-478
   og_image = meta.get("og:image")
   return KYMEntryScrape.model_validate({
       ...
       "template_image_url": og_image,
       "og_image": og_image,
       ...
   })
   ```
   Both fields are assigned the *same variable*. Whatever `template_image_url`
   was originally meant to capture (presumably KYM's dedicated "template
   image" field, distinct from the Open Graph preview image) is never
   actually extracted — the parser just duplicates `og_image` into it.

## Why it matters

The KG (`modules/kg/build.py`) already handles this data faithfully — e.g. it
dedupes `og_image`/`template_image_url` into one image node
(`dict.fromkeys(...)` at `build.py:305`) rather than creating two identical
nodes, so nothing downstream is *broken* by these gaps. But:
- `aliases` maps to `skos:altLabel` in RDF — an entire IMKG-alignable relation
  that's currently vacuous for 100% of the graph.
- `scraped_at` maps to `mk:scrapedAt`, part of the provenance story (when was
  this actually fetched vs. when was it added to KYM) — currently useless for
  telling scrape recency apart from KYM's own edit history.
- `template_image_url` gives zero information beyond what `og_image` already
  gives — it's dead weight in every entry doc and in the graph.

## TODO

- [ ] **aliases:** Check a real KYM page's HTML for how "Also known as" is
      actually rendered (label text, casing, whether it's under a different
      `dt`/`dd` structure than other sidebar fields) and fix the selector in
      `dd_text` lookup or the label list itself.
- [ ] **scraped_at:** Trace `fetched_at` back through the scrape layer
      (`kym_scrape_dag.py` / wherever pages are fetched) to confirm whether a
      real timestamp is ever passed in, or whether this parameter is
      structurally always `None`. Fix at the source.
- [ ] **template_image_url:** Find KYM's actual template-image markup (if one
      exists distinct from `og:image` — check the page's own image gallery /
      "Template" section HTML) and extract it properly. If KYM genuinely has
      no separate template image concept, consider dropping the field
      entirely rather than keeping a duplicate — cheaper than carrying dead
      weight in every store forever.
- [ ] Once the parser is fixed, no KG code changes are needed — `build.py`
      already carries whatever these fields hold. Just bump
      `parser_version`, trigger `kym_kg`, and the staleness gate will pick up
      the new parser_version stamp and rebuild automatically.
