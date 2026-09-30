# Three parser fields are structurally empty or wrong — the KG now faithfully reproduces the gap

**Status:** closed 2026-09-30 — parser 1.7.0: aliases read from the About's bold names, `scraped_at` passed through from the scrape, `template_image_url` dropped. Corpus re-parsed; the KG picks it up at its next build. See "Resolution" below.

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

- [x] **aliases:** the sidebar row does not exist — see Resolution.
- [x] **scraped_at:** `fetched_at` was never passed in — fixed at the source.
- [x] **template_image_url:** KYM has no separate template image — dropped.
- [x] Parser version bumped (1.7.0) and the corpus re-parsed with
      `trigger_entities=false`. No KG code change was needed beyond
      `build.py` no longer reading the dropped field; the KG's staleness
      gate sees the new parser version and rebuilds at the next `kym_kg`
      run (not triggered: publishing waits for Gabi).

## Resolution (2026-09-30)

Measured on every stored page (23,879 of the 23,882 entries), then fixed
in `modules/kym_parse.py` (parser 1.7.0):

1. **aliases — KYM has no alias field.** The sidebar's labels across the
   whole corpus are Status, Year, Origin, Tags (every page), Type (19,376),
   Additional References (6,979), Region (5,435), Badges (2,173) and
   Related Discussion (122). No "Also Known As" or "aka" row exists, so the
   old lookup could never match. KYM names alternatives the way Wikipedia
   does: in **bold** at the start of the About — "**Distracted
   Boyfriend**, also known as **Man Looking at Other Woman**, is …". Of
   the 21,402 pages whose About lead has a bold name, 21,377 open with it.
   `_aliases()` takes the run of bold names from the lead's start, each
   joined to the one before by a naming word ("also known as", "or",
   "a.k.a.", "real name", "spelled", a comma or parenthesis, …), stops at
   the first join that is not one (", continued" is the rest of a
   catchphrase; "is a" starts the sentence proper), and leaves out the
   title itself. Two bold runs touching (a name the markup split, "Don't F"
   + "k With Cats") and a bold that swallowed unrendered markdown (an odd
   number of `*`) end the run.
   - Result: aliases on 5,770 pages (24%), 8,101 names; 1-2 on most pages,
     at most 5+ on 27.
   - Read on a random 100 pages: 99 right on the first pass; the one error
     (a bold that swallowed "also known as *Cat Circle Of Life") and two
     trivial "The X" duplicates of the title led to the `*` and leading-"The"
     rules; re-run, the 101 pages that changed were all those cases or new
     "previously known as" names.
   - Not taken: alternative names KYM did not bold ("also known as "Adolf
     Hipster""). The same words also introduce OTHER entities' names in the
     lead ("… Cacia Kersey, also known as Acacia Clark"), so reading
     unbolded names would trade precision for recall. 386 of the 2,655
     leads with such a phrase in their first 250 characters get no alias
     for this reason.
2. **scraped_at — never passed in.** `dom_store.iter_html_for` did not
   project `fetched_at`, so the parse DAG called `parse_entry(html,
   url=url)` and `scraped_at` was always null. It now yields the stored
   page's `fetched_at` (present on 100% of pages) and the DAG passes it.
   `scraped_at` is when the page the entry was parsed from was fetched; KYM's
   own dates stay `kym_added` / `kym_last_updated`. `mk:scrapedAt` now says
   so in `memeatlas.ttl`.
3. **template_image_url — KYM has no separate template image.** The field
   comes from IMKG's scraper, which took the header photo's link
   (`header/a[contains(@class,"photo")]/@href`). In IMKG's time that was
   the original-size file while `og:image` was a resized copy; today the
   header photo IS `og:image` on 23,879 of 23,879 pages. The field is gone
   from the model (`KYMEntryScrape`), the parser and `build.py` (which
   already merged the two into one image node, so the graph is unchanged),
   and the re-parse `$unset`s it from stored docs
   (`parse_store.RETIRED_FIELDS`). A template image in the imgflip sense is
   the 6.4.0 template layer's job (gap 10).

The re-parse changes nothing the derived layers key on: events are keyed
on each section's text and positions, entity links on title, tags and
About — none of which the three fixes touch (checked below), so neither
the running event re-extraction nor the entity layer re-queues anything.

**The re-parse** (`kym_parse`, 2026-09-30 15:20-15:31Z, `trigger_entities=false`):

- 23,882 of 23,882 entries at parser 1.7.0; 0 new parse failures. The 621
  dead letters are the same photo, video and editorial pages that have
  failed at every parser version (4-8 attempts each), none for the first
  time.
- `aliases` on 5,773 entries (8,103 names); `scraped_at` on 23,882 (100%);
  `template_image_url` left on none.
- Staleness keys recomputed from the re-parsed entries: 300 of 300 sampled
  event units and 300 of 300 sampled entity frames unchanged, so no event
  or link is redone.
