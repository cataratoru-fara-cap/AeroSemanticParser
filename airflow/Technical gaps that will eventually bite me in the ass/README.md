# Technical gaps that will eventually bite me in the ass

Things found true and confirmed while doing other work (mostly the IMKG-extension
KG remodel, 2026-09-17), deliberately not fixed at the time because they were
out of scope for what was being asked. Each file is one gap: what it is, why
it's true, why it matters, and what fixing it looks like. Not urgent by
definition — if it were, it would have been fixed instead of noted — but real,
and each one gets worse the longer it sits.

- [01-mk-namespace-needs-agreement.md](01-mk-namespace-needs-agreement.md) —
  the MemeAtlas RDF namespace sits under IMKG's own domain without sign-off.
- ~~[02-category-class-casing-unverified.md](02-category-class-casing-unverified.md)~~ **(closed 2026-09-30)** —
  `kym:<Category>` IRIs were a guess at IMKG's casing; verified byte-equal
  from IMKG's own scraper and mapping, and against every stored page.
- ~~[03-parser-gaps-aliases-scraped-at-template-image.md](03-parser-gaps-aliases-scraped-at-template-image.md)~~ **(closed 2026-09-30)** —
  three fields the parser wrote were structurally wrong or always empty;
  parser 1.7.0 reads aliases from the About's bold names, passes
  `scraped_at` through, and drops `template_image_url` (always `og:image`).
- ~~[04-fuseki-disk-growth-tdb2-compaction.md](04-fuseki-disk-growth-tdb2-compaction.md)~~ **(closed 2026-09-30)** —
  TDB2 never reclaimed space when a build graph was replaced (it had reached
  36 GB for 12M live triples); `kym_kg` now compacts after every prune
  (first run: 36.0 -> 3.8 GB), and the dashboard shows the store's size.
- ~~[05-postgres-weak-password-hardcoded.md](05-postgres-weak-password-hardcoded.md)~~ **(closed 2026-09-17)** —
  `POSTGRES_PASSWORD=airflow`, and it's hardcoded a second time, so editing
  `.env` alone won't even fix it.
- ~~[06-type-semantics-artifacts-are-stale.md](06-type-semantics-artifacts-are-stale.md)~~ **(closed 2026-09-17)** —
  the definitions are prompt v1 under a v2 prompt, and the stored embeddings and
  semantic-similarity report weren't computed from the current definitions.
- ~~[07-tag-cooccurs-has-no-rdf-resource.md](07-tag-cooccurs-has-no-rdf-resource.md)~~ **(closed 2026-09-18, superseded)** —
  entry_type's `coOccursWith` was removed entirely as needless statistical noise,
  so the tag-vs-entry_type RDF asymmetry this described no longer exists.

- [08-event-layer-is-llm-derived.md](08-event-layer-is-llm-derived.md) —
  every `mk:Event` is a language model's reading of one sentence and none has
  been reviewed; grounding stops invented values, not wrong readings. Also
  records why 2.0.0's "discard anything not verbatim" rule quietly produced
  wrong dates, which is worth reading before writing the next such rule.

- [09-entity-layer-needs-curation.md](09-entity-layer-needs-curation.md) —
  **the planned next task**: the 6.1.0 entity layer links every entity it
  recognises (t-shirt, mug, field, hair …), and two thirds of the About
  links are incidental common nouns; find a method to keep only the ones
  relevant to the media frame. Everything curation needs is already stored.

- [10-imgflip-template-layer.md](10-imgflip-template-layer.md) — the 6.4.0
  template layer reads an outside site (imgflip, politely, never its
  internal API), decides "same picture" with a thumbnail hash that still
  lets crops through as separate templates, and relies on a vision model's
  reading for what a template shows. Measured at ~0.95 precision on 100
  frames and 0.787 gold recall@10; the vision model's spike and 200-template
  pilot were read and fixed; no pool cap (Gabi). Throughput and its
  mitigations are in the note.

- [11-entities-without-wikidata-item.md](11-entities-without-wikidata-item.md) —
  every entity in the graph is a Wikidata item, so whatever has none is
  left out: 22,614 names in frame text and 4,418 named regions in template
  images (a fifth of them), because the lexicon holds only items with a
  Wikipedia article or the linker did not commit. In RDF an item is only a
  label: no type, no description.

- [12-nsfw-placeholder-image-is-a-node.md](12-nsfw-placeholder-image-is-a-node.md) —
  KYM's NSFW cover image stands in for 5,211 section images in 2,553 frames
  and is cited by 3,125 events, making it a PageRank hub. The real URL is in
  the stored page, base64-encoded in `data-nsfw-src`: a parser fix and an
  offline re-parse, no scraping.

- [13-mongo-schemas-are-hard-to-read.md](13-mongo-schemas-are-hard-to-read.md) —
  raised by Riccardo: the collections grew one stage at a time and are hard
  to read by hand: hash ids, packed-string keys, cryptic names, three time
  formats, JSON stored as a string, one curation decision recorded four
  ways. The fix is a staged rewrite of the store layer.

- ~~[14-one-entry-two-addresses.md](14-one-entry-two-addresses.md)~~ **(closed 2026-10-05)** —
  KYM moves an entry (into its sensitive section and back, or to a new
  name) and the old address keeps answering, so 830 entries were held
  twice, 813 of them as two frames (Doge among them). `kym_scrape` now
  keeps the address the entry's newest page gives, `kym_parse` drops the
  others, and the graph sends their links to the kept one: 815 entries
  retired, 24,291 → 23,477.

Update the status line at the top of a file when a gap is closed; leave the
file (don't delete it) so the decision trail survives.
