# The Mongo collections are hard for a person to read

**Status:** open (2026-10-02). Raised by Riccardo: some collections' schemas
are odd and not intuitive for a human reader. Not urgent, because the
pipeline reads them correctly, but the fix is a rewrite of the store layer,
and it gets more expensive with every collection added.

## What it is

Each collection was shaped by the stage that writes it, one at a time, for
that stage's convenience. Nothing is wrong with the data. What makes the
collections hard to read or query by hand, checked on the live database
(2026-10-02):

1. **Ids are hashes.** Every collection keyed by a page (`urls`, `doms`,
   `entries`, `entities`, `entity_curation`, `frame_templates`,
   `parse_failures`) uses the SHA-1 of the frame URL as `_id`, e.g.
   `003c8978cb72c94b67579eef540707314ee82553`. It is consistent, but no one
   can tell which meme a document is without looking up its URL. That URL
   sits under `url` in some collections and `frame_url` in others, and a
   second copy of the id sits under `entry_id` in `entities` and `events`.
2. **Ids are packed strings.** Meaning is joined with separators and has to
   be split by eye:
   - `events`: `<sha1>:spread`;
   - `kg_nodes`: `<build_id>|<node id>`;
   - `kg_edges`: `<build_id>|<src>|<type>|<dst>`;
   - `run_summaries`: `templates:manual__2026-10-02T07:00:10…`;
   - a curation decision's `key` is `title|-1|50|57|Q623059`: field,
     tag index (-1 when not a tag), start, end and item, by position.
3. **Id types differ.** The imgflip collections (`imgflip_templates`,
   `template_entities`, `template_entity_failures`) are keyed by imgflip's
   integer id; everything else by strings.
4. **Names that need the code to decode:**
   - `frame_templates.selected[]` holds `R`, `s_text`, `s_vis`, `s_rank`,
     `mmr_rank` and `members`;
   - `imgflip_templates` has `key` (`"dnn"`) and `leader` (the template
     standing for a group of near-duplicates);
   - `urls.Confirmed` is the one capitalised field;
   - the same kind of stamp is `extraction_version` in `events` and
     `extractor_version` in `template_entities`.
5. **Three time formats:**
   - BSON datetimes (`parsed_at`, `fetched_at`, …);
   - ISO strings (`urls.last_scraped`, and `urls.lastmod` with KYM's own
     offset);
   - epoch integers (`entries.kym_added`, `entries.kym_last_updated`).
6. **JSON stored as a string.** `run_summaries.summary_json` is a JSON
   text, so mongo-express shows one long string and nothing in it can be
   queried.
7. **One decision recorded four ways.** An `entity_curation` doc holds:
   - `rules[]`: each rule's outcome per mention;
   - `decisions[]`: the final keep or drop per mention;
   - `pending_qids[]`;
   - `judge.verdicts` (keep per item), plus `judge.roles` and
     `judge.confirm_roles`.

   To answer "why was this link dropped" you read all of them.
8. **Events are not documents.** `events` has one document per (frame,
   section), with the events in an array inside it. 142,787 events live in
   36,673 documents, so "every event dated 2020" needs an `$unwind`.
9. **Generations share collections.** `kg_nodes` and `kg_edges` hold every
   kept build, distinguished only by the `build_id` prefix: today 2 ×
   795,711 nodes and 3.77M edges. `kg_builds` mixes build records with the
   pointer document `_id: "current"`.

The cost is not only to readers: today the metrics command-line tool read
every generation in `kg_nodes` at once (fixed in `ea9bbb3`), and ad-hoc
queries kept tripping over nested paths (`links.mentions`, not `mentions`).

## Why it matters

- Anyone new to the project (Riccardo, a supervisor, a future maintainer)
  meets Mongo first: mongo-express is the obvious place to look, and it
  shows hashes, packed strings and JSON blobs.
- Hand-written queries are where mistakes happen, as above.
- Each new stage copies the nearest existing pattern, so the
  inconsistencies spread.

## What fixing looks like

A rewrite of the store layer, in stages, cheapest and safest first:

1. **Document what is there.** A per-collection schema reference generated
   from the store modules, and Mongo JSON-Schema validators at
   `validationLevel: "moderate"`. No data changes, and it alone answers most
   "what is this field" questions.
2. **Readable views, no migration.** Mongo views over the awkward shapes:
   events one per document (`$unwind`), curation decisions with the key
   split into fields, frames joined to their URL and slug. New
   `run_summaries` written as documents, not strings.
3. **The migration** (breaking, one version bump per store):
   - readable ids (the KYM path, e.g. `memes/distracted-boyfriend`), one
     name for the page URL;
   - one time type (UTC BSON datetimes);
   - one naming scheme for version stamps;
   - decisions as sub-documents instead of packed strings;
   - one document per event;
   - KG generations in per-build collections, with the pointer in its own
     collection.
4. **Do not trigger recomputation.** Staleness keys live in these
   documents, so the migration rewrites stamps in place and is checked
   against the pending counts of every stage: entities, curation, events,
   templates and template entities must report the same pending work after
   the migration as before it.

The scope is every store module (`dom_store`, `parse_store`,
`entity_store`, `entity_curation_store`, `event_store`, `template_store`,
`template_entity_store`, `kg_store`, `summary_store`), the DAGs, the
dashboard and their tests.
