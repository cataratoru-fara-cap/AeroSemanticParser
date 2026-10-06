# One KYM entry was held at two addresses: 830 entries, 813 of them twice in the graph

**Status:** closed 2026-10-05. Found while preparing the talk: Doge had a
twin. `kym_scrape` now keeps one address per entry, and `kym_parse` drops
the others from the corpus. The graph build sends every link to the kept
address. Applied the same day: 836 addresses marked, 815 entries retired,
corpus 24,291 → 23,477. The graph follows at the 7.1.0 build.

## What it was

KYM moves an entry without retiring its old address, in three ways:

- **Into its sensitive section.** Doge moved in July 2026 from
  `/memes/doge` to `/sensitive/memes/doge`. Today
  `https://knowyourmeme.com/memes/doge` serves the same page, whose
  `og:url` is the sensitive address.
- **Back out again.** For Hide the Pain Harold, the sensitive address now
  serves the public page.
- **Renamed.** `/memes/hrjak` became `/memes/anne-hathaway-hr-meme-hrjak`,
  after a stop at `/sensitive/memes/hrjak`.

The old address keeps answering, and in 716 of the pairs KYM's sitemap
still lists both. Discovery keeps every address it ever found, so the
corpus held both copies, each parsed, linked, read and put in the graph as
its own frame.

Measured on 2026-10-05:

- 830 address pairs differ only by the `/sensitive` root; 813 had both
  copies in KG 7.0.0. Every repeated title among the 24,291 parsed entries
  was one of these pairs. No two different entries shared a title, as
  normally no two media frames can hold the same name.
- KYM's own pages still link the old addresses: 16,008 page links and
  1,640 series parents pointed at a public twin, 7 links at a sensitive
  one. This holds even for pages fetched in October.
- The parser files an entry under a page's `<link rel="canonical">` when
  there is one, and only moved pages carry one. So TikTok's two pages
  overwrote one entry on every parse. A renamed entry (Trump Flinches
  Beside Xi Jinping) sat at an address with no page of its own.

## Why it mattered

- **Every count was inflated.** 813 frames were counted twice, with their
  entities, events, templates and image readings. Rankings, the IMKG
  comparison and the talk's numbers all included them.
- **The graph split the entry.** Doge's children and the pages citing him
  reached `/memes/doge`, while the `/sensitive/` copy stood beside it as a
  stranger, joined only as its own "sibling".
- **Effort was wasted.** The vision model read the same image twice for
  every pair.

## What was done

1. **`kym_scrape` → `resolve_duplicates`** (Phase 3 of discovery, in
   `modules/kym_discover.py`; bookkeeping in `dom_store`). It runs after
   the fetch, over every stored page. Two addresses hold one entry when:
   - they differ only by the `/sensitive` root or by spelling (an emoji
     slug is percent-encoded in the sitemap and raw in `og:url`);
   - the page at one names the other as its address (`og:url`);
   - both are meme entries with the same title. Photos and editorial
     pages legitimately share titles ("Link | Rule 63" is four photos).

   The kept address is the most recently discovered one. Discovery never
   recorded when it found an address, so the date is read from KYM itself:
   the address named by the entry's most recently fetched page. In the move
   case that is the new address (Doge). It stays right when KYM moves an
   entry back (Harold) or renames it (HRjak). If the newest page names an
   address we hold no page for, the page we hold is kept. The others get
   `duplicate_of` (the kept address) and `duplicate_since` in `urls`. A
   mark is lifted when the evidence changes. A marked address is still
   refetched when its sitemap date moves, because its fresh page is how a
   move back shows up.

   Every stored page now records `page_url` and `page_title` (pages stored
   earlier were read once: 25,016 in 50 s). The run summary gives counts,
   evidence and examples, and lists in full any group joined by title
   alone. The dashboard's Scrape page lists every mark.
2. **`kym_parse`** never selects a marked address, and `retire_duplicates`
   deletes its entry and dead letter. Both are derived from the stored page,
   which stays. An entry is filed under the address its page was collected
   at, not the canonical link (`build_entry_doc(address=…)`).
3. **`kym_kg`** freezes the map `{dropped address: kept address}` with the
   snapshot and stamps it (`duplicate_addresses`, `…_digest`). It leaves the
   dropped entries out even before parse has run. It sends series parents,
   page links and event links to the kept address (`build_nodes_and_edges(
   kept_address=…)`), and lists the absorbed addresses on the kept frame as
   `also_at` (property graph only).

First run (`kym_scrape manual__gap14_duplicates_2026-10-05`, fetching
nothing; `kym_parse manual__gap14_retire_2026-10-05`):

| | |
|---|---|
| entries held at more than one address | 835 |
| kept in `/sensitive/` / at a public address | 811 / 24 |
| addresses marked | 836 (831 with a stored page) |
| grouped by address / by `og:url` / by title | 830 / 18 / 829 (none by title alone) |
| entries retired | 815 |
| corpus | 24,291 → 23,477; no two meme entries share a title |
| links now sent to a kept address | 11,509 page links, 947 series parents |

## What changes for users of the graph

A frame's IRI is the address KYM gives the entry today, so Doge is
`https://knowyourmeme.com/sensitive/memes/doge` from 7.1.0 on. IMKG (2023)
and KG 7.0.0 used `/memes/doge`. In Neo4j, look a frame up by either
address:

```cypher
MATCH (f:Frame {build_id: $bid})
WHERE f.id = $url OR $url IN f.also_at
```

RDF carries no statement of the old address. If IMKG's IRIs need to keep
resolving in SPARQL, `owl:sameAs` from each dropped address to the kept
one would do it. That is not done: it is a model decision, not part of
this fix.

## Left as is

- The outputs computed earlier for the dropped addresses (entities,
  events, templates, frame-image readings) stay in their collections, and
  nothing reads them. If a mark is ever lifted, they are reused under their
  stamps instead of being recomputed.
- Four kept addresses (TikTok, JoJo's Bizarre Adventure, The Narwhal Bacons
  at Midnight, All Your Base Are Belong to Us) hold a July page. The newer
  copy was served at their dropped `/sensitive/` address. Their sitemap
  dates are newer, so the next monthly scrape refetches them.
