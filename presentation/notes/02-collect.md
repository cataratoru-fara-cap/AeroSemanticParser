# Chapter 2 — Collect: find the pages, keep a copy, one address per entry (≈6 min)

DAGs: `kym_discovery` (monthly) → `kym_scrape`. Modules: `kym_discover.py`,
`scrapingant_client.py`, stores `mongo_store.py` (urls), `dom_store.py` (doms).

## The story
Before reading anything we need the pages. Discovery finds every entry's
address; scraping downloads it once and keeps a compressed copy forever, so the
parser can re-read it later without asking the site again.

## Doge at this step
- Found in KYM's sitemap at the address KYM gives him today,
  `/sensitive/memes/doge` (next stop), `lastmod` 9 July 2026; namespace
  `sensitive/memes`; confirmed.
- Fetched 9 September 2026 through ScrapingAnt's plain-HTML call (1 credit, no
  browser): 349 KB of HTML as KYM's server sends it — the About text is 0.8 KB,
  the rest is the site's menus, ad slots and script tags, none of them run.
  Stored zlib-compressed: 76 KB, with a SHA-256 fingerprint. (Until 7.1.0 the
  graph showed the old address: `lastmod` 18 June, fetched 10 July, 364 KB.)

## Numbers (live, `data/stages.json`)
- 39,972 URLs discovered; 25,058 confirmed entries (the others are unconfirmed
  submissions, plus photo/video/editorial pages).
- 25,016 pages stored, 42 failures: 34 permanent (gone, forbidden), 8 retryable.
- 6.86 GB of HTML, 1.41 GB stored: 4.9× smaller.
- 23,477 entries parsed (chapter 3); 708 pages are photos, videos and
  editorials living under entry-like addresses, and 815 were an entry's second
  address (gap 14).

## Design decisions — the why
- **Sitemaps first, listing pages as a fallback.** Sitemaps are cheap and say
  when a page changed; listings catch what sitemaps miss (category pages,
  paginated). Every URL gets its namespace (memes, people, events, sites,
  subcultures, cultures, sensitive…).
- **Idempotent and monotonic.** Upserting the same address twice changes
  nothing; `Confirmed` only goes false → true; `last_scraped` is never
  clobbered by discovery. That is what makes a monthly, interruptible process
  safe: if it stops, run it again.
- **ScrapingAnt, at its cheapest.** It gets past the site's bot checks. It is a
  subscription: credits each month, spent per call — the plain HTML call
  (`browser=false`) costs 1 credit, a browser-rendered one 10 (residential
  proxies 25×, never used). KYM renders on its server, so the plain HTML already
  holds every paragraph, link and image address; a browser would only run ads
  and analytics. About 40k credits for the whole corpus; failed calls are not
  billed.
- **Never pay twice.** Work is chunked, and every chunk re-filters against Mongo
  before fetching, so an Airflow retry never buys a page twice.
- **Two retry tiers, two kinds of failure.** Quick backoff inside the task, then
  Airflow retries. Errors have a *kind*: permanent (400/403/404/405/422) are never
  re-queued; retryable ones are.
- **Never lose a good copy.** A failed re-fetch never overwrites a stored DOM.
- **Fingerprint.** SHA-256 of the content: an unchanged page is not re-parsed.

## One entry, one address (gap 14 — found preparing this talk, fixed 2026-10-05)
- **The story.** Doge was in the graph twice. In July 2026 KYM moved him into its
  "sensitive" section, `/sensitive/memes/doge`, and the old address
  `/memes/doge` still opens (today it serves the same page, whose own address
  tag names the sensitive one). Discovery keeps every address it ever found, so
  we held both copies, each parsed, linked, read by the vision model and drawn.
- **How big.** 835 entries held at more than one address; 813 of them were two
  frames in the published graph (7.0.0). 811 were moved into "sensitive"; 24
  moved back out (Hide the Pain Harold) or were renamed (HRjak → "Anne Hathaway
  HR Meme / HRjak", via a stop in "sensitive"). KYM's sitemap still lists both
  addresses for 716 of them, and KYM's own pages still link the old addresses:
  16,008 links.
- **The rule (yours): normally no two entries hold the same name — keep the most
  recently discovered address.** Two addresses hold one entry when they differ
  only by `/sensitive`, when one's page names the other as its address, or when
  two meme entries share a title (photos and editorials may: "Link | Rule 63" is
  four photos). In the corpus, every repeated meme title was such a pair — the
  rule holds.
- **"Most recently discovered", read from KYM.** Discovery never recorded when it
  found an address, so the date comes from the pages: every page names its
  entry's address (og:url) on the day we fetched it; the newest page's answer
  wins. For a move that is the new address (Doge). It also gets the moves back
  and the renames right, which a "newest address" rule alone would not.
- **Where.** A step at the end of `kym_scrape`, after the fetch, every month
  (`resolve_duplicates`): marks the other addresses `duplicate_of` the kept one.
  Parse never reads them and retires their entries (24,291 → 23,477). The graph
  build sends every link to the kept address (11,509 page links, 947 series
  parents) and lists the old addresses on the frame (`also_at`). Logged in the
  run summary, listed on the dashboard's Scrape page, written up as gap 14.
- **A bug it exposed.** The parser filed a page under its canonical link, which
  only moved pages carry: TikTok's two pages overwrote one entry on every parse,
  and a renamed entry sat at an address with no page. Entries are now filed
  where their page was collected.

## If someone asks
- *Is scraping KYM allowed?* KYM has no API; pages are fetched once per change,
  politely, through ScrapingAnt. (Check with the lab before publishing
  anything derived from full page text — see the README's licence note.)
- *Why keep the whole HTML?* Parsers improve. Re-parsing from our copy (parser
  1.0 → 1.7) cost nothing and asked nothing of KYM.
- *What about the "sensitive" pages?* KYM moves entries it flags into
  `/sensitive/…` and leaves the old address up; ~1,500 entries live only there
  (they are kept), 835 were held twice (now once — see above).
- *So Doge's address in the graph changed?* Yes, from 7.1.0:
  `https://knowyourmeme.com/sensitive/memes/doge` — the one KYM gives today. IMKG
  (2023) used `/memes/doge`. Neo4j lists the old address on the frame (`also_at`);
  RDF says nothing of it (owl:sameAs would be the way — a model decision, open).
- *Why not just keep the public address?* Because it is not KYM's address for
  the entry any more — and the rule must also handle entries moved back out.
- *Numbers on other slides?* They are KG 7.1.0, the first graph that holds each
  entry once (7.0.0 still had the 813 duplicates).

## Sources
`airflow/README.md` (The stages: kym_discovery, kym_scrape), `dashboard` pages
Discovery and Scrape, gap 14 (`14-one-entry-two-addresses.md`), run summaries
`manual__gap14_duplicates_2026-10-05` (scrape) and
`manual__gap14_retire_2026-10-05` (parse).
