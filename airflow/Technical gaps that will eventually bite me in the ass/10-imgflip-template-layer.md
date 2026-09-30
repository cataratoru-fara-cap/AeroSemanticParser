# The imgflip template layer rests on an outside site, a pixel hash, and a vision model

**Status:** open — built and measured on 100 frames (2026-09-28); not yet run on the corpus, not in any published graph.

## What

KG 6.4.0 adds imgflip meme templates per frame (`kym_templates`) and what
their images show (`kym_template_entities`). Three things in it are weaker
than the rest of the graph, on purpose or by necessity:

1. **imgflip is someone else's site.** It is read through its public,
   server-rendered search and template pages at ~1 request/s with a
   research user agent; robots.txt allows those paths and sets no delay.
   imgflip's terms have no scraping clause, but do ask users not to access
   the service "using a method other than the interface and the
   instructions that we provide" — which is why its internal JSON search
   endpoint is never used, although it would cut the requests by ~40x. The
   official alternative is the Premium API (`search_memes`, $9.99/month
   plus ~$0.005 a search after 200: ~$130 for one pass over the corpus).
   If imgflip starts refusing us, the client switches to ScrapingAnt
   (capped per task) or fails loudly; it never records "no results".

2. **"Same picture" is a perceptual hash on a 250 px thumbnail.** It merges
   resizes, re-encodes, mirror images and letterboxed copies, and keeps
   crops and edits apart (Gabi's rule). Measured on 427 within-frame pairs:
   a re-crop of the same scene and a genuine edit (a hat drawn on, a face
   swapped) are equally common anywhere between pHash 13 and 24, so a crop
   still counts as its own template — the commonest reason a frame's ten
   templates are less varied than they look (Thanos, "Wide Putin",
   "Kowalski Noted"). A crop-tolerant comparison (sliding one thumbnail
   over the other at a few scales, numpy only) would close most of it.

3. **Relevance is a name and a picture.** Precision read from contact
   sheets was ~0.95 on 50 + 50 random frames — read by Claude, not a
   person. The residual: a template uploaded under a frame's name that is a
   different joke ("Do a barrel" with a noose, two "Overwatch" uploads that
   are other templates), and " / " titles whose parts are common English
   ("Enabled / Disabled"): those parts are weak evidence now, which removed
   15 wrong templates and cost a few right ones ("Reaction Guys" variants).

4. **Image entities are a model's reading** (qwen3-vl:32b), with the
   frame's own text as context. The 10% blind audit measures how often a
   name appears only with context. On the 200-template pilot that was 3 of
   23 (13%), all three RIGHT: context supplied names the blind reading got
   wrong or missed. Gabi (2026-09-29): keep the context names; the planned
   switch to agreed-only names above 10% is dropped. The residual risk is
   a wrong name the context suggests, which only reading catches. Generic regions ("man",
   "phone") are gap 09's problem again, which is why only the three largest
   per template reach the graph.

5. **Search alone finds a frame's own template about 4 times in 5.** With
   the KYM "Meme Generator" link HIDDEN, on 150 of the 782 linked frames
   (2026-09-28): recall@10 0.787 (81 the very upload, 18 a merged
   duplicate of it, 19 the same picture), recall@1 0.513. Of the 32 misses,
   20 frames got nothing (imgflip's search returned nothing for 15 titles),
   4 were the 10-template cap on person frames (Terry Crews, Hulk Hogan:
   dozens of right templates, the linked one ranked 17th-41st), 2 reordered
   names ("Jordan Peele Sweating" for "Sweating Jordan Peele" — word order
   is enforced because "History Drunk" was kept for "Drunk History"), 1 the
   weak-part rule ("The What"), 1 a one-word name for a long title
   ("Observe"). Linked frames keep their link in production, so this is
   what the other 17.7k frames get.

## TODO

- [x] Gold recall, link hidden: 0.787@10 / 0.513@1 on 150 frames (above).
- [x] Phase-2 spike (2026-09-29): 30 templates through qwen3-vl:32b,
      boxes drawn and read. The 0-1000 grid holds and the boxes are
      right on nearly every template. It takes 2-8 s per call (30 s at
      worst), not 25-45 s. What it found, all fixed before the pilot:
      * On Ollama 0.34.1, qwen3-vl:32b is a thinking model. Asked not to
        think and given a grammar, it files the JSON under
        message.thinking with empty content. The client now reads it from
        there in exactly that case.
      * The entity list had no bound. A long list ran past the token cap
        (2 in 30), so the grammar now allows at most 12.
      * `named` was set on generic things ("door", "caracal"). It now
        needs a capitalised name.
      * Printed words were put in `name` with no `text` ("Panik",
        "Kalm"). `text` is now required, and the name is used when it is
        the words.
      Still open: text can be lifted from the template name rather than
      read ("LORD FORGIVE ME" on a picture with no words), and a gesture
      was classed as text ("facepalm"). The blind audit measures names,
      not text.
- [x] 200-template pilot (2026-09-29), read on contact sheets. 0
      failures, 4.9 s p50. Named links are right. Half of the
      printed-text links were wrong, so text now links only names
      (`text_link_ok`: 19/38 right before, 12/14 after).
- [x] Pool cap: NONE (Gabi, 2026-09-29). Every frame keeps its 1-10.
      See "Throughput" below.
- [ ] Ask IMKG's authors about `mk:template/` along with gap 01.

## Throughput (measured 2026-09-29, 300-frame run + 200-template pilot)

| Stage | Bound by | Measured | Full corpus (≈18.2k frames left) |
|---|---|---|---|
| search (`kym_templates`) | our politeness gaps: 2 s per HTML page, 0.25 s per image, per task; 2 tasks | 12.6 s a frame a task (3 pages + ~30 thumbnails) | ~32 h |
| details (page + blank) | the 2 s HTML gap | ~2.1 s a template a task | ~15-30 h |
| VLM (`kym_template_entities`) | one qwen3-vl:32b call at a time on ollama-ccdd | 4.9 s p50, 7.3 s mean (tail: retries of deterministic runaways) | ~3-5 days at 50-60k templates |

The 300 frames were all priority 1 (a known imgflip link), which is the
richest kind: 5.4 templates a frame, 80 of 300 frames at the cap of 10.
The earlier cross-priority calibration gave ~3.5 a frame, so the pool is
more likely 50-65k distinct templates than 100k. Scraping needs no GPU,
so it can run while the VLM reads earlier batches. Disk: ~17 GB of
images (133 GB free).

Mitigations (2026-09-29). Measured on 100 pilot templates against their
stored readings. Same-prompt noise: 91% of regions and 86 of 89 names
matched.

| Change | Result | Adopted |
|---|---|---|
| Search in batches of 2,000 frames, each triggering the reader for all pending templates (never the KG) | reading overlaps the ~2-day crawl | yes |
| A rejected reading retried once at temperature 0.4, not 3 times at 0 | removes the identical-retry tail | yes |
| Blind audit on 1 in 100 templates, not 1 in 10 | ~500-650 audited over the pool; -9% calls | yes |
| No fallback to another vision model; same weights on any host | the reader can be spread over hosts | yes |
| Readers rotate host priority; `KG_TEMPLATES_VLM_PARALLEL` sets how many | 2 readers on ollama-ccdd alone: same 11.8/min, 2x latency | yes, set to 1 until ollama-ui serves qwen3-vl:32b (asked of the maintainers) |
| One-line JSON output | 16% faster, but lost names (82 vs 89, 76 in common: Smudge the Cat, Captain America, Toad) | no |
| qwen3-vl:8b | 1.56x faster, but a different reader: 144 names vs 89, many lifted from the frame's text ("Super Bowl LIV", "Red Table Talk") | no (Gabi's choice stands) |
| Worker slots 4 -> 8, memory 3 -> 6 GiB | the curation judge, search, reader and event extraction fit side by side | yes |
| Pool size: a 500-frame random sample, stratified by priority (`sample_seed`) | running | -- |

Found in the full run (2026-09-30), each a way ONE template failed a whole
batch of 25 or never got read. All three are fixed and tested:

- **A generation stuck repeating itself.** Ollama aborts it with "500
  prediction aborted, token repeat limit reached". The client took that
  for a host failure and failed 42 chunks. It is now a rejected answer:
  one retry at temperature 0.4, then a dead letter.
- **Gold templates with no image.** Resolving a frame's own imgflip link
  read the template's page without its image, and the details step keyed
  on "page read", so 739 kept templates were never fetched. It now keys on
  the image.
- **An image Pillow cannot decode.** One PNG of 26,719. It raised out of
  the reader; it is now a dead letter (`error_kind` "image").
