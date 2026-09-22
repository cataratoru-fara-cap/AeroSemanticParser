# The event layer is a language model's reading, and nobody has checked it

**Status:** open — narrowed 2026-09-18 by extraction 2.0.0, 2026-09-21 by
2.2.0 / 2.3.0 and 2026-09-22 by 2.4.0–2.6.0 (see the updates), not closed.

## Update 2026-09-22 — the review is drawn and the tool exists

The sample is on the table: **127 sections, 306 events to judge**, drawn
with seed 20260922 from the 1,528 sections extracted by 2.6.0, and waiting
at `:8080/dashboard/` under "Review".

    python -m modules.kg.review draw      # already run; --redraw to redo
    python -m modules.kg.review report     # the number, any time

Three samples, scored separately and never blended:

| sample | sections | why |
|---|---|---|
| `representative` | 60 | The headline. Proportional across section kind and event density, so the 43 crowded sections are not left to luck. |
| `relative` | 30 | The 40 dates the PIPELINE computed by counting from another event. 4% of events, so a proportional draw holds nine; an error here runs down a chain. |
| `unconfirmed` | 42 | Every section with a hedged event — 51 of them, a census rather than an estimate. |

Two things the tool enforces because the number is worthless without
them. **The recall question comes first**: the reviewer says which
sentences narrate an event before the extraction is revealed, because with
the events on screen attention anchors to them and misses stop being
visible. And **every field defaults to "ok"** — asking for 306 explicit
confirmations would guarantee rubber-stamping.

Rates come back with Wilson intervals, which at 20/20 clean say "at least
84%", not "100%". Verdicts carry a reviewer name so a second pass over ~20
sections measures how much of the number is one person on one day.

The standing caveat: this is the system's author reviewing the system's
output. That belongs printed next to the result, not worked around.

## Update 2026-09-22 — what a 1,528-section sample found

The 99-section pilot was too small to see anything rare. Re-drawn at random
across the corpus (1,528 sections, 5,579 events) and swept for every
invariant a record should hold internally — not just `audit()`'s "nothing
was added", but precision against date format, relative chains pointing
backwards in time, citations present in the text they are attached to,
values that name nobody — it turned up five bugs, four of them in THIS
repo's code rather than the model's reading:

* **Paragraph-final citations became empty sentences.** `"... first
  appeared. [2]"` split into the sentence and a "sentence" holding only
  `[2]`, which rendered as a blank numbered line for the model and moved
  the citation off the sentence that cites it. **18.9% of sections**, 687
  orphaned citations in a 2,909-section scan.
* **A decade dated to its first year.** "the first half of the 2010s"
  became 2010-01-01. 13 of 4,073 dated events carried a year nobody wrote.
* **A year stated only after the first event was unreachable**, leaving 30
  events undated on a day the page plainly gives.
* **`mk:dateAnchoredTo` was keyed by sentence number**, and 74 events share
  a first sentence with another, so an undated event in between could
  steal the link. Latent — zero occurrences — and silent when it happens.
* **The one model failure** (1 in 882) was Ollama's grammar-constrained
  sampler dying mid-string on Cyrillic. 1.5% of sections carry a non-Latin
  script, so dead-lettering them loses the international memes
  specifically, not a random slice.

The confirmation re-run extracted **1,528 of 1,528 with zero failures and
zero audit violations**, and every correctness invariant in the sweep
holds. What remains flagged is either the checker's own false positives
(names with internal punctuation: "Yahoo! China") or accepted behaviour:
41 overlapping events, which are real (KYM comma-splices two posts into
one sentence); 6 verbose-but-faithful actors; and 3 events in 5,579
carrying nothing but their text on top of an event that has a date, a
place and a name.

None of this touches what is still open below: whether the model's
READING is right. That still needs people.

## Update 2026-09-21 — 2.3.0: the pipeline was the one making things up

Comparing four models on the same 99 sections turned up defects in this
repo's arithmetic, not in any model's reading. A missing year was taken
from the most recent four-digit NUMBER before the event, which read years
out of quoted captions ("Reject modern memes, return to 2010"), festival
names ("Stagecoach 2025") and editorial asides ("it didn't establish the
year 2026 as a start date"): **10 of 339 dated events carried a year the
page never meant**, one of them off by 32 years. The year now comes from
the most recent DATE the section states before the event's own date words.

Worth keeping for the next time a metric is used to choose something:
`llama3.3:70b` scored HIGHER on date recall (95.1% against 94.0%) and is
worse. Its extra dates are dates inside reported content — "According to
the post, Aquaman was born on July 12th, 2025" — and one of them then
anchored a whole "on the same day" chain to a fictional birthday. The
table said pick the 70B; reading one frame said do not.

## Update 2026-09-21 — extraction 2.2.0: grounding, and what it cost to learn

2.0.0's rule was "a value not in the text verbatim is discarded". Reviewing
the retroslop entry by hand showed what that rule actually did: KYM states
a year once and then omits it ("On June 16th, TikToker ..."), the model
writes it back in, and the whole phrase was thrown away. **34 of 421 pilot
events lost a date the page plainly gives** — and worse, the events after
them that said "that same day" were then anchored to the wrong event, so
the discard did not just lose data, it silently produced wrong dates.

2.2.0 grounds instead of discarding: the model's value is a POINTER, matched
back to the section and stored in the section's own words. A value with
nothing behind it (an invented actor) still reaches nothing; a date the
sentences contradict is still refused rather than corrected to theirs. The
`discarded` field is gone — a stored value is the page's, or it is absent.

The lesson worth keeping: a strict verification rule is not automatically a
safe one. This one was strict in the wrong unit (the whole phrase rather
than each component), and its failures were invisible because they looked
like the model being wrong.

## Update 2026-09-18 — extraction 2.0.0 closes the "additions" half

After a review of real output, the extractor was rebuilt so that **nothing
the model adds to the text can reach the store**: it answers with sentence
NUMBERS (the evidence text is copied from the page by the pipeline), writes
no summary, and every value it returns — date words, place, each actor — is
checked against the page (since 2.2.0, grounded to it).
`audit()` re-checks every record independently before it is written. The
actor TODO below is done.

What 2.0.0 does NOT address — and so what is still open here — is a wrong
*reading* built entirely from real text: the right sentence tagged with the
wrong certainty, two real names where only one took part, a real year
attached to the wrong sentence. Every piece is on the page; the combination
can still be wrong. That is what the review sample below is for.

*Original write-up:*

## What

Every `mk:Event` in the graph is one language model's reading of one
sentence of KYM prose (`kg/events.py`, `ministral-3:14b` by default). At
full coverage that is on the order of 120,000 assertions — dates, places,
named actors, and a judgement of how certain the source was — and **not one
of them has been reviewed by a person.**

What protects the graph today, and what it does not cover:

- **`mk:sourceText` is checked to be a real substring of the prose the model
  was shown** (after folding whitespace, inline `[n]` citation markers and
  typography). That catches invented provenance outright. It does NOT catch
  a wrong *reading* of a real sentence: the right quote attached to a wrong
  date, a wrong actor, or a summary that says more than the quote does.
- **An event that fails any of these checks is dropped, not stored** — and
  recorded on its section (`dropped_events`, with the reason), so the drop
  rate is a number on the dashboard rather than an absence nobody sees. On
  the first real run, the typical drop was a model *improving* a quote:
  writing "On April 28th, 2025" where the page says "On April 28th". A
  plausible guess, and exactly what "verbatim" has to refuse.
- **The model no longer dates anything** (2.1.0). It returns the date words;
  the pipeline parses them and resolves relative phrases against the nearest
  earlier dated event, recording `mk:dateBasis` and `mk:dateAnchoredTo`. The
  class of error where it read "On June 4th, 2014" as 2014-06-03 cannot
  occur. What remains is the model pointing at the WRONG date words for an
  event, which the audit cannot see.
- **`mk:certainty` is the model's reading of hedging**, not ground truth.
  On the 20-unit pilot, 60 of 61 events came back `confirmed` — plausible
  for KYM's mostly chronological prose, but also exactly what a model that
  ignores hedges would produce. One pilot event summarised "tweets about
  Jaden Smith *allegedly* cheating" as `confirmed`, which is defensible
  (the tweets did surface) and also exactly the flattening the field was
  meant to prevent.
- **Actors are NOT verified the way quotes are.** On the first 40-section
  run, 12 of 149 actors were not in their own event's quote but were named
  elsewhere in the section — legitimate coreference ("the same TikToker").
  2 (1.3%) appeared nowhere in the section: the model had added a
  disambiguating parenthetical — `Invincible (TV series)`, `visit (report)`.
  Not invented people, but not verbatim either, and nothing catches it.
- **The merge rule is followed loosely.** "A few minutes later, X did Y"
  came back as its own undated event rather than being merged into the
  previous one, as the schema's description asks.

The graph now holds scraped fact and model inference under one namespace.
The only things that tell them apart are `mk:extractionModel` and
`mk:sourceText` on each event — which is enough for a careful consumer and
nothing for a careless one.

## Why it matters

A SPARQL user asking "what happened to this meme in 2019" gets a crisp,
typed answer with no visible difference from `m4s:year`, which was parsed
from the page. Errors in this layer do not look like errors. And because
the model's output is deterministic at temperature 0, a systematic misreading
is reproduced identically on every re-run, so repetition says nothing about
correctness.

## TODO

- [ ] Draw a stratified sample of ~200 events (by `source_section`,
      `date_precision`, `certainty`, and entry `category`) and review each
      against its `mk:sourceText` by hand.
- [ ] Publish the agreement number next to the graph (MODEL.md, and the
      dashboard's Events page) — per field, not one overall score.
- [ ] If `certainty` agreement is poor, either improve the prompt (bump
      `PROMPT_VERSION`, which re-queues everything) or stop emitting
      `mk:certainty` until it is.
- [ ] If overall agreement is poor, add an `mk:reviewStatus` to events and
      let consumers filter on it rather than silently shipping them.
- [x] Verify actors against the SECTION (not the quote — coreference is
      legitimate), stripping any that do not appear. Done in 2.0.0, along
      with every other returned value.
- [ ] Consider a second model as a cross-check on a sample, recording
      where two models disagree — disagreement is cheap to find and is a
      better review queue than a random sample.
