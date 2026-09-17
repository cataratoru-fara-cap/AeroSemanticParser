# Technical gaps that will eventually bite me in the ass

Things found true and confirmed while doing other work (mostly the IMKG-extension
KG remodel, 2026-09-17), deliberately not fixed at the time because they were
out of scope for what was being asked. Each file is one gap: what it is, why
it's true, why it matters, and what fixing it looks like. Not urgent by
definition — if it were, it would have been fixed instead of noted — but real,
and each one gets worse the longer it sits.

- [01-mk-namespace-needs-agreement.md](01-mk-namespace-needs-agreement.md) —
  the MemeAtlas RDF namespace sits under IMKG's own domain without sign-off.
- [02-category-class-casing-unverified.md](02-category-class-casing-unverified.md) —
  `kym:<Category>` IRIs are a guess at IMKG's actual casing.
- [03-parser-gaps-aliases-scraped-at-template-image.md](03-parser-gaps-aliases-scraped-at-template-image.md) —
  three fields the parser writes are structurally wrong or always empty, and the
  KG now faithfully reproduces that.
- [04-fuseki-disk-growth-tdb2-compaction.md](04-fuseki-disk-growth-tdb2-compaction.md) —
  TDB2 never reclaims space when a build graph is replaced; already at 2.8G.
- ~~[05-postgres-weak-password-hardcoded.md](05-postgres-weak-password-hardcoded.md)~~ **(closed 2026-09-17)** —
  `POSTGRES_PASSWORD=airflow`, and it's hardcoded a second time, so editing
  `.env` alone won't even fix it.

Update the status line at the top of a file when a gap is closed; leave the
file (don't delete it) so the decision trail survives.
