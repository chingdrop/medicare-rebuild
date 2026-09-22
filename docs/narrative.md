# Engineering narrative

This is a first-person account of building the pipeline this repository is a cleaned-up
version of — what the problem was, the decisions I made, what broke, and what I'd do
differently now. It's the one place in these docs written in my own voice; everywhere
else aims to be a neutral description of the code. For the formal version of most of
what's below, see [Design decisions](decisions/README.md); for what this repository does
and does not contain, see [Provenance and data boundary](provenance-and-data-boundary.md).

## The problem

I was asked to rebuild the data architecture behind a healthcare provider's remote
patient monitoring program billed to Medicare. The billing rules need an accurate date
of service for each billable event, and the source data — patient records, device
readings, clinical notes — lived across several legacy SQL databases and a SharePoint
list export, none of it shaped for that. See
[Provenance](provenance-and-data-boundary.md#provenance) for the fuller framing of what
this repository is and isn't.

## Decisions, in brief

Most of the real decisions from this build are written up formally as
[decision records](decisions/README.md) now, with their context, alternatives and
consequences, so I won't repeat them here. A few are worth calling out because of how
they came about rather than what they were:

- Several of them — billing logic staying in SQL stored procedures
  ([0002](decisions/0002-billing-rules-in-stored-procedures.md)), keys staying
  database-assigned rather than moving into Python
  ([0004](decisions/0004-database-assigned-keys-and-full-reset.md)), and how far
  SQLAlchemy went without full ORM models
  ([0007](decisions/0007-sqlalchemy-engine-and-raw-sql.md)) — came from a direction set
  for the project, not a technical call I'd have made unprompted. I'd have reached for
  pandas over more SQL, and moved more of the ID-resolution logic into Python, given the
  choice.
- Extracting to a separate stage rather than transforming the source database in place
  ([0001](decisions/0001-staged-etl-in-pandas.md)) was a real constraint, not a
  preference: that database was live production infrastructure, still serving the
  business, and too broken to transform safely in place.
- One dead end isn't in the ADRs because there was nothing to decide, just something
  that didn't pan out: I tried pulling device readings through a vendor's API directly
  instead of the legacy database, and it was too slow for the volume of patients
  involved to be practical — I looked at distributing the work with Celery before
  concluding the API itself was the bottleneck. The API route was eventually dropped for
  an unrelated, harder reason documented in
  [0006](decisions/0006-source-extraction-and-environment-credentials.md); the
  performance problem was the earlier warning sign.

## What broke

The most useful bug, in hindsight, was one the mocked unit test suite couldn't have
caught: `execute_query` committed a transaction before fetching its result rows, which
is invisible to a mock but breaks against a real SQL Server cursor. Standing up an
actual database for integration tests is what surfaced it — see
[0008](decisions/0008-mocked-unit-tests-and-real-sql-server-integration-tests.md) for
why the two test layers exist.

Multi-device patients loading duplicated readings — a join fans out per device rather
than per patient — is a similar case: harmless to billing (which counts distinct days,
not rows), but a real anomaly I noticed and never tracked down at the time. It's
documented now in [billing-rules.md](billing-rules.md#known-gaps-and-assumptions) and
pinned by the synthetic demo.

## What I'd change now

The pattern across several of the ADRs above is a project built under real-world
pressure: a schema and rule set that made sense operationally (nursing staff working to
a time target to cover patient volume, a billing report the client needed before a
failed-row audit trail was finished — see
[0005](decisions/0005-validate-in-transform-and-drop-bad-patient-rows.md)) more than one
built from a clean design. Given a second pass, I'd want a dedicated date-of-service
entity rather than deriving it from a timestamp column (a gap noted in
[0003](decisions/0003-medical-code-rows-carry-the-date-of-service.md)), and I'd bring
more of the logic that's currently duplicated in SQL and Python under one layer, in
whichever direction that project's constraints allowed.

## Where the rest of this story lives

| Topic | See |
|---|---|
| Why the pipeline is shaped the way it is | [docs/decisions/](decisions/README.md) |
| What the billing rules actually do | [docs/billing-rules.md](billing-rules.md) |
| Running it yourself | [docs/demo.md](demo.md) |
| What is and isn't in this repository | [Provenance and data boundary](provenance-and-data-boundary.md) |
