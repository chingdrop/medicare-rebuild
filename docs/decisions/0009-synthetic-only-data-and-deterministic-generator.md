# 0009. Use only synthetic data, generated deterministically with a by-construction manifest

Status: Accepted (2026-09-21, `adb6318`, `4e744f1`)

## Context

This repository is a published, cleaned-up version of a pipeline, and no real data may appear in it. A reviewer should still be able to run the whole pipeline without credentials or real data. The source schema is not in the repo.

## Decision

All data is synthetic (see the provenance document). `tools/synthetic_data` generates seeded source files plus a manifest of expected results. Each scenario states its expected outcome when its data is built. A safety scan fails the run on real-looking identifiers, and generated files are git-ignored. `make demo` runs the real pipeline and compares the result with the manifest; the same comparison backs an integration test.

## Alternatives considered

Computing expectations by re-implementing the billing rules is named and set aside in `adb6318`. Patching the Graph client inside the demo runner was chosen over changing `src/` (`4e744f1`). Nothing else is recorded.

## Consequences

- The demo's tables are reconstructed, not authoritative.
- The demo calls the patient-note steps that `main()` does not, and replaces Graph with a local file.
- The manifest encodes the procedures' current thresholds, not payer policy.

## Evidence

- [`tools/synthetic_data/`](../../tools/synthetic_data/), [docs/demo.md](../demo.md), [provenance statement](../provenance-and-data-boundary.md)
- [`tests/test_synthetic_data.py`](../../tests/test_synthetic_data.py)
- [`test_demo_passes_every_check`](../../tests/integration/test_demo_integration.py)
