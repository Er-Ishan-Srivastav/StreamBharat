# Contributing to StreamBharat

1. Create a branch for the question or feature you own: `feature/qNN-short-name`.
2. Do not commit raw data, checkpoints, Delta output folders, model binaries, credentials or local environment files.
3. Keep raw source files unchanged.
4. Document each module's input/output path, schema, assumptions, command and checks.
5. Add deterministic unit tests for business rules and integration tests for Spark/Delta behaviour where practical.
6. Run `pytest -q` before opening a pull request.
7. Coordinate shared schema changes with downstream owners before merging.
8. Do not mark a task complete until the output has been generated and validated against the workbook requirement.

Use the workbook and team guide under `docs/source-materials/` as the project-specific reference.
