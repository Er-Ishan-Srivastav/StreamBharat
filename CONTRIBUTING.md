# Contributing to StreamBharat

Thank you for helping improve StreamBharat. The repository is a six-member academic engineering project with two related tracks: the original Q1–Q20 PySpark workbook and a Kafka / Delta / PostgreSQL / Metabase extension.

This guide defines how to make changes without compromising the raw dataset, data contracts or the accuracy of project claims.

## Development environment

For the Docker-based local platform:

- Docker Desktop with WSL2 integration, or Docker Engine with the Compose plugin.
- WSL2 Ubuntu is the documented workflow.
- 8 GiB of Docker memory is recommended for the core stack; Airflow is optional.
- Java and Python dependencies are installed in the Spark workspace image.

For host-side Python development:

- Python 3.10 or 3.11.
- Java 17 for Spark tests.
- Install the editable package and test dependencies:

~~~bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -e ".[test]"
~~~

For the complete Docker runtime, see the [README quick start](README.md) and [synthetic demo runbook](docs/demo-runbook.md).

## Branch and commit conventions

Create a focused branch from the latest main:

- Features: feat/qNN-short-name or feat/short-name
- Fixes: fix/short-name
- Documentation: docs/short-name
- Maintenance: chore/short-name

Use clear, imperative commit messages. Examples:

- feat: add subscription cohort bridge
- fix: preserve event-time ordering in player merge
- docs: clarify synthetic acceptance boundary

Do not work directly on main. Open a pull request for review and automated checks.

## Engineering requirements

### Data contracts

For each changed module, document:

- source paths/topics and required fields;
- schema, units, timestamps and event/business keys;
- output paths, table grain and partition keys;
- late-arrival, duplicate, malformed-record and retry behaviour;
- checkpoint and state compatibility, where applicable;
- assumptions, edge cases and known limitations.

Preserve stable identifiers such as event_id, user_id, profile_id and content_id across layers. Preserve Kafka topic, partition and offset lineage in the Kafka path.

### Data integrity and security

- Never modify supplied raw source files in place.
- Do not commit the course dataset, synthetic run data, Delta tables, checkpoints, model binaries, logs, local notebooks with data, or generated reports.
- Never commit .env, tokens, passwords, private keys or credentials.
- Keep new local paths under the established ignored data, lake, artifacts and logs directories unless a fixture is intentionally small and suitable for tests.
- Do not add real personal data. The platform's fictional scenario and the supplied learning sample are synthetic.

### Test expectations

Add or update deterministic tests for business rules. Add Spark/Delta contract tests for transformations, schemas, keys and persistence behaviour where practical.

Before opening a pull request, run the relevant checks:

~~~bash
# Python tests (host environment with Java 17)
python3 -m pytest -q

# Syntax / import compilation
python3 -m compileall -q src/streambharat producers dags tests scripts

# Shell entry points
bash -n scripts/*.sh config/*.sh

# Docker Compose models
docker compose -f docker-compose.kafka.yml config --quiet
docker compose -f docker-compose.airflow.yml config --quiet
~~~

When the core stack is already running, run tests in its isolated workspace:

~~~bash
docker compose -f docker-compose.kafka.yml exec -T workspace \
  python3 -m pytest -q
~~~

Do not replay the same synthetic date into a persistent Kafka/Delta environment as a routine retry. Kafka offsets, streaming checkpoints, Delta tables and PostgreSQL volumes persist. Use a dedicated fresh environment for clean acceptance runs.

## Pull request checklist

- [ ] The change has one clear purpose and is scoped to the appropriate workstream.
- [ ] Data contracts, schemas, assumptions and output paths are documented.
- [ ] Tests were added or updated for relevant behaviour.
- [ ] Python compilation, shell syntax and Compose validation pass where applicable.
- [ ] Raw data, generated state and credentials are excluded.
- [ ] Screenshots or query examples are labelled as synthetic or measured evidence.
- [ ] README/runbook/status docs are updated when user-facing commands or project status changes.
- [ ] The PR distinguishes code that is present from code that has been run and validated.

## Validation language

Use these terms consistently:

- **Implemented:** source code/configuration is present.
- **Unit-tested:** deterministic rules or contracts pass automated tests.
- **Locally validated:** the relevant job was executed and its persisted outputs were inspected.
- **CI-validated:** the relevant GitHub Actions run completed successfully for the commit.
- **Not yet validated:** runtime, source-data or performance evidence is still missing.

A passing test suite does not establish that the full course dataset, all Q1–Q20 outputs, long-running streams or production workload have been accepted. The [project status and validation record](docs/project-status-and-validation.md) is the source of truth for recorded evidence.

## Project references

- [Solution overview](docs/solution-overview.md)
- [Q1–Q20 question register and owners](docs/questions-and-ownership.md)
- [Detailed workstream implementation notes](docs/team-workstreams.md)
- [Architecture](docs/architecture.md)
- [Kafka extension architecture](docs/architecture-kafka-platform.md)
- [Delivery contracts and completion gates](docs/delivery-plan.md)
