# StreamBharat — OTT Streaming Analytics Platform

**Project 4 · PySpark Industry Projects · Six-member team implementation**

StreamBharat is an end-to-end data engineering and analytics project for a fictional Indian OTT platform. It uses PySpark, Spark Structured Streaming, Delta Lake and Spark MLlib to turn playback, billing, catalogue, advertising, search and CDN data into analytics and machine-learning outputs.

> **Source of truth:** Workbook 2 — *Project 4: StreamBharat: OTT Streaming Analytics Platform*. This repository uses the supplied file-based simulator for Q17/Q18. Kafka, Airflow, PostgreSQL and Metabase are not required by this workbook's core implementation.

## Project at a glance

- **Sample data:** 2,500 accounts, 4,736 profiles and approximately 1.62 million player events.
- **Data periods:** subscriptions May 2025–October 2026; playback/search/ads July–October 2026; CDN logs September 2026.
- **Important convention:** a playback heartbeat represents up to five minutes in this dataset, not 30 seconds.
- **Architecture:** immutable raw inputs → Bronze ingestion → Silver cleansing/conformance → Gold facts/reports → ML features and scores.
- **Stack:** Python 3.10/3.11, Java 17, PySpark 3.5.3, Delta Lake 3.2.0, pytest and Spark MLlib.

## Repository structure

```text
StreamBharat/
├── README.md
├── pyproject.toml / requirements.txt
├── ott_stream_simulator.py
├── data/README.md
├── docs/
│   ├── project-brief.md
│   ├── questions-and-ownership.md
│   ├── team-workstreams.md
│   ├── data-catalog.md
│   ├── architecture.md
│   ├── delivery-plan.md
│   ├── implementation/                 # Q4, Q17 and Q20 design notes
│   └── source-materials/                # Workbook, team guide, dataset README
├── src/streambharat/
│   ├── q1_catalog.py ... q20_churn.py   # Question-specific implementations
│   ├── q18_live_ad_pacing.py
│   ├── pipeline.py                      # Finite batch orchestrator
│   ├── quality_rules.py                 # Deterministic business rules
│   └── spark_session.py
├── tests/                               # Pure-Python deterministic tests
├── scripts/
└── .github/workflows/
```

## Team ownership and implementation map

| Member | Questions | Implementations |
|---|---|---|
| Ishan | Q4, Q17, Q20 | `q4_incremental_ingestion.py`, `q17_trending.py`, `q20_churn.py`; final batch orchestration |
| Rutik | Q2, Q5, Q12, Q14 | `q2_playback_clean.py`, `q5_sessions.py`, `q12_account_sharing.py`, `q14_skew_optimization.py` |
| Harshvardhan | Q1, Q8, Q10, Q16 | `q1_catalog.py`, `q8_content_history.py`, `q10_top10.py`, `q16_recommendations.py` |
| Kshitij Jaisawal | Q6, Q11, Q15, Q18 | `q6_subscriptions.py`, `q11_cohorts_mrr.py`, `q15_royalties.py`, `q18_live_ad_pacing.py` |
| Siddhi | Q3, Q7 | `q3_cdn.py`, `q7_search.py` |
| Ankita | Q9, Q13, Q19 | `q9_active_users.py`, `q13_ads.py`, `q19_quality.py` |

The complete question-by-question purpose, outputs, rules, dependencies and known assumptions are documented in [`docs/team-workstreams.md`](docs/team-workstreams.md) and [`docs/questions-and-ownership.md`](docs/questions-and-ownership.md).

## Set up the project

### 1. Obtain the course dataset

Download `StreamBharat_dataset.zip` from the course distribution, extract it and copy its `data/` directory into the repository root. The expected local paths include:

```text
data/raw/player/*.json
data/raw/billing/subscription_events.json
data/raw/app/users.json
data/raw/content/catalog_2026-10-01.json
data/raw/reference/*.csv
```

Raw files are intentionally excluded from Git. Do not edit them. See [`data/README.md`](data/README.md) and [`docs/data-catalog.md`](docs/data-catalog.md).

### 2. Install dependencies

Use Python 3.10 or 3.11 and Java 17.

```bash
python -m venv .venv
source .venv/bin/activate             # Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[test]"
```

Ensure `JAVA_HOME` points to JDK 17. The first Spark launch may need internet access to resolve the Delta Lake Maven package.

### 3. Run the full batch pipeline

From the repository root, after placing the dataset:

```bash
python -m streambharat.pipeline --list
python -m streambharat.pipeline --dry-run
python -m streambharat.pipeline --as-of 2026-10-31
```

The finite batch pipeline runs Q1–Q16, Q19 and Q20 in dependency order. Q17 and Q18 are continuous streams and are deliberately launched separately. The `--only` and `--skip` options support selected stages; selected stages assume their upstream Delta outputs already exist.

Examples:

```bash
python -m streambharat.pipeline --only Q1,Q2,Q4,Q5,Q6
python -m streambharat.pipeline --skip Q14,Q16
```

### 4. Run the two live streams

**Q17 — real-time trending:**

Terminal A:

```bash
python ott_stream_simulator.py --stream plays
```

Terminal B:

```bash
python -m streambharat.q17_trending --as-of 2026-11-02
```

**Q18 — live ad pacing and frequency caps:**

Terminal A:

```bash
python ott_stream_simulator.py --stream ads
```

Terminal B:

```bash
python -m streambharat.q18_live_ad_pacing
```

Stop the simulator and stream with Ctrl+C after the demonstration. The simulator writes JSON files to `data/stream/plays/` and `data/stream/ads/`.

### 5. Run tests

```bash
pytest -q
python -m compileall -q src/streambharat
```

Pure-Python business-rule tests do not require Spark. They do not prove that a Spark/Delta job has executed successfully against the full dataset.

## Implementation and validation status

All 20 question modules are now present, and the batch orchestrator wires their documented dependencies. Deterministic tests and Python compilation are passing. **Full Spark/Delta integration is still pending**: this build environment does not have PySpark installed, so no claim is made that the full 1.6-million-event jobs, live streams, Delta schema evolution, ML training or model metrics have been runtime-validated here.

Treat a question as complete for submission only after it has been executed against the course data, its output schemas and counts have been inspected, its business checks have passed, and its owner can explain it.

## Important repository documents

- [Project brief](docs/project-brief.md)
- [All 20 questions and ownership](docs/questions-and-ownership.md)
- [Implementation notes and output contracts for every question](docs/team-workstreams.md)
- [Dataset catalogue](docs/data-catalog.md)
- [Architecture](docs/architecture.md)
- [Integration plan and completion gates](docs/delivery-plan.md)
- [Q4 implementation design](docs/implementation/q4-incremental-ingestion.md)
- [Q17 implementation design](docs/implementation/q17-real-time-trending.md)
- [Q20 implementation design](docs/implementation/q20-churn-model.md)
- [Student-facing workbook](docs/source-materials/Project-4-StreamBharat-PySpark-Workbook.pdf)
- [Team distribution and implementation guide](docs/source-materials/StreamBharat-Team-Distribution-and-Implementation-Guide.pdf)

## Data integrity

All accounts, people, titles, advertisers and other entities in the supplied sample are fictional. Instructor-only answer/seed notes and dataset-generation scripts are not included. Raw dataset files and simulator output remain untracked by Git.
