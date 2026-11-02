# StreamBharat — OTT Streaming Analytics Platform

**Project 4 · PySpark Industry Projects · Team implementation workspace**

StreamBharat is an end-to-end data engineering and analytics project for a fictional Indian OTT platform. The project uses PySpark, Spark Structured Streaming and Delta Lake to turn application, playback, billing, advertising, catalogue, search and CDN data into trusted analytical datasets and ML outputs.

> **Active assignment:** Workbook 2 — *Project 4: StreamBharat: OTT Streaming Analytics Platform*. This repository follows the file-based Spark streaming simulator supplied with that workbook. Kafka, Airflow, PostgreSQL and Metabase are **not** required by this assignment's core implementation.

## Project at a glance

- **Business domains:** playback and engagement, content catalogue and licensing, subscriptions and revenue, ads, CDN delivery, recommendations, data quality and churn.
- **Processing pattern:** raw files → Bronze → Silver → Gold / ML outputs.
- **Scale of supplied sample:** 2,500 accounts and 4,736 profiles; approximately 1.6 million playback events.
- **Dataset periods:** subscriptions May 2025–October 2026; playback, search and advertising July–October 2026; CDN logs September 2026.
- **Primary stack:** Python 3.10/3.11, Java 17, PySpark 3.5.3, Delta Lake 3.2.0, pytest and Spark MLlib.

## Repository map

```text
StreamBharat-Project/
├── README.md
├── pyproject.toml                 # Pinned Spark/Delta dependencies and pytest config
├── requirements.txt
├── ott_stream_simulator.py        # Supplied Q17/Q18 file-stream simulator
├── data/
│   └── README.md                  # How to obtain and place the dataset
├── docs/
│   ├── project-brief.md           # Business context, scope and goals
│   ├── questions-and-ownership.md # All 20 questions and six-person allocation
│   ├── data-catalog.md            # Dataset inventory, formats, keys and conventions
│   ├── architecture.md            # Bronze/Silver/Gold and streaming architecture
│   ├── delivery-plan.md           # Integration contracts, quality gates and milestones
│   ├── implementation/            # Design notes for Q4, Q17 and Q20
│   └── source-materials/          # Student-facing workbook, team guide and dataset README
├── src/streambharat/
│   ├── spark_session.py
│   ├── q4_incremental_ingestion.py
│   ├── q17_trending.py
│   ├── q20_churn.py
│   ├── pipeline.py
│   └── logic.py
├── tests/
├── scripts/
└── .github/workflows/tests.yml
```

The repository currently contains Ishan's implementation for Q4, Q17 and Q20. The other workstreams are documented with owners and expected interfaces; their implementation modules should be added by the respective owners as they are completed. The repo does not claim that all 20 questions are already implemented.

## Team ownership

| Member | Workstream | Questions |
|---|---|---|
| Ishan | Project lead; incremental ingestion, streaming, integration | Q4, Q17, Q20 |
| Rutik | Playback engineering and Spark optimization | Q2, Q5, Q12, Q14 |
| Harshvardhan | Content modelling and discovery | Q1, Q8, Q10, Q16 |
| Kshitij Jaisawal | Subscriptions, finance and live ad pacing | Q6, Q11, Q15, Q18 |
| Siddhi | CDN processing and search analytics | Q3, Q7 |
| Ankita | Engagement metrics, advertising and data quality | Q9, Q13, Q19 |

Q20 end-to-end integration is owned by Ishan, with Rutik providing technical support on the churn implementation.

## Get started

### 1. Obtain the dataset

The dataset archive is distributed separately as `StreamBharat_dataset.zip` (approximately 36 MB compressed). Extract its `data/` directory into the repository root. Do not modify the raw files. See [`data/README.md`](data/README.md) and [`docs/data-catalog.md`](docs/data-catalog.md).

Expected local paths include:

```text
data/raw/player/*.json
data/raw/billing/subscription_events.json
data/raw/app/users.json
data/raw/content/catalog_2026-10-01.json
data/raw/reference/*.csv
```

The raw dataset is intentionally excluded from Git to avoid committing large input files. `.gitignore` already excludes it.

### 2. Configure the runtime

Use Python 3.10 or 3.11 and Java 17. From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate             # Windows: .venv\\Scripts\\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[test]"
```

Ensure `JAVA_HOME` points to JDK 17. The first Spark launch may need internet access to resolve the Delta Lake Maven package.

### 3. Run Ishan's modules

Run from the repository root after extracting the dataset:

```bash
# Q4 — incremental playback files → Bronze Delta
python -m streambharat.q4_incremental_ingestion

# Q20 — train/evaluate churn model and score active accounts
python -m streambharat.q20_churn --as-of 2026-10-31

# Q4 → Q20 batch orchestration
python -m streambharat.pipeline --as-of 2026-10-31
```

Q17 is a live demonstration. Open two terminals in the repository root:

```bash
# Terminal A: simulator
python ott_stream_simulator.py --stream plays

# Terminal B: trending stream
python -m streambharat.q17_trending --as-of 2026-11-02
```

The simulator emits JSON files into `data/stream/plays/`. Stop both processes with Ctrl+C after the demonstration.

### 4. Run tests

```bash
pytest -q
```

The deterministic business-rule tests do not require a running Spark cluster. They are not a substitute for executing the Spark/Delta jobs against the actual data.

## Implementation status

| Area | Status | Notes |
|---|---|---|
| Q4 incremental ingestion | Implemented; runtime validation pending | Explicit schema, `availableNow`, checkpointing, idempotent Delta merge and additive `cast_device` support |
| Q17 real-time trending | Implemented; runtime validation pending | 10-minute window, 1-minute slide, bounded event buffer, 15-minute late allowance, baseline and language-level top-N |
| Q20 churn model | Implemented; runtime validation pending | Matured labels, pre-anchor features, chronological evaluation, MLlib logistic regression and value-weighted risk ranking |
| Q1–Q3, Q5–Q16, Q18–Q19 | Team workstreams | See question registry and owners |
| Full integrated release | In progress | Requires Silver playback contract from Q2, all team-owned Gold outputs, and full Spark validation |

A previous local check passed 5/5 deterministic unit tests and Python syntax checks. PySpark/Delta execution was not available in the build environment at packaging time. Do not report Spark runtime success or model metrics until the jobs have been run and their outputs inspected.

## Important links inside this repository

- [Project brief](docs/project-brief.md)
- [All 20 questions and ownership](docs/questions-and-ownership.md)
- [Dataset catalogue](docs/data-catalog.md)
- [Architecture](docs/architecture.md)
- [Delivery plan and integration contracts](docs/delivery-plan.md)
- [Q4 design](docs/implementation/q4-incremental-ingestion.md)
- [Q17 design](docs/implementation/q17-real-time-trending.md)
- [Q20 design](docs/implementation/q20-churn-model.md)
- [Student-facing source workbook](docs/source-materials/Project-4-StreamBharat-PySpark-Workbook.pdf)
- [Team distribution and implementation guide](docs/source-materials/StreamBharat-Team-Distribution-and-Implementation-Guide.pdf)

## Data and academic integrity note

All names, accounts, titles, advertisers and other entities in the supplied dataset are fictional. The instructor-only answer/seed notes and dataset-generation scripts are intentionally not included in this student-facing repository. Raw data is distributed separately and must remain unchanged.
