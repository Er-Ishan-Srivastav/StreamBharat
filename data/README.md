# Local data directory

All datasets and runtime outputs are local-only. Do not commit raw source files, generated events, external data snapshots, Delta tables, checkpoints or credentials.

## Data locations

~~~text
data/
├── external/
│   ├── wikidata/       # fetched CC0 content metadata snapshot
│   └── wikimedia/      # fetched CC0 Wikipedia pageview sidecar
├── hybrid/
│   └── raw/            # reproducible hybrid OTT source dataset
├── demo/
│   └── raw/            # compact 5–20 user Kafka integration fixture
├── raw/                # optional original course dataset, if later recovered
├── stream/             # simulator output
├── delta/              # batch Bronze/Silver/Gold outputs
└── checkpoints/        # Structured Streaming checkpoints
~~~

## Recommended path

The original course dataset is currently unavailable. A contributor can reproduce the hybrid lab without it:

1. Run python3 scripts/fetch_wikidata_catalog.py.
2. Run python3 scripts/fetch_wikimedia_pageviews.py.
3. Run python3 scripts/generate_hybrid_dataset.py --output data/hybrid/raw --start-date YYYY-MM-DD --days 7 --users 500 --seed 42.

Read the Hybrid Data Lab guide at ../docs/hybrid-data-lab.md for source semantics, provenance, expected formats and the Kafka replay command.

data/hybrid/raw/ is not the original course dataset. It has a similar source-domain layout, but the synthetic generator does not claim exact course row counts or full Q1–Q20 compatibility. The original course preflight must remain strict and must never be weakened to accept generated data.

The compact Kafka integration fixture remains separate under data/demo/raw/. Use it for the already-recorded 20-user local acceptance path.

## Raw-data rules

- Treat fetched and generated input files as immutable after a run.
- Use a new output directory or the explicit overwrite flag to create a replacement run.
- Inspect manifest.json and verify SHA-256 checksums before processing.
- Keep public-source and synthetic provenance markers intact.
- Store Spark/Delta outputs and checkpoints outside source directories.
