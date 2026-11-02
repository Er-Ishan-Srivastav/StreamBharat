# Local data directory

Obtain `StreamBharat_dataset.zip` from the course-provided dataset archive. Extract the archive and copy its `data/` directory here, so the repository contains `data/raw/...` locally.

Expected layout:

```text
data/
├── raw/
│   ├── app/
│   ├── ads/
│   ├── billing/
│   ├── cdn/
│   ├── content/
│   ├── player/
│   ├── reference/
│   └── search/
├── stream/       # created by ott_stream_simulator.py
├── delta/        # Bronze/Silver/Gold Delta outputs
└── checkpoints/  # Structured Streaming checkpoints
```

Raw files are input-only. Do not hand-edit them. The `/raw`, `/stream`, `/delta` and checkpoint folders are ignored by Git so that large datasets and generated state are not committed.
