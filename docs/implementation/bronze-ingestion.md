# Seven-topic Kafka Bronze ingestion

Module: \`streambharat.bronze_ingest\`.

The job creates one independent Structured Streaming query for each source topic:
- \`ott.player.events\`
- \`ott.billing.subscr\`
- \`ott.billing.settle\`
- \`ott.app.search\`
- \`ott.app.ratings\`
- \`ott.ads.breaks\`
- \`ott.ads.impressions\`

It preserves the raw Kafka value as a string and records topic, partition, offset, Kafka timestamp, key, headers, ingestion timestamp and ingestion date. Each topic has a separate Delta destination and checkpoint under \`lake/bronze\` and \`lake/_checkpoints\`.

Example:

\`\`\`bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \\
  src/streambharat/bronze_ingest.py --available-now
\`\`\`

Continuous mode is the default. Use \`--available-now\` for bounded catch-up/replay. Keep checkpoints for normal recovery; use a new checkpoint only for a deliberate replay plan.

The source rows are not parsed in Bronze. Parsing, schema validation and reason-coded quarantine belong to Silver/DLQ, preserving the audit copy.
