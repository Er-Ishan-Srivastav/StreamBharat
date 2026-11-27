# StreamBharat — Data Engineering Field Guide

This is a practical learning path through the data lifecycle implemented by StreamBharat. The goal is not merely to start the stack; it is to understand why each stage exists, what it guarantees, what can go wrong, and how to prove the output is trustworthy.

Start with the [Hybrid Data Lab](hybrid-data-lab.md) to create the inputs. Use a small deterministic dataset for your first experiments. Increase volume only after you understand the smaller path.

## 1. The lifecycle

~~~mermaid
flowchart TD
    A["Acquire public metadata<br/>Wikidata"] --> B["Generate source activity<br/>seeded OTT simulation"]
    B --> C["Source contracts<br/>JSONL / CSV / CDN logs"]
    C --> D["Kafka topics<br/>event transport"]
    D --> E["Bronze<br/>immutable source envelope"]
    D --> X["Validation and DLQ<br/>reason-coded exceptions"]
    E --> F["Silver<br/>typed, clean, canonical"]
    F --> G["Gold<br/>business facts and aggregates"]
    G --> H["PostgreSQL<br/>serving model"]
    H --> I["Metabase<br/>business and operational insight"]
    C --> J["Manifest and preflight<br/>hashes, counts, keys"]
    E --> K["Offset reconciliation"]
    F --> L["Data quality tests"]
    G --> M["Business reconciliation"]
~~~

The stages are separate because each solves a different problem:

| Stage | Main question | Output |
|---|---|---|
| Acquisition | Where did this information come from, and may we reuse it? | Source snapshot + source terms |
| Generation | Which scenario assumptions create event activity? | Deterministic source files |
| Contract | What does a valid source record look like? | Explicit formats, fields, keys and timestamp semantics |
| Transport | How are events delivered, partitioned and replayed? | Kafka topic, key, partition and offset |
| Bronze | What exactly arrived? | Append-only raw payload and Kafka metadata |
| DLQ | Which records failed validation, and why? | Quarantined payload + reason code |
| Silver | What canonical business event should downstream jobs use? | Typed, deduplicated, corrected records |
| Gold | What business fact or metric does the cleaned data support? | Facts, aggregates and model features |
| Serving | How should analysts query it reliably? | PostgreSQL tables and views |
| BI | What decision does the metric inform? | Dashboard, question and documented definition |
| Evidence | How can another engineer reproduce and verify the result? | Manifest, row counts, hashes, tests and run report |

## 2. Acquire and preserve the source

The project uses two public sources for different purposes.

**Wikidata** supplies content descriptions. The original QID remains attached to each record so a title can be traced to its source. The project maps QIDs to StreamBharat content IDs, but the new content ID is a synthetic business key.

**Wikimedia pageviews** supplies article-interest data. It remains an external signal, not playback. The pageviews adapter is joined to StreamBharat content by Wikidata QID in scripts/build_public_interest_features.py. The output contains article_pageviews and rolling_7d_article_pageviews. It must never be labelled as plays, unique viewers, watch time or OTT engagement.

Inspect local source snapshots:

~~~bash
python3 -m json.tool data/external/wikidata/catalog.json
head -n 5 data/external/wikimedia/pageviews.jsonl
~~~

Fetch the source snapshots once and retain their retrieval timestamps and licence declarations. The generator then reads the cached snapshot; generation does not depend on the public API being live.

## 3. Understand the source contract before processing

A source contract describes more than column names. It includes format, keys, timestamp meaning, expected cardinality, allowed values, nullability and relationships.

Example playback JSONL record:

~~~json
{
  "event_id": "EVT000000000001",
  "user_id": "U000001",
  "profile_id": "U000001-P1",
  "device_id": "DV-U000001-P1",
  "content_id": "CNT00001",
  "event_type": "heartbeat",
  "position_sec": 300,
  "client_ts": "2026-11-01T10:05:00",
  "server_ts": "2026-11-01T10:05:19",
  "ip_city": "Pune",
  "device_type": "android",
  "app_version": "7.4.0",
  "download_id": null,
  "provenance": "synthetic_generated"
}
~~~

Subscription event JSONL has an account key and lifecycle event time. A payment failure and a renewal are separate events; neither is a settlement.

Gateway settlement CSV represents a gateway's report of money settled. It must be reconciled to subscription charge events using user, amount, event type and a bounded time window. A successful subscription event is not by itself proof that payment settled.

CDN logs arrive in two provider formats. A parser must normalize both before calculating request counts, egress, status rates and cost.

The dedicated hybrid preflight validates source files and relationships:

~~~bash
python3 scripts/preflight_hybrid_dataset.py \
  --data-root data/hybrid/raw \
  --report logs/hybrid_preflight/latest.json
~~~

Do not skip preflight just because a file opens in a text editor. A dataset can be syntactically valid but referentially inconsistent.

## 4. Event time, arrival time and processing time

These timestamps answer different questions:

- **Client/event time:** when the device says the viewing action happened.
- **Server/arrival time:** when the platform received the event.
- **Processing time:** when a consumer or Spark job processed the record.

For an online event, arrival may be seconds after the action. For an offline download, events may arrive days later. A file named by arrival day therefore must not be interpreted as a partition by viewing day.

Example:

| Clock | Timestamp | Interpretation |
|---|---|---|
| client_ts | 10:00 | Device claims playback happened at 10:00 |
| server_ts | 10:00:19 | Platform received the event at 10:00:19 |
| ingested_at | 10:00:24 | Bronze accepted the Kafka record at 10:00:24 |

This distinction affects daily active users, sessions, late-event handling, windowed streaming, freshness and replay. The generator also assigns stable per-device clock offsets. Silver estimates a reliable online offset before applying a correction; it must not treat late offline upload time as viewing time.

## 5. Kafka — transport, not storage

The source replay producer publishes records to seven topic contracts:

| Source | Topic |
|---|---|
| Playback | ott.player.events |
| Subscription lifecycle | ott.billing.subscr |
| Gateway settlements | ott.billing.settle |
| Search | ott.app.search |
| Ratings | ott.app.ratings |
| Ad breaks | ott.ads.breaks |
| Ad impressions | ott.ads.impressions |

A Kafka record has a key and a value. The value contains the source payload; the key controls partition affinity and ordering for records with that key. A partition offset identifies a record's position within one partition. Offsets are not globally unique.

Replay is not a one-time file copy. It makes event-time order, arrival-time simulation, source keys, offsets, retries and idempotency observable. A test run should record the topic, partition, starting offset, ending offset and message count.

## 6. Bronze — preserve what arrived

Bronze is append-only source storage. The existing Kafka-to-Delta ingestion retains:

- raw payload value;
- topic name;
- Kafka partition and offset;
- Kafka timestamp;
- Kafka message key and headers;
- ingestion timestamp and ingestion date.

A raw envelope is retained so a transformation can be corrected and re-run without losing the original input. If a Silver result is challenged, an engineer can follow its lineage back to the topic, partition and offset.

Bronze is not the place to silently fix values or drop bad records. Preserve the arrival record.

## 7. Validation and the dead-letter queue

Validation tests records against source contracts. Examples include required IDs, allowed event types, parseable timestamps, ratings within 1–5, non-negative playback positions, and required source references.

A record that fails is routed to the DLQ with a reason code and source lineage. This is preferable to dropping it because the failure count and rejected payload remain inspectable.

The practical rule is:

**Accepted source rows + quarantined source rows = all source rows received**, after accounting for deliberate filtering or duplicate handling at the correct stage.

A DLQ count is not a generic data-quality score. Group it by feed and reason so an operator can tell whether a producer, schema version or business rule changed.

## 8. Silver — establish canonical meaning

Silver converts source-specific details into stable, reusable data contracts.

Examples in this repository:
- Player JSON v1/v2 parsing, deduplication by event ID, offline/online classification and device-clock correction.
- Subscription event parsing and type-2 history of state changes, including the seven-day payment-failure grace period.
- Ad-break/impression reconciliation using source IDs and event-time constraints.
- CDN normalization from two provider log formats.
- Timestamp typing, standardised field names and explicit quarantine paths.

Silver should preserve lineage while documenting every changed field. A corrected event timestamp should not erase the original client or server timestamps.

## 9. Gold — answer a defined business question

Gold tables are not arbitrary group-bys. Each should have a metric definition, a grain and a business key.

| Gold product | Grain | Business use |
|---|---|---|
| Daily watch time | Date × profile × content | Consumption and completion |
| Daily active users | Date × metric definition | Product activity |
| Viewing sessions | One row per constructed session | Session length and drop-off |
| Ad delivery | One row per reconciled break/impression outcome | Fill and campaign measurement |
| Revenue reconciliation | One row per charge | Cash settlement control |
| Quarterly royalties | Quarter × partner | Partner payout |
| CDN egress | Date × provider × region | Network cost and reliability |
| DLQ metrics | Date × topic × reason | Source quality operations |

A metric must document its numerator, denominator, time basis, exclusions and handling of missing data. For example, ad fill rate must state whether the denominator is all available breaks or only eligible ad opportunities, and whether the metric is platform-wide or campaign-specific.

## 10. Serving and dashboards

PostgreSQL provides a stable query layer on top of Gold tables. Metabase questions and dashboards should query documented warehouse views or tables, not reimplement business logic independently in each chart.

For every dashboard card, a contributor should be able to answer:

1. Which source fields contributed to the metric?
2. Which transformations and filters were applied?
3. What is the row grain?
4. Which date or timestamp determines the reporting period?
5. What is excluded or quarantined?
6. What reconciliation or quality test proves the result?

If a dashboard number cannot be traced to a query and a source contract, it is not an auditable metric.

## 11. The end-to-end learning exercise

Use the following sequence. Do not skip a stage.

1. Fetch the public catalogue snapshot.
2. Generate a small dataset with a fixed seed.
3. Run the hybrid preflight and inspect its JSON report.
4. Open one source row from playback, a subscription event, one settlement, one ad break and one CDN log from each provider.
5. Replay the seven event feeds into a fresh Kafka environment.
6. Inspect topic records and offsets in Kafka UI.
7. Run Bronze ingestion and compare source counts with Bronze counts and offsets.
8. Run DLQ validators and inspect at least one accepted and one rejected record.
9. Build Silver and inspect a deduplicated or corrected event.
10. Build Gold and reconcile the business totals.
11. Publish outputs to PostgreSQL and run the committed SQL questions.
12. Open Metabase and trace a dashboard card back to SQL and Gold.
13. Change the generator seed or scale and repeat. Record the new manifest and run ID.

For every step, save both the command and its result. A screenshot is useful for explanation, but retain machine-readable counts, hashes and validation reports as evidence.

## 12. Concepts to explain after completing the lab

| Concept | Explain it without relying on a definition |
|---|---|
| Schema contract | What a producer promises and a consumer is allowed to expect |
| Primary/foreign key | How entities and facts are uniquely identified and related |
| Event time vs processing time | Why the timestamp used changes the metric |
| Late-arriving data | Why an event can arrive after its business date |
| Idempotency | Why retrying the same source must not double-count it |
| Offset/checkpoint | How a stream resumes and what state it remembers |
| Bronze/Silver/Gold | Why raw preservation, conformance and business aggregation are separate |
| DLQ | How failed records remain visible and recoverable |
| SCD Type 2 | How state history is retained instead of overwriting the past |
| Reconciliation | How independently reported facts are compared and exceptions surfaced |
| Data lineage | How an output can be traced to its inputs and code/config |
| Data quality | Which rule is tested, at what grain, and what happens on failure |
| Metric grain | What one row represents and what the aggregate can safely be compared with |

A successful demo is not just a screen with charts. It is a repeatable chain of evidence from source to decision.
