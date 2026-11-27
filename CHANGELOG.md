# Changelog

This changelog records repository-level milestones. It is not a claim that every listed module has passed full-course-data acceptance.

## Unreleased — repository presentation and onboarding

- Reworked the README into a project showcase with business outcomes, architecture diagrams, verified acceptance snapshot, setup instructions and documentation navigation.
- Added a solution overview mapping business needs to the Q1–Q20 implementation scope.
- Expanded contributor guidance and pull-request acceptance checks.
- Clarified the current validation boundary and operational limitations.

## 1.0.0 — local Kafka and BI integration baseline

- Integrated the Kafka-based ETL extension with source replay, topic contracts, Bronze/DLQ processing and retained-offset reconciliation.
- Added Silver player/subscription processing and Gold engagement, session, advertising and finance transformations.
- Added PostgreSQL staging/upsert serving and Metabase SQL questions/dashboards.
- Added Docker Compose definitions for the core local stack and optional Airflow orchestration.
- Added a deterministic synthetic demo generator, output verifiers and a local acceptance workflow.

## 0.1.0 — initial project workspace

- Added Workbook 2 project brief, dataset inventory and all 20 question summaries.
- Added six-person question ownership and integration contracts.
- Added Q4, Q17 and Q20 implementation modules, shared Spark helper, orchestration and tests.
- Added supplied file-stream simulator and student-facing workbook/team guide.
- Added Python package configuration and CI workflow.
- Excluded instructor-only materials and raw dataset binaries.
