# Real-Time Banking Lakehouse

A production-style **real-time banking data platform** that ingests transaction events through Kafka, processes them with PySpark Structured Streaming, validates data quality, lands data in Amazon S3, orchestrates the workflow with Apache Airflow, curates an Apache Iceberg table with AWS Glue, exposes analytics through Amazon Athena, and integrates **Claude API-based agentic data-quality diagnosis** into the failure workflow.

The project is designed as an end-to-end data-engineering portfolio system with reproducible infrastructure, local Docker execution, AWS-native curation, automated DQ controls, quarantine handling, tests, and operational evidence.

---

## Architecture

```text
                         +----------------------+
                         | Transaction Producer |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         | Apache Kafka          |
                         | Topic: txn_events     |
                         | 3 partitions          |
                         +----------+-----------+
                                    |
                                    v
                    +-------------------------------+
                    | PySpark Structured Streaming   |
                    | Spark 3.4.1                   |
                    +---------------+---------------+
                                    |
                 +------------------+------------------+
                 |                                     |
                 v                                     v
        +------------------+                 +------------------+
        | Source DQ        |                 | S3 Raw Zone      |
        | schema           |                 | durable landing  |
        | nulls            |                 +---------+--------+
        | duplicates       |                           |
        | domain rules     |                           v
        | amounts/times    |                 +------------------+
        +--------+---------+                 | Transformations  |
                 |                           | + enrichment     |
                 |                           +---------+--------+
                 |                                     |
                 +------------------+------------------+
                                    |
                                    v
                         +----------------------+
                         | Curated S3 + DQ      |
                         | reports / metrics    |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         | Apache Airflow        |
                         | LocalExecutor         |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         | DQ Validation Gate   |
                         +----------+-----------+
                                    |
                    +---------------+---------------+
                    |                               |
                  PASS                             FAIL
                    |                               |
                    v                               v
        +------------------------+       +------------------------+
        | AWS Glue 4.0 Curation  |       | Claude API Diagnosis   |
        | transformation + DQ    |       | root-cause hypotheses  |
        +-----------+------------+       | confidence / action    |
                    |                    +-----------+------------+
                    v                                |
        +------------------------+                    v
        | Apache Iceberg         |          +---------------------+
        | Glue Data Catalog      |          | Quarantine workflow |
        +-----------+------------+          +----------+----------+
                    |                                  |
          +---------+---------+                        v
          |                   |                 Failure notification
          v                   v
      +--------+          +---------+
      | Athena |          | BI layer |
      +--------+          +---------+

Infrastructure: Terraform
Runtime: Docker / Docker Compose
Cloud: AWS ap-south-1
```

---

## Key capabilities

### 1. Real-time ingestion

- Kafka topic `txn_events` used for transaction-event ingestion.
- Three Kafka partitions for parallel consumption.
- PySpark Structured Streaming consumer using Spark 3.4.1.
- AWS S3A integration for durable raw and curated storage.
- Bounded producer and bounded streaming execution under Airflow.
- Watermarking and streaming transformation support for late-arriving events.

### 2. Production-style Data Quality

The project applies DQ controls before publishing curated data.

Checks include:

- Schema-drift detection
- Null-rate validation
- Duplicate transaction detection
- Domain and business-rule validation
- Amount validity checks
- Timestamp validity checks
- Curated-schema validation
- Curated business-rule validation
- Statistical anomaly detection

DQ reports are structured and passed back to Airflow through the pipeline's DQ-report mechanism.

Anomaly detection is treated as an operational warning when configured as a soft check, while hard DQ failures route the batch away from normal publication.

### 3. Quarantine workflow

When a source or curated DQ failure occurs, the affected batch can be isolated instead of being silently published.

Typical S3 paths:

```text
s3a://<bucket>/banking/quarantine/
  batch_id=<id>/
    reason=source_dq_failure/

s3a://<bucket>/banking/quarantine/
  batch_id=<id>/
    reason=curated_dq_failure/
```

This creates an auditable separation between accepted data and failed data.

### 4. AWS lakehouse curation

AWS Glue 4.0 performs the cloud-side curation stage and publishes the curated dataset as an **Apache Iceberg** table.

The table is registered in the AWS Glue Data Catalog and is queryable through Amazon Athena.

Validated platform components include:

- Amazon S3
- AWS Glue Data Catalog
- AWS Glue 4.0
- Apache Iceberg
- Amazon Athena
- IAM roles and policies
- Athena workgroup configuration

### 5. Claude API agentic DQ diagnosis

The project includes an **LLM-assisted DQ diagnosis layer using the Claude API**.

The design intentionally separates **diagnosis from automated remediation**. The agent analyzes structured DQ failure context and produces a machine-readable diagnostic response such as:

```json
{
  "likely_cause": "Example root-cause hypothesis",
  "confidence": 0.91,
  "suggested_action": "Investigate the upstream transaction timestamp mapping",
  "is_actionable_now": true
}
```

The integration is designed to run from the Airflow failure path:

```text
DQ failure
    |
    v
Airflow failure branch
    |
    v
Claude diagnosis
    |
    +--> likely_cause
    +--> confidence
    +--> suggested_action
    +--> is_actionable_now
    |
    v
Quarantine / failure notification
```

The Claude integration code is part of the project implementation and is structured so that the same diagnosis function can be exercised independently from the Airflow task path.

**Important security rule:** API keys must be supplied through environment variables or another secret-management mechanism and must never be committed to Git.

---

## Airflow orchestration

The main DAG coordinates service checks, streaming execution, DQ validation, AWS Glue curation, diagnosis, quarantine, and notifications.

```text
start
  |
  v
health_check_kafka
  |
  +--------------------------+
  |                          |
  v                          v
start_producer       start_streaming_consumer
  |                          |
  +------------+-------------+
               |
               v
       dq_validation_gate
           /          \
       PASS            FAIL
        |                |
        v                v
run_glue_curation   agentic_dq_diagnosis
        |                |
        v                v
notify_success      quarantine_batch
        |                |
        |                v
        |           notify_failure
        |                |
        +-------+--------+
                |
                v
               end
```

The PASS path invokes the provisioned AWS Glue curation job rather than acting as a placeholder optimization stage.

The runtime is configured around PostgreSQL-backed Airflow metadata and `LocalExecutor` so independent producer and streaming-consumer tasks can execute concurrently.

---

## AWS services and infrastructure

Terraform provisions the core cloud infrastructure for the project.

### Managed resources

- S3 lakehouse bucket
- S3 versioning / public-access protection
- Glue database
- Glue raw-zone crawler
- Glue curation job
- Glue IAM role
- Athena workgroup
- Optional Redshift Serverless resources

### Region

```text
AWS Region: ap-south-1
```

### Example resources

```text
S3 bucket:
banking-pipeline-lakehouse-gkumar-2026

Glue database:
banking_pipeline_db

Glue job:
banking-pipeline-curate-transactions

Iceberg table:
banking_curated_iceberg

Athena workgroup:
banking-pipeline-workgroup
```

Redshift Serverless is intentionally kept as an **optional Terraform capability** and disabled by default. The core project does not depend on Redshift for successful execution.

---

## Data flow

A transaction follows this lifecycle:

```text
Kafka event
   |
   v
PySpark ingestion
   |
   +--> Source DQ
   |       |
   |       +--> FAIL --> quarantine
   |
   v
Raw S3
   |
   v
Transform / enrich
   |
   v
Curated DQ
   |
   +--> FAIL --> quarantine
   |
   v
Curated S3
   |
   v
AWS Glue 4.0
   |
   v
Apache Iceberg
   |
   v
Glue Data Catalog
   |
   v
Athena
```

---

## Repository structure

```text
realtime-banking-lakehouse/
│
├── producer/
│   └── transaction_producer.py
│
├── consumer/
│   ├── stream_consumer.py
│   └── transformations.py
│
├── dq/
│   ├── dq_checks.py
│   └── dq_alert.py
│
├── dags/
│   └── banking_pipeline_dag.py
│
├── glue_jobs/
│   └── curate_transactions_job.py
│
├── storage/
│   └── s3_writer.py
│
├── config/
│   └── config.yaml
│
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   ├── terraform.tfvars.example
│   ├── glue-args.json
│   └── deployer-iam-policy.json
│
├── tests/
│   ├── test_dq.py
│   └── test_transformations.py
│
├── docs/
│   └── screenshots/
│
├── Dockerfile.spark
├── docker-compose.yml
├── docker-compose.airflow.yml
├── requirements.txt
├── requirements.spark.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## Local development

### Prerequisites

Install or configure:

- Docker Desktop
- Git
- AWS CLI
- Terraform
- AWS credentials with the required S3 / Glue / Athena / IAM permissions

Do not commit credential files, API keys, Terraform state, or other secrets.

### Start Kafka / Spark infrastructure

```powershell
docker compose -f docker-compose.yml up -d
```

### Start Airflow / PostgreSQL

```powershell
docker compose -f docker-compose.airflow.yml up -d
```

### Check containers

```powershell
docker ps
```

### Trigger the DAG

```powershell
docker exec airflow airflow dags trigger banking_pipeline_dag
```

### Inspect DAGs

```powershell
docker exec airflow airflow dags list
```

---

## Terraform

From the repository root:

```powershell
terraform -chdir=terraform init
terraform -chdir=terraform validate
terraform -chdir=terraform plan
```

Apply the infrastructure only after reviewing the plan:

```powershell
terraform -chdir=terraform apply
```

The example variable file is intentionally safe to commit:

```text
terraform/terraform.tfvars.example
```

Real credentials and environment-specific values should stay outside Git or be supplied through approved secret mechanisms.

---

## AWS Glue deployment

The Glue script used by the curation job is:

```text
s3://banking-pipeline-lakehouse-gkumar-2026/scripts/curate_transactions_job.py
```

Example upload command:

```powershell
aws s3 cp .\glue_jobs\curate_transactions_job.py `
  s3://banking-pipeline-lakehouse-gkumar-2026/scripts/curate_transactions_job.py `
  --region ap-south-1
```

The Glue job creates or updates the curated Iceberg table registered in the Glue Data Catalog.

---

## Athena analytics

The curated table is exposed through:

```text
database: banking_pipeline_db
table:    banking_curated_iceberg
```

Example query:

```sql
SELECT COUNT(*)
FROM banking_pipeline_db.banking_curated_iceberg;
```

Athena uses the configured workgroup and S3 query-results location defined by Terraform.

---

## Testing

The repository contains unit tests for transformation and DQ behavior.

Run:

```powershell
pytest -q
```

For the Dockerized pipeline, validate the Airflow task directly when troubleshooting:

```powershell
docker exec airflow airflow tasks test banking_pipeline_dag health_check_kafka <execution-date>
```

Use the actual DAG run and AWS console evidence for end-to-end verification rather than relying only on unit tests.

---

## Validation evidence

The project has been validated across the main platform layers, including:

| Area | Evidence |
|---|---|
| Airflow | Successful end-to-end PASS-path DAG run |
| Kafka | `txn_events` topic with 3 partitions and active ISR |
| Streaming | PySpark Structured Streaming task completed successfully |
| Data Quality | DQ report received by Airflow and PASS branch executed |
| S3 | Raw, curated, DQ-report, Iceberg, and quarantine zones present |
| Quarantine | Actual S3 quarantine object verified for a source-DQ failure batch |
| AWS Glue | Glue 4.0 curation job completed successfully |
| Iceberg | `banking_curated_iceberg` registered in Glue Data Catalog |
| Athena | Curated Iceberg table successfully queried |
| Terraform | `plan` returned no infrastructure drift |
| Claude | Diagnosis integration implemented in the DQ failure path |

Current project evidence screenshots are stored under:

```text
docs/screenshots/
```

Recommended evidence set:

```text
Airflow E2E run
AWS Glue successful run
Glue Data Catalog Iceberg table
Athena result
Terraform refresh-only / plan
S3 lakehouse layout
Airflow DQ gate
Kafka UI topic
S3 quarantine object
```

---

## Operational design decisions

### Why Kafka?

Kafka provides durable, partitioned event ingestion and decouples transaction producers from downstream stream processing.

### Why PySpark Structured Streaming?

Spark provides scalable stream processing, schema-aware transformations, checkpointing, watermarking, and integration with the broader Spark ecosystem.

### Why S3?

S3 provides durable object storage and a natural raw/curated lakehouse boundary.

### Why Iceberg?

Apache Iceberg provides a table abstraction with schema evolution, partition-aware table management, and reliable analytical access on object storage.

### Why Glue Data Catalog?

The Data Catalog provides a centralized metastore for AWS analytics services and allows Athena to discover the curated table.

### Why separate DQ from transformation?

Separating validation from transformation makes data-quality decisions explicit, auditable, and easier to test.

### Why Claude for DQ diagnosis?

The LLM is used to accelerate **diagnosis** of hard DQ failures by turning structured failure context into a concise root-cause hypothesis and recommended next action. It is intentionally not granted autonomous write/remediation authority.

---

## Reliability and safety controls

- Bounded producer execution prevents orphan background producer processes.
- Bounded Spark execution allows Airflow to manage task lifecycle.
- DQ failures are routed to quarantine rather than silently published.
- Airflow branching is based on the actual DQ report.
- Terraform keeps cloud infrastructure reproducible.
- Optional infrastructure such as Redshift remains disabled by default.
- Secrets are expected to come from environment variables / secret stores, not source control.

---

## Security

Never commit:

```text
.env
AWS access keys
AWS secret keys
Anthropic API keys
Terraform state files
Terraform state backups
local database files
private credentials
```

Use:

```text
.env.example
```

as the safe configuration template.

If an API key is ever exposed in terminal output, logs, screenshots, or Git history, revoke and rotate it before publishing the repository.

---

## Project status

### Core platform

- Kafka ingestion: implemented
- PySpark streaming: implemented
- S3 raw / curated storage: implemented
- DQ framework: implemented
- Airflow orchestration: implemented
- AWS Glue curation: implemented
- Apache Iceberg: implemented
- Glue Data Catalog: implemented
- Athena analytics: implemented
- Terraform infrastructure: implemented
- Quarantine workflow: implemented
- Automated tests: implemented
- Claude API DQ diagnosis integration: implemented

### Validation note

The Claude diagnosis layer is implemented as part of the project architecture and failure workflow. A final live API smoke test can be completed separately using valid Anthropic account credits/credentials; the core repository integration does not depend on fabricating a successful external API response.

---

## Interview-ready discussion points

This project can be discussed in an interview at several levels:

### Data engineering

- Streaming ingestion and micro-batching
- Partitioning and parallelism
- Checkpointing and fault tolerance
- Schema drift
- DQ gating
- Quarantine design
- Idempotency
- Late-arriving data

### Spark

- Structured Streaming
- Kafka source connector
- S3A integration
- Watermarking
- Transformations
- Partition pruning
- File-size optimization

### Lakehouse

- S3 raw / curated zoning
- Apache Iceberg
- Glue Catalog
- Athena
- Schema evolution
- Table maintenance

### Orchestration

- Airflow DAG dependencies
- LocalExecutor
- failure branches
- XCom-based DQ handoff
- AWS Glue job orchestration

### Cloud / IaC

- Terraform resource lifecycle
- IAM roles and least privilege
- S3 / Glue / Athena integration
- repeatable AWS environments

### AI / Agentic data engineering

- Structured DQ failure context
- LLM-assisted diagnosis
- confidence and actionability fields
- human-in-the-loop remediation
- why diagnosis and remediation should remain separated

---

## Resume-ready summary

> Built a production-style real-time banking lakehouse using Kafka, PySpark Structured Streaming, Amazon S3, Apache Airflow, AWS Glue, Apache Iceberg, Glue Data Catalog, Athena, Terraform, automated data-quality gates, quarantine workflows, and Claude API-assisted DQ diagnosis.

---

## Future extensions

- Add a production secret manager such as AWS Secrets Manager for API credentials.
- Add a controlled Claude smoke-test workflow in CI/CD.
- Add CloudWatch-based operational alerting.
- Add schema-contract versioning.
- Add more Iceberg maintenance and compaction automation.
- Add a dedicated BI layer when required.
- Expand DQ lineage and historical quality metrics.

---

## License

Add the repository license of your choice before publishing a formal open-source license. No license is currently assumed by this README.
