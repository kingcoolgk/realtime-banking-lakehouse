# 🏦 Real-Time Banking Lakehouse

### Kafka → PySpark → S3 → Data Quality → Airflow → AWS Glue → Apache Iceberg → Athena

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.5-231F20?logo=apachekafka&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.4.1-E25A1C?logo=apachespark&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.8.1-017CEE?logo=apacheairflow&logoColor=white)
![AWS S3](https://img.shields.io/badge/Amazon%20S3-Lakehouse-569A31?logo=amazons3&logoColor=white)
![AWS Glue](https://img.shields.io/badge/AWS%20Glue-4.0-FF9900?logo=amazonaws&logoColor=white)
![Iceberg](https://img.shields.io/badge/Apache%20Iceberg-Table%20Format-2B2B2B?logo=apache)
![Athena](https://img.shields.io/badge/Amazon%20Athena-Analytics-FF9900?logo=amazonaws&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-IaC-7B42BC?logo=terraform&logoColor=white)
![Claude](https://img.shields.io/badge/Claude-API%20DQ%20Diagnosis-D97757?logo=anthropic&logoColor=white)

> Production-style portfolio project for real-time banking data engineering, combining streaming ingestion, automated data-quality gates, AWS lakehouse curation, quarantine handling, and LLM-assisted DQ diagnosis.

---

## ✨ What it demonstrates

| Area | Implementation |
|---|---|
| Streaming | Kafka + PySpark Structured Streaming |
| Data Quality | Schema, null, duplicate, domain, timestamp, business-rule & anomaly checks |
| Orchestration | Apache Airflow + LocalExecutor |
| Lakehouse | S3 + Apache Iceberg + Glue Data Catalog |
| Analytics | Amazon Athena |
| Cloud / IaC | AWS + Terraform |
| AI-assisted DQ | Claude API diagnosis layer |

---

## 🏗️ Architecture

```mermaid
flowchart LR
    P[Transaction Producer] --> K[(Kafka<br/>txn_events<br/>3 partitions)]
    K --> S[PySpark Structured Streaming]

    S --> DQ1{Source DQ}
    DQ1 -- PASS --> R[(S3 Raw)]
    DQ1 -- FAIL --> Q1[(S3 Quarantine)]

    R --> T[Transform + Enrich]
    T --> DQ2{Curated DQ}

    DQ2 -- PASS --> C[(S3 Curated)]
    DQ2 -- FAIL --> Q2[(S3 Quarantine)]

    C --> G[AWS Glue 4.0]
    G --> I[(Apache Iceberg)]
    I --> CAT[Glue Data Catalog]
    CAT --> ATH[Athena]

    DQ1 --> AF[Airflow DQ Gate]
    DQ2 --> AF
    AF -- FAIL --> CL[Claude API<br/>DQ Diagnosis]
    CL --> Q2
```

---

## 🔄 End-to-End Flow

```text
Kafka Event
    ↓
PySpark Streaming
    ↓
Source DQ
    ├── FAIL → Quarantine
    └── PASS
           ↓
       S3 Raw
           ↓
    Transform + Enrich
           ↓
      Curated DQ
       ├── FAIL → Quarantine
       └── PASS
              ↓
          AWS Glue
              ↓
       Apache Iceberg
              ↓
       Glue Data Catalog
              ↓
           Athena
```

---

## 🤖 Claude-powered DQ Diagnosis

The DQ failure path includes an **implemented Claude API diagnosis component**.

It converts structured DQ failures into a machine-readable diagnosis:

```json
{
  "likely_cause": "Upstream timestamp validation failure",
  "confidence": 0.91,
  "suggested_action": "Inspect source timestamp mapping",
  "is_actionable_now": true
}
```

**Design principle:** Claude is used for diagnosis and recommendations; data quarantine and pipeline control remain deterministic.

> The integration is implemented in the repository. A final live API smoke test can be completed separately with valid Anthropic credits/credentials.

---

## 🛡️ Data Quality

The pipeline checks the data before curated publication:

- Schema drift
- Null-rate thresholds
- Duplicate transactions
- Domain / business rules
- Amount validity
- Timestamp validity
- Curated schema
- Curated business rules
- Statistical anomaly detection

DQ results are passed to Airflow so the DAG can branch on the **actual validation result**.

---

## 📸 Live Project Evidence

### Airflow — End-to-End Pipeline

![Airflow DAG](docs/screenshots/01_Airflow_dag_final.png)

### AWS Glue — Successful Curation

![AWS Glue](docs/screenshots/02_aws_glue_success.png)

### Glue Data Catalog — Apache Iceberg

![Iceberg Catalog](docs/screenshots/03_glue_catalog_iceberg.png)

### Athena — Curated Iceberg Query

![Athena](docs/screenshots/04_athena_iceberg_count.png)

### Terraform — Infrastructure Validation

![Terraform](docs/screenshots/05_terraform_refresh.png)

### S3 — Lakehouse Layout

![S3 Lakehouse](docs/screenshots/06_s3_lakehouse_structure.png)

### Airflow — DQ Validation Gate

![Airflow DQ](docs/screenshots/07_airflow_dq_validation.png)

### Kafka — `txn_events`

![Kafka](docs/screenshots/08_kafka_txn_events.png)

### Quarantine — DQ Failure Evidence

![Quarantine](docs/screenshots/09_dq_quarantine_evidence.png)

---

## 📊 Validated Demo Snapshot

| Metric | Demonstrated |
|---|---:|
| Kafka partitions | **3** |
| Curated Iceberg records queried | **2.13M+** |
| AWS Glue version | **4.0** |
| Curated table | **`banking_curated_iceberg`** |
| DQ categories | **9+** |
| Quarantine path | **Verified in S3** |
| Infrastructure drift | **None at final Terraform validation** |

---

## 🛠️ Technology Stack

| Layer | Technology |
|---|---|
| Ingestion | Apache Kafka |
| Processing | PySpark Structured Streaming |
| Storage | Amazon S3 |
| DQ | Custom PySpark DQ framework |
| Orchestration | Apache Airflow |
| Cloud Curation | AWS Glue 4.0 |
| Table Format | Apache Iceberg |
| Metadata | AWS Glue Data Catalog |
| Query | Amazon Athena |
| Infrastructure | Terraform |
| Runtime | Docker / Docker Compose |
| AI-assisted diagnosis | Claude API |
| Testing | Pytest |

---

## ☁️ AWS Resources

```text
Region
└── ap-south-1

S3
├── raw
├── curated
├── dq_reports
├── quarantine
├── iceberg_curated
└── athena-results

Glue
├── banking_pipeline_db
├── banking-pipeline-curate-transactions
└── banking_curated_iceberg

Athena
└── banking-pipeline-workgroup
```

---

## 📁 Project Structure

```text
realtime-banking-lakehouse/
├── producer/
├── consumer/
├── dq/
├── dags/
├── glue_jobs/
├── storage/
├── config/
├── terraform/
├── tests/
├── docs/
│   └── screenshots/
├── Dockerfile.spark
├── docker-compose.yml
├── docker-compose.airflow.yml
├── requirements.txt
├── requirements.spark.txt
├── .env.example
└── README.md
```

---

## ▶️ Run Locally

### 1. Start Kafka / Spark

```powershell
docker compose -f docker-compose.yml up -d
```

### 2. Start Airflow / PostgreSQL

```powershell
docker compose -f docker-compose.airflow.yml up -d
```

### 3. Validate containers

```powershell
docker ps
```

### 4. Trigger the DAG

```powershell
docker exec airflow airflow dags trigger banking_pipeline_dag
```

### 5. Run tests

```powershell
pytest -q
```

---

## 🏗️ Terraform

```powershell
terraform -chdir=terraform init
terraform -chdir=terraform validate
terraform -chdir=terraform plan
```

Apply only after reviewing the plan:

```powershell
terraform -chdir=terraform apply
```

Secrets and Terraform state are intentionally excluded from Git.

---

## 🎯 Key Engineering Decisions

**Kafka** — durable partitioned event ingestion and producer/consumer decoupling.

**PySpark Structured Streaming** — distributed processing with Spark-native streaming, transformations and AWS storage integration.

**S3 + Iceberg** — durable object storage plus an analytical table abstraction.

**Airflow** — dependency-aware orchestration and explicit PASS/FAIL branching.

**Quarantine** — isolates failed batches instead of silently publishing bad data.

**Claude** — assists engineers with DQ root-cause diagnosis while deterministic pipeline logic controls publication and quarantine.

---

## ✅ Project Status

| Capability | Status |
|---|---|
| Kafka ingestion | ✅ Implemented |
| PySpark streaming | ✅ Implemented |
| S3 raw / curated zones | ✅ Implemented |
| Automated DQ | ✅ Implemented |
| Airflow orchestration | ✅ Implemented |
| AWS Glue curation | ✅ Implemented |
| Apache Iceberg | ✅ Implemented |
| Glue Data Catalog | ✅ Implemented |
| Athena analytics | ✅ Implemented |
| Quarantine workflow | ✅ Implemented |
| Terraform IaC | ✅ Implemented |
| Tests | ✅ Included |
| Claude DQ diagnosis | ✅ Implemented |
| Live Claude API smoke test | ⏳ Final external validation |

---

## 🔐 Security

Never commit:

```text
.env
AWS credentials
Anthropic API keys
Terraform state / backups
local databases
private credentials
```

Use `.env.example` as the safe configuration template.

---

## 👤 Author

**Gaurav Kumar**
Data Engineer | IIT Kharagpur

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?logo=linkedin&logoColor=white)](https://linkedin.com/in/gaurav-kumar-999710188)
[![GitHub](https://img.shields.io/badge/GitHub-kingcoolgk-181717?logo=github&logoColor=white)](https://github.com/kingcoolgk)

---

### Repository

**https://github.com/kingcoolgk/realtime-banking-lakehouse**
