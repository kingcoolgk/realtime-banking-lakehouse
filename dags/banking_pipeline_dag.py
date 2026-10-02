"""
dags/banking_pipeline_dag.py
------------------------------
Master Airflow DAG for the Real-Time Banking Transaction Pipeline.

DAG flow:

    start
      |
      v
    health_check_kafka
      |
      v
    start_producer
      |
      v
    start_streaming_consumer
      |
      v
    dq_validation_gate
      |
      +---- PASS ---> compact_s3_partitions ---> notify_success ---> end
      |
      +---- FAIL ---> agentic_dq_diagnosis ---> quarantine_batch ---> notify_failure ---> end

The Spark streaming job emits:
    AIRFLOW_DQ_REPORT=<json>

The Airflow task captures that report in XCom and uses it for
the DQ validation branch.
"""

from datetime import datetime, timedelta
import json
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


# ---------------------------------------------------------------------------
# Airflow import path
# ---------------------------------------------------------------------------

if "/opt/airflow" not in sys.path:
    sys.path.insert(0, "/opt/airflow")


# ---------------------------------------------------------------------------
# Default Args
# ---------------------------------------------------------------------------

default_args = {
    "owner": "gaurav.kumar",
    "depends_on_past": False,
    "email": ["gkumar318raja@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
}


# ---------------------------------------------------------------------------
# Python Callables
# ---------------------------------------------------------------------------

def check_kafka_health(**context):
    """
    Verify Kafka broker is reachable and the txn_events topic exists.
    """
    from kafka import KafkaAdminClient
    import yaml

    with open("/opt/airflow/config/config.yaml", "r") as f:
        cfg = yaml.safe_load(f)

    broker = cfg["kafka"]["broker"]
    topic = cfg["kafka"]["topic"]

    admin = KafkaAdminClient(
        bootstrap_servers=broker,
        request_timeout_ms=5000,
    )

    try:
        topics = admin.list_topics()

        if topic not in topics:
            raise ValueError(
                f"Kafka topic '{topic}' not found on broker {broker}. "
                f"Available topics: {topics}"
            )

        print(
            f"Kafka health check PASSED. "
            f"Broker: {broker} | Topic: {topic} exists."
        )

    finally:
        admin.close()


def run_spark_streaming(**context):
    """
    Execute the Spark streaming/DQ job inside the Spark container,
    capture the AIRFLOW_DQ_REPORT emitted by the job, and store it in XCom.
    """
    import docker

    client = docker.from_env()
    spark_container = client.containers.get("spark")

    result = spark_container.exec_run(
        [
            "bash",
            "-lc",
            (
                "cd /opt/bitnami/spark && "
                "mkdir -p /tmp/ivy2 && "
                "timeout 300 "
                "/opt/spark/bin/spark-submit "
                "--conf spark.jars.ivy=/tmp/ivy2 "
                "--packages "
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
                "/opt/bitnami/spark/consumer/stream_consumer.py"
            ),
        ],
        stdout=True,
        stderr=True,
    )

    output = result.output.decode("utf-8", errors="replace")
    print(output)

    if result.exit_code not in (0, 124):
        raise RuntimeError(
            f"Spark streaming failed with exit code {result.exit_code}"
        )

    dq_report = None

    for line in reversed(output.splitlines()):
        if "AIRFLOW_DQ_REPORT=" in line:
            raw_report = line.split("AIRFLOW_DQ_REPORT=", 1)[1].strip()
            dq_report = json.loads(raw_report)
            break

    if dq_report is None:
        raise RuntimeError(
            "Spark job completed but AIRFLOW_DQ_REPORT was not found in its output."
        )

    context["ti"].xcom_push(
        key="dq_report",
        value=dq_report,
    )

    print(f"Captured DQ report: {dq_report}")

def run_dq_validation(**context):
    """
    Pull the DQ report from XCom and branch based on batch_passed.

    PASS:
        compact_s3_partitions

    FAIL:
        agentic_dq_diagnosis
    """
    ti = context["ti"]

    dq_report = ti.xcom_pull(
        task_ids="start_streaming_consumer",
        key="dq_report",
    )

    if dq_report is None:
        raise RuntimeError(
            "No DQ report found in XCom. "
            "The streaming task must publish AIRFLOW_DQ_REPORT."
        )

    print(f"DQ report received: {dq_report}")

    if dq_report.get("batch_passed", True):
        print("DQ validation PASSED.")
        return "run_glue_curation_job"

    print(
        "DQ validation FAILED. "
        f"Failures: {dq_report.get('failures')}"
    )

    return "agentic_dq_diagnosis"


def run_agentic_diagnosis(**context):
    """
    Run the LLM-powered diagnosis layer for failed DQ checks.

    The actual Claude API call happens here, outside the Spark executor,
    avoiding external API calls from Spark foreachBatch processing.

    Each generated diagnosis is written to XCom using:
        diagnosis_<check_name>
    """
    from dq.agentic_diagnosis import diagnose

    ti = context["ti"]

    dq_report = ti.xcom_pull(
        task_ids="start_streaming_consumer",
        key="dq_report",
    )

    if not dq_report:
        raise RuntimeError(
            "DQ report not available for agentic diagnosis."
        )

    failures = dq_report.get("failures", [])

    if not failures:
        print(
            "DQ gate failed, but no individual failure details "
            "were provided."
        )
        return

    batch_id = dq_report.get("batch_id", "unknown")

    print(
        f"Starting agentic diagnosis for batch_id={batch_id}. "
        f"Failure count={len(failures)}"
    )

    for failure in failures:
        if isinstance(failure, dict):
            check_name = (
                failure.get("check")
                or failure.get("name")
                or failure.get("alert_type")
                or "unknown_check"
            )

            alert_type = failure.get(
                "alert_type",
                check_name,
            )

            details = failure
        else:
            check_name = "dq_failure"
            alert_type = "dq_failure"
            details = {"failure": str(failure)}

        print(
            f"Running agentic diagnosis for check={check_name}"
        )

        diagnosis = diagnose(
            batch_id=batch_id,
            alert_type=alert_type,
            details=details,
        )

        if diagnosis is None:
            print(
                f"No diagnosis generated for {check_name}. "
                "The pipeline will continue to quarantine the failed batch."
            )
            continue

        safe_key = (
            str(check_name)
            .replace(" ", "_")
            .replace("-", "_")
            .replace(".", "_")
        )

        ti.xcom_push(
            key=f"diagnosis_{safe_key}",
            value=diagnosis,
        )

        print(
            f"Agentic diagnosis generated for {check_name}: "
            f"{diagnosis}"
        )


def compact_s3(**context):
    """
    Compact small Parquet files in the curated zone.

    The current Spark/curated pipeline already writes partitioned data.
    This task represents the downstream optimization stage and can later
    be connected to a dedicated Spark/Glue compaction implementation.
    """
    print(
        "Curated S3 optimization stage reached. "
        "Partitioned curated data is available for compaction."
    )


def quarantine_batch(**context):
    """
    Confirm the failed batch has been routed to the quarantine workflow.

    Actual record-level quarantine is performed by the Spark DQ pipeline.
    This Airflow task records the workflow transition and exposes the
    expected quarantine location for operational review.
    """
    import yaml

    ti = context["ti"]

    dq_report = ti.xcom_pull(
        task_ids="start_streaming_consumer",
        key="dq_report",
    )

    if not dq_report:
        raise RuntimeError(
            "DQ report unavailable while processing quarantine workflow."
        )

    with open("/opt/airflow/config/config.yaml", "r") as f:
        cfg = yaml.safe_load(f)

    bucket = cfg["s3"]["bucket"]
    quarantine_prefix = cfg["s3"].get(
        "quarantine_prefix",
        "banking/quarantine",
    )

    batch_id = dq_report.get(
        "batch_id",
        "unknown",
    )

    failures = dq_report.get(
        "failures",
        [],
    )

    quarantine_location = (
        f"s3://{bucket}/"
        f"{quarantine_prefix}/"
        f"batch_id={batch_id}/"
    )

    print(
        "Quarantine workflow completed.\n"
        f"Batch ID: {batch_id}\n"
        f"Failures: {failures}\n"
        f"Quarantine location: {quarantine_location}"
    )


def notify_success(**context):
    """
    Final success notification.
    """
    dag_run = context["dag_run"]

    print(
        f"Pipeline SUCCESS | "
        f"DAG Run: {dag_run.run_id} | "
        f"Execution date: {context['execution_date']}"
    )


def notify_failure(**context):
    """
    Final failure notification.
    """
    dag_run = context["dag_run"]

    print(
        f"Pipeline FAILED | "
        f"DAG Run: {dag_run.run_id} | "
        f"DQ failures detected. "
        f"Review the quarantine workflow and diagnosis output."
    )


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="banking_pipeline_dag",
    description=(
        "Real-Time Banking Transaction Pipeline - Master DAG"
    ),
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=[
        "banking",
        "streaming",
        "kafka",
        "pyspark",
        "iceberg",
        "dq",
        "agentic-ai",
    ],
) as dag:

    # -----------------------------------------------------------------------
    # Start
    # -----------------------------------------------------------------------

    start = DummyOperator(
        task_id="start",
    )

    # -----------------------------------------------------------------------
    # Kafka Health Check
    # -----------------------------------------------------------------------

    health_check_kafka = PythonOperator(
        task_id="health_check_kafka",
        python_callable=check_kafka_health,
    )

    # -----------------------------------------------------------------------
    # Producer
    # -----------------------------------------------------------------------

    start_producer = BashOperator(
        task_id="start_producer",
        bash_command=(
            "cd /opt/airflow && "
            "timeout 180 python producer/transaction_producer.py "
            "> /tmp/banking-producer.log 2>&1; "
            "rc=$?; "
            "if [ $rc -eq 124 ]; then exit 0; else exit $rc; fi"
        ),
        doc_md=(
            "Runs the Kafka transaction producer for a bounded 180-second window "
            "and keeps the producer lifecycle under Airflow."
        ),
    )

    # -----------------------------------------------------------------------
    # Spark Streaming + DQ
    # -----------------------------------------------------------------------

    start_streaming_consumer = PythonOperator(
        task_id="start_streaming_consumer",
        python_callable=run_spark_streaming,
        provide_context=True,
        doc_md=(
            "Runs the bounded Spark Structured Streaming job, "
            "executes DQ validation, and captures the DQ report."
        ),
    )

    # -----------------------------------------------------------------------
    # DQ Branch
    # -----------------------------------------------------------------------

    dq_validation_gate = BranchPythonOperator(
        task_id="dq_validation_gate",
        python_callable=run_dq_validation,
        provide_context=True,
    )

    # -----------------------------------------------------------------------
    # PASS path
    # -----------------------------------------------------------------------

    run_glue_curation_job = GlueJobOperator(
        task_id="run_glue_curation_job",
        job_name="banking-pipeline-curate-transactions",
        aws_conn_id=None,
        region_name="ap-south-1",
        wait_for_completion=True,
        deferrable=False,
        verbose=False,
        retry_limit=1,
        job_poll_interval=15,
        doc_md=(
            "Runs the AWS Glue 4.0 curation job that reads the raw S3 zone, "
            "deduplicates/transforms transactions, and writes the curated "
            "Apache Iceberg table registered in the AWS Glue Data Catalog."
        ),
    )

    # -----------------------------------------------------------------------
    # FAIL path - agentic diagnosis
    # -----------------------------------------------------------------------

    agentic_dq_diagnosis = PythonOperator(
        task_id="agentic_dq_diagnosis",
        python_callable=run_agentic_diagnosis,
        provide_context=True,
        doc_md=(
            "Uses the Claude API to analyze failed DQ checks and "
            "generate root-cause hypotheses and recommended actions."
        ),
    )

    # -----------------------------------------------------------------------
    # FAIL path - quarantine
    # -----------------------------------------------------------------------

    quarantine_batch_task = PythonOperator(
        task_id="quarantine_batch",
        python_callable=quarantine_batch,
        provide_context=True,
        doc_md=(
            "Confirms the failed batch has been routed to the "
            "Spark-managed S3 quarantine workflow."
        ),
    )

    # -----------------------------------------------------------------------
    # Notifications
    # -----------------------------------------------------------------------

    success_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    failure_notify = PythonOperator(
        task_id="notify_failure",
        python_callable=notify_failure,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # -----------------------------------------------------------------------
    # End
    # -----------------------------------------------------------------------

    end = DummyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # -----------------------------------------------------------------------
    # Task Dependencies
    # -----------------------------------------------------------------------
    (
        start
        >> health_check_kafka
    )

    # Producer and streaming consumer run concurrently after Kafka health check.
    (
        health_check_kafka
        >> [start_producer, start_streaming_consumer]
    )

    # DQ gate waits for both producer and consumer to finish.
    (
        [start_producer, start_streaming_consumer]
        >> dq_validation_gate
    )

    # PASS branch
    (
        dq_validation_gate
        >> run_glue_curation_job
        >> success_notify
        >> end
    )

    # FAIL branch
    (
        dq_validation_gate
        >> agentic_dq_diagnosis
        >> quarantine_batch_task
        >> failure_notify
        >> end
    )
