"""Airflow DAG for the NammaMart DuckDB ELT pipeline."""

from pathlib import Path

import pandas as pd
import pendulum
from airflow.sdk import dag, task
from datetime import timedelta

from include.nammamart.elt import OUTPUT_DIR, run_elt


@dag(
    dag_id="nammamart_elt_pipeline",
    description="NammaMart SQL ELT pipeline using DuckDB",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(
        2026, 10, 13, tz="Asia/Kolkata"
    ),
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["nammamart", "elt", "duckdb"],
)
def nammamart_elt_pipeline():

    @task
    def run_transformations() -> dict:
        """Load raw data, transform it in DuckDB, and export datasets."""
        results = run_elt()

        counts = {}

        for dataset in [
            "customers",
            "products",
            "stores",
            "orders",
            "payments",
        ]:
            counts[dataset] = {
                "curated": results[f"{dataset}_curated"]["rows"],
                "quarantine": results[f"{dataset}_quarantine"]["rows"],
            }

        print("ELT transformations completed.")
        print(counts)

        return counts

    @task
    def validate_and_report(expected_counts: dict) -> dict:
        """Validate business keys and reconcile output row counts."""
        report_path = OUTPUT_DIR / "elt_quality_report.csv"

        if not Path(report_path).is_file():
            raise FileNotFoundError(
                f"ELT quality report not found: {report_path}"
            )

        report = pd.read_csv(report_path)

        for _, row in report.iterrows():
            dataset = row["dataset"]

            if dataset not in expected_counts:
                raise ValueError(
                    f"Unexpected dataset in quality report: {dataset}"
                )

            expected = expected_counts[dataset]

            if int(row["curated_rows"]) != expected["curated"]:
                raise ValueError(
                    f"{dataset}: curated row count mismatch"
                )

            if int(row["quarantined_rows"]) != expected["quarantine"]:
                raise ValueError(
                    f"{dataset}: quarantine row count mismatch"
                )

            if not bool(row["business_key_unique"]):
                raise ValueError(
                    f"{dataset}: duplicate business keys detected"
                )

            if int(row["missing_business_keys"]) != 0:
                raise ValueError(
                    f"{dataset}: missing business keys detected"
                )

        if len(report) != len(expected_counts):
            raise ValueError("Quality report is missing datasets")

        print("ELT quality validation passed.")
        print(report.to_string(index=False))

        return {
            "status": "PASS",
            "datasets_validated": len(report),
            "quality_report": str(report_path),
        }

    @task
    def publish_summary(validation: dict) -> None:
        """Publish the final pipeline summary to the task logs."""
        print("NammaMart ELT pipeline completed successfully.")
        print(validation)

    counts = run_transformations()
    validation = validate_and_report(counts)
    publish_summary(validation)


nammamart_elt_dag = nammamart_elt_pipeline()
