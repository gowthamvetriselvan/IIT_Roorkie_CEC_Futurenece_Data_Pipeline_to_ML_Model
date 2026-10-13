
"""Airflow DAG for the NammaMart local-file ETL pipeline."""

import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow.sdk import dag, task
from include.nammamart.etl import (
    OUTPUT_DIR,
    build_quality_report,
    run_etl,
)


@dag(
    dag_id="nammamart_etl_pipeline",
    description="Clean NammaMart CSVs and publish curated CSV/Parquet datasets",
    schedule="0 9 * * 1-6",

    start_date=datetime(2026, 10, 13, tzinfo=ZoneInfo("Asia/Kolkata")),
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["nammamart", "etl", "data-quality"],
)
def nammamart_etl_pipeline():

    @task
    def run_transformations() -> dict:
        """Run all transformations and write curated/quarantine outputs."""
        outputs = run_etl()

        artifact_count = sum(len(paths) for paths in outputs.values())
        logging.info("ETL completed; generated %d artifacts", artifact_count)
        logging.info("Output directory: %s", OUTPUT_DIR)

        return {
            "dataset_count": len(outputs),
            "artifact_count": artifact_count,
            "output_dir": str(OUTPUT_DIR),
        }

    @task
    def validate_and_report(run_info: dict) -> dict:
        """Generate the quality report and enforce key integrity."""
        report = build_quality_report()

        invalid_keys = report.loc[~report["business_key_unique"]]
        missing_keys = report["missing_business_keys"].sum()

        if not invalid_keys.empty:
            raise ValueError(
                "Duplicate business keys found in: "
                f"{invalid_keys['dataset'].tolist()}"
            )

        if missing_keys:
            raise ValueError(
                f"Curated datasets contain {missing_keys} missing business keys"
            )

        summary = report[
            ["dataset", "curated_rows", "quarantined_rows"]
        ].to_dict(orient="records")

        logging.info("Quality report generated successfully")
        logging.info("Dataset quality summary: %s", summary)

        return {
            "run_info": run_info,
            "datasets": summary,
            "quality_report": str(OUTPUT_DIR / "quality_report.csv"),
        }

    @task
    def publish_summary(summary: dict) -> None:
        """Publish the final summary to the Airflow task logs."""
        logging.info("NammaMart ETL run summary: %s", summary)

    run_info = run_transformations()
    quality_summary = validate_and_report(run_info)
    publish_summary(quality_summary)


#nammamart_etl_pipeline()
nammamart_etl_dag = nammamart_etl_pipeline()
