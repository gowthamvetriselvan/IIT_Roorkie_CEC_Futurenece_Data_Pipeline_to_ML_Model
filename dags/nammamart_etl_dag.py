
"""Airflow DAG for the NammaMart local-file ETL pipeline."""

import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


from include.nammamart.reconciliation import (
    get_source_reconciliation_metrics,
)

from airflow.sdk import dag, task, get_current_context
from airflow.utils.trigger_rule import TriggerRule
from include.nammamart.audit import record_pipeline_run
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
        context = get_current_context()
        run_id = context["dag_run"].run_id
        outputs = run_etl(run_id=run_id)
        report = build_quality_report()

        artifact_count = sum(len(paths) for paths in outputs.values())
        rows_loaded = int(report["curated_rows"].sum())
        rows_quarantined = int(report["quarantined_rows"].sum())
        source_metrics = get_source_reconciliation_metrics()

        logging.info("ETL completed; generated %d artifacts", artifact_count)
        logging.info("Output directory: %s", OUTPUT_DIR)
        logging.info("Source reconciliation metrics: %s", source_metrics)

        return {
            "run_id": run_id,
            "dataset_count": len(report),
            "artifact_count": artifact_count,
            "rows_extracted": source_metrics["rows_extracted"],
            "rows_deduplicated": source_metrics["rows_deduplicated"],
            "rows_superseded": source_metrics["rows_superseded"],
            "rows_loaded": rows_loaded,
            "rows_quarantined": rows_quarantined,
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
    def publish_summary(summary: dict) -> dict:
        """Publish the final summary to the Airflow task logs."""
        logging.info("NammaMart ETL run summary: %s", summary)
        return summary

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def record_run() -> None:
        """Record this DAG run even when an upstream task fails."""
        context = get_current_context()
        dag_run = context["dag_run"]
        ti = context["ti"]

        run_info = ti.xcom_pull(task_ids="run_transformations")
        quality_summary = ti.xcom_pull(task_ids="validate_and_report")
        published_summary = ti.xcom_pull(task_ids="publish_summary")

        # Use XCom results to determine whether the pipeline completed.
        failed = not all(
            isinstance(result, dict)
            for result in (run_info, quality_summary, published_summary)
        )

        run_info = run_info if isinstance(run_info, dict) else {}

        now = datetime.now(timezone.utc)
        record_pipeline_run(
            run_id=dag_run.run_id,
            dag_id=dag_run.dag_id,
            start_time=dag_run.start_date or now,
            end_time=now,
            rows_deduplicated=run_info.get("rows_deduplicated", 0),
            rows_superseded=run_info.get("rows_superseded", 0),
            rows_extracted=run_info.get("rows_extracted", 0),
            rows_loaded=run_info.get("rows_loaded", 0),
            rows_quarantined=run_info.get("rows_quarantined", 0),
            status="FAILED" if failed else "SUCCESS",
    )

    run_info = run_transformations()
    quality_summary = validate_and_report(run_info)
    published = publish_summary(quality_summary)
    published >> record_run()


#nammamart_etl_pipeline()
nammamart_etl_dag = nammamart_etl_pipeline()
