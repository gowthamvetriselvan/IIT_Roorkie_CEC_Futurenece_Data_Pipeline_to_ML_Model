
"""DuckDB SQL-based ELT pipeline for NammaMart."""

from pathlib import Path

import duckdb
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = PROJECT_ROOT / "include" / "data" / "nammamart"
OUTPUT_DIR = PROJECT_ROOT / "include" / "output" / "nammamart_elt"
DB_PATH = OUTPUT_DIR / "nammamart_elt.duckdb"


SOURCE_FILES = {
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
    "orders": "orders.csv",
    "orders_increment": "orders_2026-10-01_increment.csv",
    "payments": "payments.csv",
}


def run_elt() -> dict:
    """Load raw CSVs into DuckDB, transform using SQL, and export results."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH))
    results = {}

    try:
        # EXTRACT + LOAD: retain raw source data in DuckDB.
        for table, filename in SOURCE_FILES.items():
            source_path = SOURCE_DIR / filename
            if not source_path.exists():
                raise FileNotFoundError(source_path)

            con.execute(
                f"""
                CREATE OR REPLACE TABLE raw_{table} AS
                SELECT *
                FROM read_csv_auto(?, all_varchar = true)
                """,
                [str(source_path)],
            )

        # TRANSFORM: dimensions with conflicting business keys are quarantined.
        for table, key in [
            ("customers", "customer_id"),
            ("products", "product_id"),
            ("stores", "store_id"),
        ]:
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {table}_conflict_keys AS
                SELECT {key}
                FROM (
                    SELECT DISTINCT *
                    FROM raw_{table}
                ) d
                GROUP BY {key}
                HAVING COUNT(*) > 1
                """
            )

            # Identical duplicate rows are removed before conflict handling.
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {table}_distinct AS
                SELECT DISTINCT *
                FROM raw_{table}
                """
            )

            con.execute(
                f"""
                CREATE OR REPLACE TABLE {table}_quarantine AS
                SELECT d.*,
                       'conflicting_{key}' AS quarantine_reason
                FROM {table}_distinct d
                JOIN {table}_conflict_keys k USING ({key})
                UNION ALL
                SELECT d.*,
                       'missing_{key}' AS quarantine_reason
                FROM {table}_distinct d
                WHERE {key} IS NULL OR TRIM({key}) = ''
                """
            )

            #con.execute(
            #    f"""
            #    CREATE OR REPLACE TABLE {table}_curated AS
            #    SELECT * EXCLUDE ({key})
            #    FROM {table}_distinct
            #    WHERE FALSE
            #    """
            #) if False else None

            con.execute(
                f"""
                CREATE OR REPLACE TABLE {table}_curated AS
                SELECT * EXCLUDE (_rn, _conflict)
                FROM (
                    SELECT d.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY {key}
                               ORDER BY {key}
                           ) AS _rn,
                           CASE
                               WHEN k.{key} IS NOT NULL THEN 1
                               ELSE 0
                           END AS _conflict
                    FROM {table}_distinct d
                    LEFT JOIN {table}_conflict_keys k USING ({key})
                    WHERE {key} IS NOT NULL AND TRIM({key}) <> ''
                ) q
                WHERE _rn = 1 AND _conflict = 0
                """
            )

        # Fill known optional dimension fields.
        con.execute(
            "UPDATE customers_curated SET city = 'UNKNOWN' "
            "WHERE city IS NULL OR TRIM(city) = ''"
        )
        con.execute("""
                UPDATE customers_curated
                SET email = NULL
                WHERE email IS NOT NULL
                AND UPPER(TRIM(email)) = 'N/A'
        """)
        con.execute(
            "UPDATE products_curated SET category = 'UNKNOWN' "
            "WHERE category IS NULL OR TRIM(category) = ''"
        )
        con.execute(
            "UPDATE stores_curated SET zone = 'UNKNOWN' "
            "WHERE zone IS NULL OR TRIM(zone) = ''"
        )

        # Merge base and incremental orders: the increment updates status
        # for existing order IDs and inserts new order IDs.
        con.execute("""
            CREATE OR REPLACE TABLE orders_base_distinct AS
            SELECT DISTINCT * FROM raw_orders
        """)
        con.execute("""
            CREATE OR REPLACE TABLE orders_increment_distinct AS
            SELECT DISTINCT * FROM raw_orders_increment
        """)
        con.execute("""
            CREATE OR REPLACE TABLE orders_base_conflict_keys AS
            SELECT order_id
            FROM orders_base_distinct
            WHERE order_id IS NOT NULL AND TRIM(order_id) <> ''
            GROUP BY order_id
            HAVING COUNT(*) > 1
        """)
        con.execute("""
            CREATE OR REPLACE TABLE orders_increment_conflict_keys AS
            SELECT order_id
            FROM orders_increment_distinct
            WHERE order_id IS NOT NULL AND TRIM(order_id) <> ''
            GROUP BY order_id
            HAVING COUNT(*) > 1
        """)
        con.execute("""
            CREATE OR REPLACE TABLE orders_quarantine AS
            SELECT b.*, 'conflicting_order_id' AS quarantine_reason
            FROM orders_base_distinct b
            JOIN orders_base_conflict_keys k USING (order_id)
            UNION ALL
            SELECT i.*, 'conflicting_increment_order_id' AS quarantine_reason
            FROM orders_increment_distinct i
            JOIN orders_increment_conflict_keys k USING (order_id)
        """)
        con.execute("""
            CREATE OR REPLACE TABLE orders_merged AS
            SELECT b.*, 'orders.csv' AS source_file
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY order_id ORDER BY order_id
                ) AS _rn
                FROM orders_base_distinct
                WHERE order_id IS NOT NULL AND TRIM(order_id) <> ''
                  AND order_id NOT IN (SELECT order_id FROM orders_base_conflict_keys)
            ) b
            WHERE b._rn = 1
            UNION ALL
            SELECT i.*, 'orders_2026-10-01_increment.csv' AS source_file
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY order_id ORDER BY order_id
                ) AS _rn
                FROM orders_increment_distinct
                WHERE order_id IS NOT NULL AND TRIM(order_id) <> ''
                  AND order_id NOT IN (SELECT order_id FROM orders_increment_conflict_keys)
                  AND order_id NOT IN (
                      SELECT order_id FROM orders_base_distinct
                  )
            ) i
            WHERE i._rn = 1
        """)
        con.execute("""

            CREATE OR REPLACE TABLE orders_upserted AS
            SELECT
                b.order_id,
                b.order_ts,
                b.customer_id,
                b.store_id,
                b.product_id,
                b.quantity,
                b.unit_price,
                b.discount_pct,
                b.order_amount,
                b.payment_mode,
                COALESCE(i.order_status, b.order_status) AS order_status,
                b.channel,
                b.delivery_minutes,
                'orders.csv' AS source_file
            FROM orders_base_distinct b
            LEFT JOIN orders_increment_distinct i
                ON b.order_id = i.order_id
            WHERE b.order_id IS NOT NULL
            AND TRIM(b.order_id) <> ''
            AND b.order_id NOT IN (
                SELECT order_id FROM orders_base_conflict_keys
            )
            AND b.order_id NOT IN (
                SELECT order_id FROM orders_increment_conflict_keys
            )

            UNION ALL

            SELECT
                i.order_id,
                i.order_ts,
                i.customer_id,
                i.store_id,
                i.product_id,
                i.quantity,
                i.unit_price,
                i.discount_pct,
                i.order_amount,
                i.payment_mode,
                i.order_status,
                i.channel,
                i.delivery_minutes,
                'orders_2026-10-01_increment.csv' AS source_file
            FROM orders_increment_distinct i
            WHERE i.order_id IS NOT NULL
            AND TRIM(i.order_id) <> ''
            AND i.order_id NOT IN (
                SELECT order_id FROM orders_increment_conflict_keys
            )
            AND i.order_id NOT IN (
                SELECT order_id FROM orders_base_distinct
            );



         """)
        con.execute("""
            CREATE OR REPLACE TABLE orders_validated AS
            SELECT o.*,
                CONCAT_WS(',',
                    CASE
                        WHEN o.order_ts IS NULL
                            OR TRIM(o.order_ts) = ''
                            OR UPPER(TRIM(o.order_ts)) = 'N/A'
                            THEN 'missing_order_ts'
                        WHEN TRY_CAST(o.order_ts AS TIMESTAMP) IS NULL
                            THEN 'invalid_order_ts'
                        END,
                    CASE WHEN o.customer_id IS NULL OR TRIM(o.customer_id) = ''
                         THEN 'missing_customer_id'
                         WHEN c.customer_id IS NULL THEN 'orphan_customer_id' END,
                    CASE WHEN o.product_id IS NULL OR TRIM(o.product_id) = ''
                         THEN 'missing_product_id'
                         WHEN p.product_id IS NULL THEN 'orphan_product_id' END,
                    CASE WHEN o.store_id IS NULL OR TRIM(o.store_id) = ''
                         THEN 'missing_store_id'
                         WHEN s.store_id IS NULL THEN 'orphan_store_id' END
                ) AS quarantine_reason
            FROM orders_upserted o
            LEFT JOIN customers_curated c USING (customer_id)
            LEFT JOIN products_curated p USING (product_id)
            LEFT JOIN stores_curated s USING (store_id)
        """)
        con.execute("""
            CREATE OR REPLACE TABLE orders_quarantine_final AS
            SELECT * FROM orders_validated
            WHERE quarantine_reason <> ''
        """)
        con.execute("""
            CREATE OR REPLACE TABLE orders_curated AS
            SELECT * EXCLUDE (quarantine_reason)
            FROM orders_validated
            WHERE quarantine_reason = ''
        """)
        con.execute("""
            CREATE OR REPLACE TABLE orders_quarantine AS
            SELECT *
            FROM orders_quarantine_final

            UNION ALL

            SELECT
                b.order_id,
                b.order_ts,
                b.customer_id,
                b.store_id,
                b.product_id,
                b.quantity,
                b.unit_price,
                b.discount_pct,
                b.order_amount,
                b.payment_mode,
                b.order_status,
                b.channel,
                b.delivery_minutes,
                'orders.csv' AS source_file,
                'missing_order_id' AS quarantine_reason
            FROM orders_base_distinct b
            WHERE b.order_id IS NULL OR TRIM(b.order_id) = ''

            UNION ALL

            SELECT
                i.order_id,
                i.order_ts,
                i.customer_id,
                i.store_id,
                i.product_id,
                i.quantity,
                i.unit_price,
                i.discount_pct,
                i.order_amount,
                i.payment_mode,
                i.order_status,
                i.channel,
                i.delivery_minutes,
                'orders_2026-10-01_increment.csv' AS source_file,
                'missing_order_id' AS quarantine_reason
            FROM orders_increment_distinct i
            WHERE i.order_id IS NULL OR TRIM(i.order_id) = ''
        """)

        # Payments referencing orders excluded from curation are quarantined.
        con.execute("""
            CREATE OR REPLACE TABLE payments_distinct AS
            SELECT DISTINCT * FROM raw_payments
        """)
        con.execute("""
            CREATE OR REPLACE TABLE payments_quarantine AS
            SELECT p.*, 'orphan_order_id' AS quarantine_reason
            FROM payments_distinct p
            LEFT JOIN orders_curated o USING (order_id)
            WHERE o.order_id IS NULL
        """)
        con.execute("""
            CREATE OR REPLACE TABLE payments_curated AS
            SELECT p.*
            FROM payments_distinct p
            JOIN orders_curated o USING (order_id)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY p.payment_id ORDER BY p.payment_id
            ) = 1
        """)

        # Export curated and quarantine tables to CSV and Parquet.
        datasets = [
            "customers", "products", "stores", "orders", "payments"
        ]
        for dataset in datasets:
            for suffix in ["curated", "quarantine"]:
                table = f"{dataset}_{suffix}"
                df = con.execute(f"SELECT * FROM {table}").df()
                csv_path = OUTPUT_DIR / (
                    f"{dataset}.csv" if suffix == "curated"
                    else f"{dataset}_quarantine.csv"
                )
                parquet_path = OUTPUT_DIR / (
                    f"{dataset}.parquet" if suffix == "curated"
                    else f"{dataset}_quarantine.parquet"
                )
                df.to_csv(csv_path, index=False)
                df.to_parquet(parquet_path, index=False)
                results[table] = {
                    "rows": len(df),
                    "csv": str(csv_path),
                    "parquet": str(parquet_path),
                }

        # Quality summary from final table contents.
        report_rows = []
        for dataset, key in [
            ("customers", "customer_id"),
            ("products", "product_id"),
            ("stores", "store_id"),
            ("orders", "order_id"),
            ("payments", "payment_id"),
        ]:
            curated_count = con.execute(
                f"SELECT COUNT(*) FROM {dataset}_curated"
            ).fetchone()[0]
            quarantine_count = con.execute(
                f"SELECT COUNT(*) FROM {dataset}_quarantine"
            ).fetchone()[0]
            key_stats = con.execute(
                f"""
                SELECT
                    COUNT(*) - COUNT(DISTINCT {key}),
                    COUNT(*) FILTER (
                        WHERE {key} IS NULL OR TRIM({key}) = ''
                    )
                FROM {dataset}_curated
                """
            ).fetchone()
            report_rows.append({
                "dataset": dataset,
                "curated_rows": curated_count,
                "quarantined_rows": quarantine_count,
                "total_processed_rows": curated_count + quarantine_count,
                "business_key": key,
                "business_key_unique": key_stats[0] == 0,
                "missing_business_keys": key_stats[1],
            })

        report = pd.DataFrame(report_rows)
        report.to_csv(OUTPUT_DIR / "elt_quality_report.csv", index=False)
        report.to_parquet(OUTPUT_DIR / "elt_quality_report.parquet", index=False)
        results["quality_report"] = report.to_dict(orient="records")
        return results
    finally:
        con.close()
