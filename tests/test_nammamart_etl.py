import pandas as pd

from include.nammamart.etl import (
    OUTPUT_DIR,
    build_quality_report,
    read_source_csv,
    run_etl,
    transform_customers,
    transform_orders,
    transform_payments,
    transform_products,
    transform_stores,
)


def test_customer_and_product_cleaning():
    customers, customers_q = transform_customers()
    products, products_q = transform_products()

    assert len(customers) == 395
    assert len(customers_q) == 10
    assert customers["customer_id"].is_unique

    assert len(products) == 148
    assert len(products_q) == 4
    assert products["product_id"].is_unique


def test_order_incremental_upsert():
    orders, quarantine = transform_orders()
    incremental = read_source_csv("orders_2026-10-01_increment.csv")

    assert len(orders) == 2550
    assert quarantine.empty
    assert orders["order_id"].is_unique

    # Verify the expected split between base and newly inserted orders.
    assert (orders["source_file"] == "orders.csv").sum() == 2400
    assert (
        orders["source_file"] == "orders_2026-10-01_increment.csv"
    ).sum() == 150

    # The incremental file must win for overlapping order statuses.
    #overlap = set(orders["order_id"]) & set(incremental["order_id"])
    #assert len(overlap) == 10

    base = read_source_csv("orders.csv")
    overlap = set(base["order_id"]) & set(incremental["order_id"])
    assert len(overlap) == 10

   #Verify that incremental status wins for all overlapping IDs.
    final_status = orders.set_index("order_id")["order_status"]
    incremental_status = incremental.set_index("order_id")["order_status"]

    for order_id in overlap:
       assert final_status.loc[order_id] == incremental_status.loc[order_id]

    final_status = orders.set_index("order_id")["order_status"]
    incremental_status = incremental.set_index("order_id")["order_status"]

    for order_id in overlap:
        assert final_status.loc[order_id] == incremental_status.loc[order_id]


def test_payment_and_store_transformations():
    orders, _ = transform_orders()
    payments, payments_q = transform_payments(
        set(orders["order_id"].astype(str))
    )
    stores, stores_q = transform_stores()

    assert len(payments) == 2216
    assert len(payments_q) == 8
    assert payments["payment_id"].is_unique

    assert stores["store_id"].is_unique
    assert len(stores) == 15
    assert stores_q.empty


def test_complete_etl_outputs_and_quality_report():
    run_etl()

    expected_counts = {
        "customers": (395, 10),
        "products": (148, 4),
        "stores": (15, 0),
        "orders": (2446, 104),
        "payments": (2122, 102),
    }

    for name, (expected_curated, expected_quarantine) in expected_counts.items():
        curated = pd.read_parquet(OUTPUT_DIR / f"{name}.parquet")
        quarantine = pd.read_parquet(
            OUTPUT_DIR / f"{name}_quarantine.parquet"
        )

        assert len(curated) == expected_curated, name
        assert len(quarantine) == expected_quarantine, name

        key = {
            "customers": "customer_id",
            "products": "product_id",
            "stores": "store_id",
            "orders": "order_id",
            "payments": "payment_id",
        }[name]

        assert curated[key].notna().all(), name
        assert curated[key].is_unique, name

    payment_q = pd.read_parquet(
        OUTPUT_DIR / "payments_quarantine.parquet"
    )
    assert payment_q["quarantine_reason"].notna().all()
    assert set(payment_q["quarantine_reason"]) == {"orphan_order_id"}

    report = build_quality_report()
    assert len(report) == 5
    assert report["business_key_unique"].all()

    for name in [
        "customers",
        "products",
        "stores",
        "orders",
        "payments",
    ]:
        csv_df = pd.read_csv(OUTPUT_DIR / f"{name}.csv", dtype="string")
        parquet_df = pd.read_parquet(OUTPUT_DIR / f"{name}.parquet")
        assert len(csv_df) == len(parquet_df), name


def test_repeated_etl_run_is_idempotent():
    run_etl()
    first_orders = pd.read_parquet(OUTPUT_DIR / "orders.parquet")
    first_payments = pd.read_parquet(OUTPUT_DIR / "payments.parquet")

    run_etl()
    second_orders = pd.read_parquet(OUTPUT_DIR / "orders.parquet")
    second_payments = pd.read_parquet(OUTPUT_DIR / "payments.parquet")

    pd.testing.assert_frame_equal(first_orders, second_orders)
    pd.testing.assert_frame_equal(first_payments, second_payments)
