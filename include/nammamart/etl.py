"""Reusable CSV and Parquet output helpers for NammaMart ETL."""

from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = PROJECT_ROOT / "include" / "data" / "nammamart"
OUTPUT_DIR = PROJECT_ROOT / "include" / "output" / "nammamart"


def read_source_csv(filename: str) -> pd.DataFrame:
    """Read an input CSV without modifying the source file."""
    path = SOURCE_DIR / filename

    if not path.is_file():
        raise FileNotFoundError(f"Source CSV not found: {path}")

    return pd.read_csv(path, dtype="string", keep_default_na=True)


def write_dataset(df: pd.DataFrame, dataset_name: str) -> dict[str, str]:
    """Write a DataFrame as CSV and Parquet; return their paths."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_DIR / f"{dataset_name}.csv"
    parquet_path = OUTPUT_DIR / f"{dataset_name}.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False, engine="pyarrow")

    return {
        "csv": str(csv_path),
        "parquet": str(parquet_path),
    }



def _split_conflicting_keys(
    df: pd.DataFrame, key: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Quarantine all rows for business keys with conflicting records."""
    valid = df[key].notna() & df[key].str.strip().ne("")
    keyed = df.loc[valid]

    duplicate_rows = keyed.loc[keyed.duplicated(subset=[key], keep=False)]
    conflicting_keys = []

    for value, group in duplicate_rows.groupby(key, sort=False):
        # Exact repeats are not conflicts; differing records are.
        if len(group.drop_duplicates()) > 1:
            conflicting_keys.append(value)

    conflicts = df.loc[df[key].isin(conflicting_keys)].copy()
    clean = df.loc[~df[key].isin(conflicting_keys)].copy()

    # Missing business keys cannot safely identify a unique entity.
    missing_key = df[key].isna() | df[key].str.strip().eq("")
    conflicts = pd.concat([conflicts, df.loc[missing_key]], ignore_index=True)
    clean = clean.loc[~missing_key].copy()

    return clean, conflicts


def transform_customers() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean customers and quarantine conflicting or missing IDs."""
    df = read_source_csv("customers.csv")

    # Remove repeated identical records first.
    df = df.drop_duplicates().copy()

    clean, quarantine = _split_conflicting_keys(df, "customer_id")

    clean["city"] = clean["city"].fillna("UNKNOWN")
    clean["city"] = clean["city"].str.strip().replace("", "UNKNOWN")

    return clean, quarantine


def transform_products() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean products and quarantine conflicting or missing IDs."""
    df = read_source_csv("products.csv")

    df = df.drop_duplicates().copy()

    clean, quarantine = _split_conflicting_keys(df, "product_id")

    clean["category"] = clean["category"].fillna("UNKNOWN")
    clean["category"] = clean["category"].str.strip().replace("", "UNKNOWN")

    return clean, quarantine


def transform_orders() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Deduplicate base orders and upsert the incremental order file."""
    base = read_source_csv("orders.csv")
    incremental = read_source_csv("orders_2026-10-01_increment.csv")

    base["source_file"] = "orders.csv"
    incremental["source_file"] = "orders_2026-10-01_increment.csv"

    # Remove exact repeated rows from the base data.
    base = base.drop_duplicates().copy()

    # Incremental records are authoritative for the order_status field.
    # Reject missing/blank order IDs into quarantine.
    missing_base = (
        base["order_id"].isna() | base["order_id"].str.strip().eq("")
    )
    missing_incremental = (
        incremental["order_id"].isna()
        | incremental["order_id"].str.strip().eq("")
    )

    quarantine = pd.concat(
        [base.loc[missing_base], incremental.loc[missing_incremental]],
        ignore_index=True,
    )

    base = base.loc[~missing_base].copy()
    incremental = incremental.loc[~missing_incremental].copy()

    # A repeated order ID with different records is ambiguous.
    base, base_conflicts = _split_conflicting_keys(base, "order_id")
    incremental, incremental_conflicts = _split_conflicting_keys(
        incremental, "order_id"
    )
    quarantine = pd.concat(
        [quarantine, base_conflicts, incremental_conflicts],
        ignore_index=True,
    )

    # Incremental order IDs update order_status only.
    # New incremental IDs are inserted as new orders.
    base_by_id = base.set_index("order_id", drop=False)
    incremental_by_id = incremental.set_index("order_id", drop=False)

    overlap = base_by_id.index.intersection(incremental_by_id.index)
    new_ids = incremental_by_id.index.difference(base_by_id.index)

    if len(overlap):
        base_by_id.loc[overlap, "order_status"] = (
            incremental_by_id.loc[overlap, "order_status"]
        )

    combined = pd.concat(
        [base_by_id, incremental_by_id.loc[new_ids]],
        ignore_index=True,
    )

    # Re-check the key after merging.
    duplicate_ids = combined["order_id"].duplicated(keep=False)
    if duplicate_ids.any():
        raise ValueError(
            "Order upsert produced duplicate order IDs: "
            f"{combined.loc[duplicate_ids, 'order_id'].tolist()}"
        )

    return combined, quarantine


#def transform_payments(
#    valid_order_ids: set[str],
#) -> tuple[pd.DataFrame, pd.DataFrame]:
#    """Remove exact payment replays and quarantine invalid order references."""
#    df = read_source_csv("payments.csv")
#
#    # Remove exact repeated payment rows.
#    df = df.drop_duplicates().copy()
#
#    missing_id = df["payment_id"].isna() | df["payment_id"].str.strip().eq("")
#    duplicate_id = df["payment_id"].duplicated(keep=False) & ~missing_id
#
#    # Conflicting records for one payment ID must not be selected arbitrarily.
#    conflicting_ids = []
#    for payment_id, group in df.loc[duplicate_id].groupby("payment_id"):
#        if len(group.drop_duplicates()) > 1:
#            conflicting_ids.append(payment_id)
#
#    conflicts = df.loc[df["payment_id"].isin(conflicting_ids)].copy()
#    missing_keys = df.loc[missing_id].copy()
#
#    df = df.loc[~df["payment_id"].isin(conflicting_ids) & ~missing_id].copy()
#
#    # Payment records without a matching curated order are quarantined.
#    order_ref = df["order_id"].astype("string").str.strip()
#    orphan_mask = order_ref.isna() | order_ref.eq("") | ~order_ref.isin(valid_order_ids)
#    orphans = df.loc[orphan_mask].copy()
#    clean = df.loc[~orphan_mask].copy()

#   quarantine = pd.concat(
#        [conflicts, missing_keys, orphans], ignore_index=True
#    )

#    return clean, quarantine

def transform_payments(
    valid_order_ids: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Deduplicate payments and quarantine invalid references with reasons."""
    df = read_source_csv("payments.csv").drop_duplicates().copy()

    quarantine_parts = []

    # Identify missing payment IDs.
    payment_id = df["payment_id"].astype("string").str.strip()
    missing_id = payment_id.isna() | payment_id.eq("")

    missing_payments = df.loc[missing_id].copy()
    if not missing_payments.empty:
        missing_payments["quarantine_reason"] = "missing_payment_id"
        quarantine_parts.append(missing_payments)

    # Identify conflicting records sharing a payment ID.
    keyed = df.loc[~missing_id]
    duplicate_mask = keyed.duplicated("payment_id", keep=False)
    duplicate_rows = keyed.loc[duplicate_mask]

    conflicting_ids = []
    for value, group in duplicate_rows.groupby("payment_id", sort=False):
        if len(group.drop_duplicates()) > 1:
            conflicting_ids.append(value)

    conflicts = df.loc[payment_id.isin(conflicting_ids)].copy()
    if not conflicts.empty:
        conflicts["quarantine_reason"] = "conflicting_payment_id"
        quarantine_parts.append(conflicts)

    # Continue with records having usable, non-conflicting payment IDs.
    df = df.loc[
        ~missing_id & ~payment_id.isin(conflicting_ids)
    ].copy()

    # Validate order references separately from payment IDs.
    order_ref = df["order_id"].astype("string").str.strip()
    missing_order = order_ref.isna() | order_ref.eq("")

    valid_ids = {
        str(value).strip()
        for value in valid_order_ids
        if pd.notna(value) and str(value).strip()
    }

    orphan_order = ~missing_order & ~order_ref.isin(valid_ids)

    missing_order_rows = df.loc[missing_order].copy()
    if not missing_order_rows.empty:
        missing_order_rows["quarantine_reason"] = "missing_order_id"
        quarantine_parts.append(missing_order_rows)

    orphan_rows = df.loc[orphan_order].copy()
    if not orphan_rows.empty:
        orphan_rows["quarantine_reason"] = "orphan_order_id"
        quarantine_parts.append(orphan_rows)

    # Keep only payments with valid references to curated orders.
    clean = df.loc[~missing_order & ~orphan_order].copy()

    quarantine = (
        pd.concat(quarantine_parts, ignore_index=True)
        if quarantine_parts
        else pd.DataFrame(
            columns=[*df.columns, "quarantine_reason"]
        )
    )

    return clean, quarantine

def transform_stores() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean stores and normalize missing zones."""
    df = read_source_csv("stores.csv").drop_duplicates().copy()

    clean, quarantine = _split_conflicting_keys(df, "store_id")
    clean["zone"] = clean["zone"].fillna("UNKNOWN")
    clean["zone"] = clean["zone"].str.strip().replace("", "UNKNOWN")

    return clean, quarantine




def validate_orders(
    orders: pd.DataFrame,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    stores: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Quarantine orders with missing fields, invalid timestamps, or orphan references."""
    df = orders.copy()
    reasons = pd.Series("", index=df.index, dtype="string")

    required_fields = [
        "order_id", "order_ts", "customer_id", "product_id", "store_id"
    ]

    # Validate required fields.
    for column in required_fields:
        values = df[column].astype("string").str.strip()
        missing = df[column].isna() | values.eq("")

        if column == "order_ts":
            missing = missing | values.str.upper().eq("N/A")

        reasons.loc[missing] = reasons.loc[missing].apply(
            lambda current: f"{current}; missing_{column}".strip("; ")
        )

    # Reject timestamps that are present but cannot be parsed.
    timestamp_text = df["order_ts"].astype("string").str.strip()
    timestamp_values = pd.to_datetime(
        timestamp_text,
        errors="coerce",
        format="mixed",
    )
    invalid_timestamp = (
        df["order_ts"].notna()
        & timestamp_text.ne("")
        & timestamp_text.str.upper().ne("N/A")
        & timestamp_values.isna()
    )
    reasons.loc[invalid_timestamp] = reasons.loc[invalid_timestamp].apply(
        lambda current: f"{current}; invalid_order_ts".strip("; ")
    )

    # Validate foreign-key references.
    references = [
        ("customer_id", customers, "customer_id"),
        ("product_id", products, "product_id"),
        ("store_id", stores, "store_id"),
    ]

    for child_key, parent, parent_key in references:
        key = df[child_key].astype("string").str.strip()
        valid_parent_keys = (
            parent[parent_key].dropna().astype("string").str.strip()
        )
        orphan = key.notna() & key.ne("") & ~key.isin(valid_parent_keys)
        reasons.loc[orphan] = reasons.loc[orphan].apply(
            lambda current: f"{current}; orphan_{child_key}".strip("; ")
        )

    quarantine_mask = reasons.ne("")
    quarantined = df.loc[quarantine_mask].copy()
    quarantined["quarantine_reason"] = reasons.loc[quarantine_mask]

    curated = df.loc[~quarantine_mask].copy()
    return curated, quarantined

def run_etl() -> dict[str, dict[str, str]]:
    """Run all transformations and persist curated and quarantine outputs."""
    customers, customers_q = transform_customers()
    products, products_q = transform_products()
    stores, stores_q = transform_stores()

    orders, orders_initial_q = transform_orders()
    orders, orders_validation_q = validate_orders(
        orders, customers, products, stores
    )

    # Payments must reference orders that survive validation.
    payments, payments_q = transform_payments(
        set(orders["order_id"].astype(str))
    )

    outputs = {
        "customers": write_dataset(customers, "customers"),
        "customers_quarantine": write_dataset(customers_q, "customers_quarantine"),
        "products": write_dataset(products, "products"),
        "products_quarantine": write_dataset(products_q, "products_quarantine"),
        "stores": write_dataset(stores, "stores"),
        "stores_quarantine": write_dataset(stores_q, "stores_quarantine"),
        "orders": write_dataset(orders, "orders"),
        "orders_quarantine": write_dataset(
            pd.concat([orders_initial_q, orders_validation_q], ignore_index=True),
            "orders_quarantine",
        ),
        "payments": write_dataset(payments, "payments"),
        "payments_quarantine": write_dataset(payments_q, "payments_quarantine"),
    }

    return outputs


def build_quality_report() -> pd.DataFrame:
    """Build a summary of curated outputs and their quarantine counts."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    datasets = [
        "customers",
        "products",
        "stores",
        "orders",
        "payments",
    ]

    report_rows = []

    for name in datasets:
        curated_path = OUTPUT_DIR / f"{name}.parquet"
        quarantine_path = OUTPUT_DIR / f"{name}_quarantine.parquet"

        curated = pd.read_parquet(curated_path)
        quarantine = pd.read_parquet(quarantine_path)

        key = {
            "customers": "customer_id",
            "products": "product_id",
            "stores": "store_id",
            "orders": "order_id",
            "payments": "payment_id",
        }[name]

        report_rows.append(
            {
                "dataset": name,
                "curated_rows": len(curated),
                "quarantined_rows": len(quarantine),
                "total_processed_rows": len(curated) + len(quarantine),
                "business_key": key,
                "business_key_unique": curated[key].is_unique,
                "missing_business_keys": int(curated[key].isna().sum()),
                "curated_parquet": str(curated_path),
                "quarantine_parquet": str(quarantine_path),
            }
        )

    report = pd.DataFrame(report_rows)
    report.to_csv(OUTPUT_DIR / "quality_report.csv", index=False)
    report.to_parquet(
        OUTPUT_DIR / "quality_report.parquet",
        index=False,
        engine="pyarrow",
    )

    return report
