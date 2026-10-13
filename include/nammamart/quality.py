"""Reusable data-quality checks for NammaMart pipelines."""

import pandas as pd


def duplicate_key_rows(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Return rows whose non-null business key occurs more than once."""
    valid = df[key].notna() & df[key].astype(str).str.strip().ne("")
    return df.loc[valid & df.duplicated(subset=[key], keep=False)].copy()


def missing_key_rows(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Return rows where the business key is missing or blank."""
    missing = df[key].isna() | df[key].astype(str).str.strip().eq("")
    return df.loc[missing].copy()


def orphan_rows(
    child: pd.DataFrame,
    child_key: str,
    parent: pd.DataFrame,
    parent_key: str,
) -> pd.DataFrame:
    """Return child rows with nonblank keys absent from the parent."""
    key = child[child_key].astype("string").str.strip()
    parent_keys = (
        parent[parent_key]
        .dropna()
        .astype("string")
        .str.strip()
    )
    valid = key.notna() & key.ne("")
    return child.loc[valid & ~key.isin(parent_keys)].copy()


def exact_duplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Return repeated full rows after their first occurrence."""
    return df.loc[df.duplicated(keep="first")].copy()
