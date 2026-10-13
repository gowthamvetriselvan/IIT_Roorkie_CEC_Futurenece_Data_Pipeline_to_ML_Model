import pandas as pd

from include.nammamart.quality import (
    duplicate_key_rows,
    missing_key_rows,
    orphan_rows,
    exact_duplicate_rows,
)


def test_duplicate_key_rows():
    df = pd.DataFrame({
        "id": ["A", "A", "B", None, ""],
        "value": [1, 2, 3, 4, 5],
    })

    result = duplicate_key_rows(df, "id")

    assert len(result) == 2
    assert set(result["value"]) == {1, 2}


def test_missing_key_rows():
    df = pd.DataFrame({
        "id": ["A", None, "", "  ", "B"],
    })

    result = missing_key_rows(df, "id")

    assert len(result) == 3


def test_orphan_rows():
    child = pd.DataFrame({
        "customer_id": ["C1", "C2", None, ""],
    })
    parent = pd.DataFrame({
        "customer_id": ["C1"],
    })

    result = orphan_rows(child, "customer_id", parent, "customer_id")

    assert result["customer_id"].tolist() == ["C2"]


def test_exact_duplicate_rows():
    df = pd.DataFrame({
        "id": ["A", "A", "B"],
        "value": [1, 1, 2],
    })

    result = exact_duplicate_rows(df)

    assert len(result) == 1
    assert result.iloc[0]["id"] == "A"
