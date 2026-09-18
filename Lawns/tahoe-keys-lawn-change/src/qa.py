"""
src/qa.py — Reusable QA checks for TRPA data pipelines.

Each check returns a plain dict summarizing findings. Checks never raise —
they produce a report. Humans review the report before the load step runs.

Copy this file verbatim into src/qa.py of any new TRPA pipeline repo.
Extend with project-specific checks as needed.
"""
from typing import Any

import pandas as pd


def check_nulls(df: pd.DataFrame, required_cols: list[str]) -> dict[str, Any]:
    """Count nulls in required columns. Returns {col: null_count_or_missing}."""
    result: dict[str, Any] = {}
    for col in required_cols:
        if col not in df.columns:
            result[col] = "MISSING COLUMN"
        else:
            result[col] = int(df[col].isna().sum())
    return result


def check_duplicates(df: pd.DataFrame, key_cols: list[str]) -> dict[str, Any]:
    """Count duplicate rows by key columns."""
    missing = [c for c in key_cols if c not in df.columns]
    if missing:
        return {"key_cols": key_cols, "error": f"missing columns: {missing}"}
    dup_count = df.duplicated(subset=key_cols, keep=False).sum()
    return {"key_cols": key_cols, "duplicate_rows": int(dup_count)}


def check_row_count(
    df: pd.DataFrame, expected_min: int, expected_max: int
) -> dict[str, Any]:
    """Flag if row count falls outside expected range."""
    n = len(df)
    status = "ok"
    if n < expected_min:
        status = f"LOW — expected at least {expected_min}"
    elif n > expected_max:
        status = f"HIGH — expected at most {expected_max}"
    return {"rows": n, "expected_min": expected_min, "expected_max": expected_max, "status": status}


def check_value_domain(
    df: pd.DataFrame, col: str, allowed: list
) -> dict[str, Any]:
    """Count rows with values outside the allowed set."""
    if col not in df.columns:
        return {"column": col, "error": "MISSING COLUMN"}
    mask = ~df[col].isin(allowed) & df[col].notna()
    return {
        "column": col,
        "allowed": allowed,
        "unexpected_rows": int(mask.sum()),
        "unexpected_values": sorted(df.loc[mask, col].dropna().unique().tolist()),
    }


def check_spatial_nulls(
    joined_gdf, join_col: str, source_count: int
) -> dict[str, Any]:
    """
    For a spatial-joined GeoDataFrame, count rows where the join column is null.
    High rates (>5%) usually indicate a CRS mismatch or a stale reference layer.
    """
    null_count = int(joined_gdf[join_col].isna().sum())
    pct = (null_count / source_count * 100) if source_count > 0 else 0.0
    status = "ok"
    if pct > 5:
        status = f"HIGH spatial null rate: {pct:.1f}%"
    elif pct > 1:
        status = f"elevated spatial null rate: {pct:.1f}%"
    return {
        "join_col": join_col,
        "null_count": null_count,
        "source_count": source_count,
        "null_pct": round(pct, 2),
        "status": status,
    }


def check_yoy_change(
    current: pd.DataFrame,
    previous: pd.DataFrame,
    group_col: str,
    value_col: str,
    threshold_pct: float = 30.0,
) -> dict[str, Any]:
    """
    Flag groups with year-over-year change exceeding threshold_pct (absolute).
    Common TRPA case: unit counts by jurisdiction, where a >30% swing
    typically warrants human review before loading.
    """
    cur = current.groupby(group_col)[value_col].sum()
    prev = previous.groupby(group_col)[value_col].sum()
    joined = pd.concat([prev.rename("prev"), cur.rename("cur")], axis=1).fillna(0)
    joined["pct_change"] = ((joined["cur"] - joined["prev"]) / joined["prev"].replace(0, pd.NA)) * 100
    flagged = joined[joined["pct_change"].abs() > threshold_pct]
    return {
        "group_col": group_col,
        "threshold_pct": threshold_pct,
        "flagged_groups": flagged.to_dict("index"),
        "flagged_count": len(flagged),
    }
