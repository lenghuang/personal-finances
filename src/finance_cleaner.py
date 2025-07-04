"""finance_cleaner.py
A small utility module for loading, cleaning and de-duplicating personal
finance transaction CSVs that follow the Fidelity export layout.

Improvements over the notebook prototype:
1.   Clearer function names & extensive doc-strings.
2.   Hashing logic separated for *within-file* vs *across-file* duplicates.
3.   Robust type conversions that gracefully handle unexpected formats.
4.   Pipeline-style `clean_transactions` helper for one-liner usage.

Typical usage (in another notebook / script):

>>> import finance_cleaner as fc
>>> tidy_df = fc.clean_transactions("../data/transactions_*.csv")
"""

from __future__ import annotations

import glob
import hashlib
import re
from pathlib import Path
from typing import Iterable, List

import numpy as np
import pandas as pd

__all__ = [
    "read_transactions",
    "add_row_hashes",
    "convert_types",
    "coalesce_duplicates",
    "clean_transactions",
]

###############################################################################
# I/O helpers
###############################################################################


def read_transactions(pattern: str = "../data/transactions_*.csv") -> pd.DataFrame:
    """Read all CSVs matching *pattern* and add a *SourceFile* column.

    Parameters
    ----------
    pattern : str, optional
        Glob pattern for locating the transaction CSV files.

    Returns
    -------
    pd.DataFrame
        Concatenated frame of all CSVs; empty if no files exist.
    """
    print("\n=== READING IN FILES ===\n")

    files: List[str] = sorted(glob.glob(pattern))
    if not files:
        print(f"No CSVs matched pattern {pattern!r}")
        return pd.DataFrame()

    frames: List[pd.DataFrame] = []
    for f in files:
        try:
            file_name = Path(f).name
            df = pd.read_csv(f)
            df["SourceFile"] = file_name
            print(f"✅ Read in frame {file_name} with shape {df.shape}")
            frames.append(df)
        except Exception as exc:  # pragma: no cover – we just warn.
            print(f"[WARN] Skipping {f!s}: {exc}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


###############################################################################
# Hash helpers – for identifying duplicates.
###############################################################################


def _row_hash(row: pd.Series, *, include_source: bool) -> str:
    """Generate a stable 5-char SHA-256 hash for *row*.

    If *include_source* is False the *SourceFile* column is excluded to capture
    duplicates **within** the same file only ("intra" duplicates).
    """
    vals: Iterable[str]
    if include_source:
        vals = row.astype(str).values  # all cols incl. SourceFile
    else:
        vals = row.drop(labels=["SourceFile"], errors="ignore").astype(str).values

    joined: str = "".join(vals)
    return hashlib.sha256(joined.encode()).hexdigest()[:5]


def add_row_hashes(df: pd.DataFrame) -> pd.DataFrame:
    """Attach Intra/Cross-file grouping keys and deterministic row IDs.

    The resulting columns are:
    * `CrossKey`  – unique identifier across all files, used to deduplicate within files
    * `IntraKey`  – unique identifier based on content regardless of file, used to deduplicate across files
    * `RowID`     – stable index built from `CrossKey` + duplicate counter
    """
    if df.empty:
        return df.copy()

    print("\n=== ADDING ROW HASHES ===\n")

    intra, cross = zip(
        *df.apply(
            lambda r: (
                _row_hash(r, include_source=False),
                _row_hash(r, include_source=True),
            ),
            axis=1,
        )
    )
    df = df.copy()
    df["IntraKey"] = intra
    df["CrossKey"] = cross

    # Create deterministic unique row identifier ➜ CrossKey suffix with counter
    df["RowID"] = (
        df.groupby("CrossKey").cumcount().add(1).astype(str).radd(df["CrossKey"] + "_")
    )
    df.set_index("RowID", inplace=True)

    print("✅ Created columns IntraKey, CrossKey, and RowID")
    print("✅ Set RowID as index")
    return df


###############################################################################
# Data type coercions
###############################################################################


def _to_bool(s: pd.Series) -> pd.Series:
    mapping = {
        "yes": True,
        "no": False,
        "y": True,
        "n": False,
        True: True,
        False: False,
    }
    return s.str.lower().map(mapping)


def _parse_amount(col: pd.Series) -> pd.Series:
    # Remove $ signs/spaces, handle parentheses for negatives.
    cleaned = col.astype(str).str.replace(r"[$,\s]", "", regex=True)
    cleaned = cleaned.str.replace(r"^\(([-\d.]+)\)$", r"-\1", regex=True)
    return pd.to_numeric(cleaned, errors="coerce")


def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise column dtypes (Date ➜ datetime, Amount ➜ float, flags ➜ bool)."""
    if df.empty:
        return df

    print("\n=== CONVERTING TYPES ===\n")

    df = df.copy()
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        print("✅ Converted date string to_datetime")

    for col in ("Is Hidden", "Is Pending"):
        if col in df.columns:
            df[col] = _to_bool(df[col].astype(str))
            print("✅ Converted boolean strings to boolean")

    if "Amount" in df.columns:
        df["Amount"] = _parse_amount(df["Amount"])
        print("✅ Converted amount strings to numeric")

    return df


###############################################################################
# De-duplication helpers
###############################################################################


def coalesce_duplicates(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Group on *key* and roll-up numeric columns with sum, others with first."""
    if key not in df.columns:
        raise KeyError(f"Column {key!r} not in DataFrame")

    print(f"\n=== COALESCING DUPLICATES on key='{key}' ===\n")

    grouped = df.groupby(key, dropna=False)

    # For transparency, show which rows are being merged.
    show_rows = 3
    for group_key, group_df in grouped:
        if len(group_df) > 1 and show_rows > 0:
            show_rows -= 1
            print(
                f"  Merging {len(group_df)} rows for group {group_key!r}. "
                f"RowIDs: {group_df.index.tolist()}"
            )

    if show_rows == 0:
        print("  ... (remaining rows hidden)")

    numeric_cols = df.select_dtypes("number").columns.difference([key])
    other_cols = df.columns.difference(numeric_cols.union([key]))

    agg_spec = {**{c: "sum" for c in numeric_cols}, **{c: "first" for c in other_cols}}
    grouped = grouped.agg(agg_spec).reset_index()

    print(f"\n✅ Total rows after de-duplication: {len(grouped)}")

    return grouped


def remove_duplicates(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Remove duplicate rows, keeping the first occurrence per *key*."""
    if key not in df.columns:
        raise KeyError(f"Column {key!r} not in DataFrame")

    print(f"\n=== REMOVING DUPLICATES on key='{key}' ===\n")

    # Track duplicates before removal
    dup_mask = df.duplicated(subset=key, keep="first")
    dup_count = dup_mask.sum()

    if dup_count > 0:
        print(f"  Removing {dup_count} duplicate rows:")
        for row_id in df[dup_mask].index.tolist()[:5]:
            row = df.loc[row_id]
            details = [f"RowID: {row_id}"]
            if "Date" in df.columns:
                details.append(f"Date: {row['Date']}")
            if "Description" in df.columns:
                details.append(f"Desc: {row['Description']}")
            if "Amount" in df.columns:
                details.append(f"Amt: {row['Amount']}")
            print("  " + " | ".join(details))

        if dup_count > 3:
            print(f"  ... and {dup_count-3} more duplicates")

    return df.drop_duplicates(subset=key, keep="first")


###############################################################################
# Full pipeline convenience wrapper
###############################################################################


def clean_transactions(pattern: str = "../data/transactions_*.csv") -> pd.DataFrame:
    """Load → type-convert → hash → de-duplicate within files."""
    df = read_transactions(pattern)
    if df.empty:
        return df
    df = convert_types(df)
    df = add_row_hashes(df)

    # Coalesce *within* file duplicates (CrossKey)
    df = coalesce_duplicates(df, key="CrossKey")

    # Remove *across* file duplicates (IntraKey)
    df = remove_duplicates(df, key="IntraKey")

    return df


###############################################################################
# CLI / quick demo when executed as a script
###############################################################################

if __name__ == "__main__":  # pragma: no cover – quick preview only
    import argparse

    parser = argparse.ArgumentParser(description="Clean personal finance CSVs.")
    parser.add_argument(
        "pattern",
        nargs="?",
        default="../data/transactions_*.csv",
        help="Glob pattern to locate CSVs (default: %(default)s)",
    )
    args = parser.parse_args()

    cleaned = clean_transactions(args.pattern)
    print(f"\n\n✅ Cleaned DataFrame shape: {cleaned.shape}\n")
    print(cleaned.head())
