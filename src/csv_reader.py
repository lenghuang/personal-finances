import glob
import hashlib
from pathlib import Path
import pandas as pd

def read_transactions(
    pattern: str = "../data/transactions_*.csv",
    show_duplicates: bool = False
) -> pd.DataFrame:
    """
    Load, clean, and de-duplicate personal finance transaction CSVs.

    Parameters:
    -----------
    pattern : str
        Glob pattern for locating transaction CSV files
    show_duplicates : bool
        Whether to print details about duplicate rows being processed

    Returns:
    --------
    pd.DataFrame
        Cleaned and de-duplicated transaction data
    """
    def _row_hash(row: pd.Series, include_source: bool) -> str:
        """Generate a stable 5-char SHA-256 hash for a row.

        Args:
            row: The row of data to hash
            include_source: If True, includes the source filename in the hash.
                          Use False to identify same transactions across files.
                          Use True to identify unique rows including their source.
        """
        vals = row.astype(str).values if include_source else row.drop('SourceFile', errors='ignore').astype(str).values
        return hashlib.sha256(''.join(vals).encode()).hexdigest()[:5]

    def _parse_amount(series: pd.Series) -> pd.Series:
        """Parse amount strings into numeric values."""
        cleaned = series.astype(str).str.replace(r"[$,\s]", "", regex=True)
        cleaned = cleaned.str.replace(r"^\(([-\d.]+)\)$", r"-\1", regex=True)
        return pd.to_numeric(cleaned, errors="coerce")

    # 1. Read and combine files
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"No CSVs matched pattern {pattern!r}")
        return pd.DataFrame()

    frames = []
    for f in files:
        try:
            df = pd.read_csv(f)
            df['SourceFile'] = Path(f).name
            frames.append(df)
        except Exception as e:
            print(f"[WARN] Skipping {f}: {e}")

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # 2. Convert types
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

    for col in ('Is Hidden', 'Is Pending'):
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().map({
                'yes': True, 'y': True, 'true': True,
                'no': False, 'n': False, 'false': False
            }).fillna(False)

    if 'Amount' in df.columns:
        df['Amount'] = _parse_amount(df['Amount'])

    # 3. Add hashes for deduplication
    # This identifies the same transaction across different files
    df['TransactionFingerprint'] = df.apply(
        lambda r: _row_hash(r, include_source=False),
        axis=1
    )

    # This creates a unique identifier for each row including its source file
    df['UniqueRowFingerprint'] = df.apply(
        lambda r: _row_hash(r, include_source=True),
        axis=1
    )

    # Create a stable row ID for reference
    df['RowID'] = df.groupby('UniqueRowFingerprint').cumcount().add(1).astype(str)
    df['RowID'] = df['UniqueRowFingerprint'] + '_' + df['RowID']
    df.set_index('RowID', inplace=True)

    # 4. Deduplication
    # First pass: merge duplicates within each file (same UniqueRowFingerprint)
    def _merge_duplicates(group):
        numeric_cols = group.select_dtypes('number').columns.difference([
            'TransactionFingerprint',
            'UniqueRowFingerprint'
        ])
        other_cols = group.columns.difference(
            numeric_cols.union([
                'UniqueRowFingerprint'  # Only exclude UniqueRowFingerprint from other_cols
            ])
        )
        agg = {**{c: 'sum' for c in numeric_cols},
              **{c: 'first' for c in other_cols}}
        return group.groupby('UniqueRowFingerprint', dropna=False).agg(agg).reset_index()

    df = _merge_duplicates(df)

    # Second pass: remove duplicates across files (same TransactionFingerprint)
    # Use drop_duplicates instead of duplicated() for cleaner code
    df = df.drop_duplicates(subset=['TransactionFingerprint'], keep='first')

    return df