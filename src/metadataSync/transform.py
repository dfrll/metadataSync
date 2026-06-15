#! /usr/bin/env python3
"""
All functions take a DataFrame (or primitive) and return a DataFrame (or primitive).
"""

import re
import logging
from pathlib import Path

import polars as pl

log = logging.getLogger(__name__)


def normalize_colnames(df: pl.DataFrame) -> pl.DataFrame:
    """
    Lowercase all column names, replace '-' with '_', and deduplicate.
    """
    # Lowercase and replace '-' -> '_'
    cols = [c.lower().replace("-", "_") for c in df.columns]

    # Deduplicate: keep first occurrence, append _1, _2... if needed
    seen = {}
    final_cols = []
    for c in cols:
        if c in seen:
            seen[c] += 1
            final_cols.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            final_cols.append(c)

    return df.rename(dict(zip(df.columns, final_cols)))


def filter_sparse_columns(df: pl.DataFrame, min_coverage: float = 0.05) -> pl.DataFrame:
    """
    Drop columns where fewer than min_coverage fraction of rows are non-null.
    Prevents sparse biosample attributes from bloating the schema.
    """
    threshold = int(len(df) * min_coverage)
    cols_to_keep = [
        col for col in df.columns if df[col].is_not_null().sum() >= threshold
    ]
    dropped = len(df.columns) - len(cols_to_keep)
    if dropped:
        log.info(
            "Dropped %d sparse columns below %.0f%% coverage threshold",
            dropped,
            min_coverage * 100,
        )
    return df.select(cols_to_keep)


def pivot_biosample(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transform long-format biosample attributes into a wide DataFrame.

    Input columns: biosample, key, value
    Output: one row per biosample, one column per attribute key.
    Multi-valued keys are joined with a comma.
    """
    return (
        df.with_columns(pl.col("key").str.replace_all(" ", "_").str.to_lowercase())
        .group_by(["biosample", "key"])
        .agg(pl.col("value").unique().str.join(","))
        .pivot(
            index="biosample",
            on="key",
            values="value",
        )
    )


def join_runinfo_biosample(
    runinfo: pl.DataFrame,
    biosample: pl.DataFrame,
) -> pl.DataFrame:
    """
    Inner join runinfo and biosample on the biosample accession column.
    Logs row counts before and after so data loss at the join is visible.
    """

    before = runinfo.height
    df = biosample.join(runinfo, on="biosample", how="inner")
    after = df.height

    if after < before:
        log.info(
            "Join dropped %d/%d runinfo rows with no matching biosample",
            before - after,
            before,
        )

    if after == 0:
        raise ValueError(
            "Join produced an empty DataFrame, check biosample fetch completeness"
        )
    return df


def rename_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Ensure DataFrame has 'project' and 'external_id' columns."""

    # Handle project
    if "srastudy" in df.columns:
        df = df.with_columns(pl.col("srastudy").alias("project"))
    elif "project" not in df.columns:
        df = df.with_columns(pl.lit(None).alias("project"))

    # Handle external_id
    if "run" in df.columns:
        df = df.with_columns(pl.col("run").alias("external_id"))
    elif "external_id" not in df.columns:
        df = df.with_columns(pl.lit(None).alias("external_id"))

    return df


def strip_unistr(line: str) -> str:
    """
    Replace SQLite unistr() calls with their decoded string literals.
    D1 does not support unistr(), so these must be expanded before upload.

    Example: unistr('caf\\u00e9') -> 'café'
    """

    def decode(match: re.Match) -> str:
        decoded = match.group(1).encode("utf-8").decode("unicode_escape")
        return f"'{decoded}'"

    return re.sub(
        r"unistr\('([^']*)'\)",
        decode,
        line,
        flags=re.IGNORECASE,
    )


def concatenate_csvs(paths: list[Path]) -> pl.DataFrame:
    """
    Read and vertically concatenate a list of CSV files.
    All files are read with infer_schema_length=0 (all columns as strings)
    to avoid type inference conflicts across batches.
    Returns an empty DataFrame if paths is empty.
    """
    if not paths:
        return pl.DataFrame()

    return pl.concat(
        [pl.read_csv(p, infer_schema_length=0) for p in paths],
        how="vertical",
    )


def parse_biosample_csvs(paths: list[Path]) -> pl.DataFrame:
    """
    Read long-format biosample CSVs and concatenate them.
    Columns: biosample, organism, attribute_name, attribute_value.
    Returns an empty DataFrame if paths is empty.
    """
    if not paths:
        return pl.DataFrame()

    dfs = [pl.read_csv(p, infer_schema_length=0) for p in paths]
    return pl.concat(dfs, how="vertical").rename(
        {
            "attribute_name": "key",
            "attribute_value": "value",
        }
    )
