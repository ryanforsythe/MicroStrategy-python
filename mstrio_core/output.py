"""
Output helpers for MicroStrategy scripts.

Provides standardized CSV (semicolon-delimited), Excel, and DataFrame output,
plus common data-shaping utilities used across scripts.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Optional, Sequence, Union

import pandas as pd
from loguru import logger

# Register the project-standard CSV dialect once at import time.
# Uses semicolons to avoid conflicts with commas in MicroStrategy object names.
csv.register_dialect(
    "mstr_csv",
    delimiter=";",
    quoting=csv.QUOTE_NONNUMERIC,
    lineterminator="\n",
)


def write_csv(
    rows: Sequence[Sequence[Any]],
    columns: Sequence[str],
    path: Union[str, Path],
    *,
    mode: str = "w",
    encoding: str = "utf-8",
) -> Path:
    """
    Write rows to a semicolon-delimited CSV file.

    Args:
        rows:     Iterable of row sequences (one per record).
        columns:  Column header names.
        path:     Output file path.
        mode:     File open mode. "w" (overwrite) or "a" (append).
        encoding: File encoding. Default utf-8.

    Returns:
        Resolved Path of the written file.

    Example:
        write_csv(
            rows=[[guid, name, location], ...],
            columns=["GUID", "Name", "Location"],
            path=config.output_dir / "reports.csv",
        )
    """
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)

    write_header = mode == "w" or not out.exists()

    with out.open(mode, newline="", encoding=encoding) as f:
        writer = csv.writer(f, dialect="mstr_csv")
        if write_header:
            writer.writerow(columns)
        writer.writerows(rows)

    logger.success(
        "CSV written: {path} ({count} rows)", path=out, count=len(rows)
    )
    return out


def write_excel(
    data: Union[pd.DataFrame, Sequence[Sequence[Any]]],
    path: Union[str, Path],
    *,
    columns: Optional[Sequence[str]] = None,
    sheet_name: str = "Sheet1",
    index: bool = False,
) -> Path:
    """
    Write a DataFrame or list of rows to an Excel file.

    Args:
        data:       DataFrame or list of row sequences.
        path:       Output file path (.xlsx).
        columns:    Column names — required when data is a list of rows.
        sheet_name: Excel worksheet name. Default "Sheet1".
        index:      Include DataFrame index. Default False.

    Returns:
        Resolved Path of the written file.

    Example:
        write_excel(rows, path=config.output_dir / "metrics.xlsx", columns=["GUID", "Name"])
        write_excel(df, path=config.output_dir / "metrics.xlsx")
    """
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)

    if isinstance(data, pd.DataFrame):
        df = data
    else:
        if columns is None:
            raise ValueError("columns must be provided when data is a list of rows.")
        df = pd.DataFrame(data, columns=columns)

    df.to_excel(out, sheet_name=sheet_name, index=index)

    logger.success(
        "Excel written: {path} ({rows} rows, {cols} cols)",
        path=out,
        rows=len(df),
        cols=len(df.columns),
    )
    return out


def read_excel(
    path: Union[str, Path],
    sheet: Union[str, int] = 0,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Read an Excel file into a DataFrame.

    Args:
        path:   Input file path.
        sheet:  Sheet name or zero-based index. Default 0 (first sheet).
        **kwargs: Passed through to pd.read_excel().

    Returns:
        DataFrame of the sheet contents.

    Example:
        df = read_excel(xlsx_file)
        guid_list = df["Object GUID"].tolist()
    """
    src = Path(path)
    if not src.exists():
        raise FileNotFoundError(f"Excel file not found: {src}")

    df = pd.read_excel(src, sheet_name=sheet, **kwargs)
    logger.info(
        "Excel read: {path} ({rows} rows)", path=src, rows=len(df)
    )
    return df


def object_location(ancestors: list[dict]) -> str:
    """
    Build a folder path string from a MicroStrategy ancestors list.

    The ancestors list is returned by the REST API when includeAncestors=true.
    The first entry is always the root node; we skip it and join the rest.

    Args:
        ancestors: List of ancestor dicts, each containing at least a "name" key.

    Returns:
        Slash-prefixed path string, e.g. "/Shared Reports/Finance".

    Example:
        location = object_location(search_result["ancestors"])
    """
    named = [a["name"] for a in ancestors if "name" in a]
    # named[0] is the root ("MicroStrategy Object") — skip it
    return "/" + "/".join(named[1:]) if len(named) > 1 else "/"


def to_dataframe(data: Union[list[dict], dict]) -> pd.DataFrame:
    """
    Convert a list of dicts or a single dict to a DataFrame.

    Convenience wrapper around pd.DataFrame.from_dict / pd.DataFrame.

    Example:
        df = to_dataframe(env.list_loaded_projects(to_dictionary=True))
    """
    if isinstance(data, list):
        return pd.DataFrame(data)
    return pd.DataFrame.from_dict(data)
