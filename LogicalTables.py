"""
LogicalTables.py — Export and compare MicroStrategy logical table definitions.

Subcommands
───────────
  export  — Document all logical tables in a project.  Outputs table metadata
             (ID, name, type, physical table, logical size …) and the
             attributes / facts mapped to each table with key indicators.

  compare — Compare logical tables between two projects (same or different
             environments).  By default shows only differences; pass --all to
             include matching tables/objects.

Output formats
──────────────
  csv   — Two files: *_tables.csv + *_objects.csv  (default)
  json  — Single nested file
  excel — Single workbook with Tables + TableObjects sheets

Usage
─────
  python LogicalTables.py export  <env> <project>
                                  [--format csv|json|excel] [--output-dir PATH]

  python LogicalTables.py compare <env> <project> <env2> [<project2>]
                                  [--all] [--format csv|json|excel]
                                  [--output-dir PATH]

Examples
────────
  # Export all tables in "My Project" on dev
  python LogicalTables.py export dev "My Project"

  # Export to Excel (2 sheets)
  python LogicalTables.py export dev "My Project" --format excel

  # Compare tables between dev and qa (same project name)
  python LogicalTables.py compare dev "My Project" qa

  # Compare different projects on the same environment
  python LogicalTables.py compare prod "Project A" prod "Project B"

  # Compare — show all tables including matches
  python LogicalTables.py compare dev "My Project" qa --all
"""

import argparse
import json as _json
from pathlib import Path

import pandas as pd
from loguru import logger

from mstrio_core import MstrConfig, MstrRestSession, object_location, write_csv
from mstrio_core.config import MstrEnvironment

# ── Constants ─────────────────────────────────────────────────────────────────

ENVS = [e.value for e in MstrEnvironment]

_TABLES_CSV_COLS = [
    "table_id",
    "table_name",
    "description",
    "sub_type",
    "ext_type",
    "date_created",
    "date_modified",
    "location",
    "is_logical_size_locked",
    "logical_size",
    "physical_table_name",
    "physical_table_id",
]

_OBJECTS_CSV_COLS = [
    "table_id",
    "table_name",
    "object_type",
    "object_name",
    "object_id",
    "is_key",
]

_CMP_TABLES_CSV_COLS = [
    "table_name",
    "source_table_id",
    "target_table_id",
    "source_is_locked",
    "target_is_locked",
    "locked_match",
    "source_logical_size",
    "target_logical_size",
    "size_match",
    "source_env",
    "source_project",
    "target_env",
    "target_project",
    "status",
]

_CMP_OBJECTS_CSV_COLS = [
    "table_name",
    "object_type",
    "object_name",
    "source_object_id",
    "target_object_id",
    "source_is_key",
    "target_is_key",
    "is_key_match",
    "source_env",
    "source_project",
    "target_env",
    "target_project",
    "status",
]

# ── Internal helpers ──────────────────────────────────────────────────────────


def _make_config(env: str) -> MstrConfig:
    return MstrConfig(environment=MstrEnvironment(env))


def _out_dir(config: MstrConfig, output_dir: Path | None) -> Path:
    d = output_dir or config.output_dir
    d.mkdir(parents=True, exist_ok=True)
    return d


def _dicts_to_rows(dicts: list[dict], columns: list[str]) -> list[list]:
    return [[d.get(c, "") for c in columns] for d in dicts]


def _safe_str(val) -> str:
    """Convert mstrio enum/object values to clean strings.

    Strips prefixes like 'ObjectSubType.LOGICAL_TABLE' → 'LOGICAL_TABLE'.
    """
    if val is None:
        return ""
    s = str(val)
    for prefix in (
        "ObjectTypes.",
        "ObjectSubType.",
        "ObjectSubTypes.",
        "ExtendedType.",
    ):
        if s.startswith(prefix):
            return s[len(prefix):]
    return s


def _write_excel_multi(
    sheets: dict[str, tuple[list[list], list[str]]], path: Path
) -> None:
    """Write multiple named sheets to a single Excel workbook."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet_name, (rows, columns) in sheets.items():
            df = pd.DataFrame(rows, columns=columns)
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    logger.success("Excel written: {path}", path=path)


# ── mstrio-py data fetching ──────────────────────────────────────────────────


def _get_key_ids_sdk(table) -> set[str]:
    """Return key attribute IDs from an mstrio-py LogicalTable object."""
    key_ids: set[str] = set()
    for tk in getattr(table, "table_key", None) or []:
        # SchemaObjectReference → .object_id; fallback → .id
        oid = getattr(tk, "object_id", None) or getattr(tk, "id", None)
        if oid:
            key_ids.add(str(oid))
    return key_ids


def _extract_table_sdk(t) -> dict:
    """Convert a single mstrio-py LogicalTable object into a normalised dict.

    Accesses every lazy-loaded property eagerly so all server
    round-trips happen while the connection is still alive.
    Raises on server errors so callers can catch per-table failures.
    """
    key_ids = _get_key_ids_sdk(t)

    # ── Mapped objects (attributes + facts) ──────────────────────────
    objects: list[dict] = []
    for attr in getattr(t, "attributes", None) or []:
        attr_id = str(getattr(attr, "id", ""))
        objects.append(
            {
                "object_type": "Attribute",
                "object_name": getattr(attr, "name", ""),
                "object_id": attr_id,
                "is_key": attr_id in key_ids,
            }
        )
    for fact in getattr(t, "facts", None) or []:
        objects.append(
            {
                "object_type": "Fact",
                "object_name": getattr(fact, "name", ""),
                "object_id": str(getattr(fact, "id", "")),
                "is_key": "",
            }
        )

    # ── Physical table ────────────────────────────────────────────────
    pt = getattr(t, "physical_table", None)
    pt_name = getattr(pt, "name", "") if pt else ""
    pt_id = getattr(pt, "id", "") if pt else ""

    return {
        "table_id": t.id,
        "table_name": t.name,
        "description": t.description or "",
        "sub_type": _safe_str(
            getattr(t, "sub_type", getattr(t, "subtype", ""))
        ),
        "ext_type": _safe_str(getattr(t, "ext_type", "")),
        "date_created": str(t.date_created) if t.date_created else "",
        "date_modified": str(t.date_modified) if t.date_modified else "",
        "location": getattr(t, "location", "") or "",
        "is_logical_size_locked": getattr(
            t, "is_logical_size_locked", ""
        ),
        "logical_size": getattr(t, "logical_size", ""),
        "physical_table_name": pt_name,
        "physical_table_id": pt_id,
        "objects": objects,
    }


# Maximum consecutive per-table failures before aborting the SDK path
# and falling back to REST.  Avoids waiting for N × timeout_seconds
# when the I-Server modeling service is unreachable.
_SDK_CONSECUTIVE_FAIL_LIMIT = 3


class _SdkFetchAborted(Exception):
    """Raised when the SDK path hits too many consecutive failures."""


def _fetch_all_tables_sdk(conn, project_name: str) -> list[dict]:
    """Fetch all logical tables via mstrio-py and normalise to dicts.

    ``list_logical_tables()`` returns lightweight stub objects — most
    properties are lazy-loaded via ``__getattribute__`` → ``fetch()``.
    We eagerly extract every needed property while the connection is
    active and wrap each table in a try/except so a single server
    error (e.g. JWT expiry) does not abort the entire run.

    If ``_SDK_CONSECUTIVE_FAIL_LIMIT`` consecutive tables fail (e.g.
    timeouts on an unreachable modeling service), raises
    ``_SdkFetchAborted`` so the caller can switch to the REST fallback
    immediately instead of waiting for every table to time out.
    """
    from mstrio.modeling.schema import list_logical_tables

    logger.info("Fetching logical tables for project {p!r} (mstrio-py) ...", p=project_name)
    tables = list_logical_tables(conn, project_name=project_name)
    logger.info("Found {n} logical tables", n=len(tables))

    result: list[dict] = []
    errors = 0
    consecutive_errors = 0
    total = len(tables)
    for i, t in enumerate(sorted(tables, key=lambda x: x.name.lower()), 1):
        try:
            result.append(_extract_table_sdk(t))
            consecutive_errors = 0  # reset on success
        except Exception as e:
            errors += 1
            consecutive_errors += 1
            logger.warning(
                "Skipping table {name!r} (ID: {id}): {err}",
                name=getattr(t, "name", "?"),
                id=getattr(t, "id", "?"),
                err=e,
            )
            if consecutive_errors >= _SDK_CONSECUTIVE_FAIL_LIMIT:
                raise _SdkFetchAborted(
                    f"{consecutive_errors} consecutive table fetches failed "
                    f"(last: {e})"
                ) from e
        if i % 50 == 0 or i == total:
            logger.info(
                "Processing table definitions: {i}/{n}", i=i, n=total
            )

    if errors:
        logger.warning(
            "{errors} of {total} tables could not be read — see warnings above",
            errors=errors,
            total=total,
        )
    logger.info(
        "Processed {n}/{total} logical tables for project {p!r}",
        n=len(result),
        total=total,
        p=project_name,
    )
    return result


# ── REST API fallback ────────────────────────────────────────────────────────


def _search_tables_rest(session: MstrRestSession) -> list[dict]:
    """Search for all logical tables (type 15) in the current project."""
    results: list[dict] = []
    offset = 0
    limit = 1000
    while True:
        r = session.get(
            "/searches/results",
            params={
                "type": 15,
                "getAncestors": True,
                "offset": offset,
                "limit": limit,
            },
        )
        r.raise_for_status()
        data = r.json()
        batch = data.get("result", [])
        results.extend(batch)
        total = data.get("totalItems", len(results))
        if len(results) >= total or not batch:
            break
        offset += len(batch)
    logger.info("Search found {n} logical tables", n=len(results))
    return results


def _fetch_table_definition_rest(
    session: MstrRestSession, table_id: str
) -> dict | None:
    """Fetch the full modeling definition for one logical table via REST."""
    try:
        r = session.get(f"/model/tables/{table_id}")
        r.raise_for_status()
        logger.debug(
            "Fetched table {id}: HTTP {s}", id=table_id, s=r.status_code
        )
        return r.json()
    except Exception as e:
        logger.warning(
            "Failed to fetch table definition {id}: {err}", id=table_id, err=e
        )
        return None


def _extract_key_ids_rest(table_def: dict) -> set[str]:
    """Return the set of attribute object-IDs from the REST tableKey array."""
    key_list = table_def.get("tableKey", [])
    return {
        k.get("objectId", k.get("id", ""))
        for k in key_list
        if isinstance(k, dict)
    }


def _fetch_all_tables_rest(session: MstrRestSession) -> list[dict]:
    """Fetch all logical tables via REST API (search + model/tables).

    Used as fallback when mstrio-py list_logical_tables() is not
    supported by the target I-Server version.
    """
    search_results = _search_tables_rest(session)
    if not search_results:
        return []

    # Build a location map from search ancestors
    location_map: dict[str, str] = {}
    for sr in search_results:
        tid = sr.get("id", "")
        ancestors = sr.get("ancestors", [])
        location_map[tid] = object_location(ancestors) if ancestors else ""

    tables: list[dict] = []
    total = len(search_results)
    for i, sr in enumerate(search_results, 1):
        tid = sr.get("id", "")
        name = sr.get("name", "")

        if i % 50 == 0 or i == total:
            logger.info(
                "Fetching table definitions (REST): {i}/{n}", i=i, n=total
            )

        tdef = _fetch_table_definition_rest(session, tid)
        if tdef is None:
            continue

        info = tdef.get("information", {})
        phys = tdef.get("physicalTable", {})
        phys_info = phys.get("information", {}) if isinstance(phys, dict) else {}

        # Determine key attributes
        key_ids = _extract_key_ids_rest(tdef)

        # Build objects list (attributes + facts)
        objects: list[dict] = []
        for attr in tdef.get("attributes", []):
            ai = attr.get("information", {})
            aid = ai.get("objectId", "")
            objects.append(
                {
                    "object_type": "Attribute",
                    "object_name": ai.get("name", ""),
                    "object_id": aid,
                    "is_key": aid in key_ids,
                }
            )
        for fact in tdef.get("facts", []):
            fi = fact.get("information", {})
            objects.append(
                {
                    "object_type": "Fact",
                    "object_name": fi.get("name", ""),
                    "object_id": fi.get("objectId", ""),
                    "is_key": "",
                }
            )

        tables.append(
            {
                "table_id": tid,
                "table_name": name,
                "description": info.get(
                    "description", sr.get("description", "")
                ),
                "sub_type": info.get("subType", sr.get("subtype", "")),
                "ext_type": info.get("extType", sr.get("extType", "")),
                "date_created": info.get(
                    "dateCreated", sr.get("dateCreated", "")
                ),
                "date_modified": info.get(
                    "dateModified", sr.get("dateModified", "")
                ),
                "location": location_map.get(tid, ""),
                "is_logical_size_locked": tdef.get(
                    "isLogicalSizeLocked", ""
                ),
                "logical_size": tdef.get("logicalSize", ""),
                "physical_table_name": phys_info.get("name", ""),
                "physical_table_id": phys_info.get("objectId", ""),
                "objects": objects,
            }
        )

    logger.info(
        "Retrieved {n}/{total} table definitions via REST",
        n=len(tables),
        total=total,
    )
    return sorted(tables, key=lambda t: t["table_name"].lower())


# ── Unified fetch (SDK → REST fallback) ──────────────────────────────────────


def _fetch_all_tables(session: MstrRestSession, project_name: str) -> list[dict]:
    """Fetch all logical tables, trying mstrio-py SDK first, REST API as fallback.

    The mstrio-py ``list_logical_tables()`` may fail on older I-Server
    versions or when the modeling service is unreachable.  In that case
    we fall back to REST API discovery (``GET /searches/results?type=15``)
    plus per-table detail fetch (``GET /model/tables/{id}``).

    The SDK path also aborts early (via ``_SdkFetchAborted``) if
    multiple consecutive per-table fetches time out, to avoid waiting
    for every table to fail individually.
    """
    # ── Try mstrio-py SDK first ───────────────────────────────────────────
    try:
        conn = session.mstrio_conn
        return _fetch_all_tables_sdk(conn, project_name)
    except _SdkFetchAborted as abort_err:
        logger.warning(
            "SDK per-table fetch aborted after consecutive failures: {err} "
            "— switching to REST API",
            err=abort_err,
        )
    except Exception as sdk_err:
        logger.warning(
            "mstrio-py list_logical_tables() failed: {err} "
            "— falling back to REST API",
            err=sdk_err,
        )

    # ── Fallback: REST API ────────────────────────────────────────────────
    logger.info(
        "Fetching logical tables for project {p!r} via REST API ...",
        p=project_name,
    )
    return _fetch_all_tables_rest(session)


# ── Row builders ──────────────────────────────────────────────────────────────


def _build_table_rows(tables: list[dict]) -> list[dict]:
    """Flatten tables to rows for the Tables sheet/CSV."""
    rows = []
    for t in tables:
        rows.append(
            {
                "table_id": t["table_id"],
                "table_name": t["table_name"],
                "description": t["description"],
                "sub_type": t["sub_type"],
                "ext_type": t["ext_type"],
                "date_created": t["date_created"],
                "date_modified": t["date_modified"],
                "location": t["location"],
                "is_logical_size_locked": t["is_logical_size_locked"],
                "logical_size": t["logical_size"],
                "physical_table_name": t["physical_table_name"],
                "physical_table_id": t["physical_table_id"],
            }
        )
    return rows


def _build_object_rows(tables: list[dict]) -> list[dict]:
    """Flatten table objects to rows for the TableObjects sheet/CSV."""
    rows = []
    for t in tables:
        for obj in t.get("objects", []):
            rows.append(
                {
                    "table_id": t["table_id"],
                    "table_name": t["table_name"],
                    "object_type": obj["object_type"],
                    "object_name": obj["object_name"],
                    "object_id": obj["object_id"],
                    "is_key": obj["is_key"],
                }
            )
    return rows


# ── Compare logic ─────────────────────────────────────────────────────────────


def _diff_tables(
    src_tables: list[dict],
    tgt_tables: list[dict],
    src_env: str,
    src_project: str,
    tgt_env: str,
    tgt_project: str,
    show_all: bool = False,
) -> tuple[list[dict], list[dict]]:
    """
    Compare logical tables and their objects between two projects.

    Returns (table_diff_rows, object_diff_rows).
    """
    # Index by lowercase name
    src_by_name = {t["table_name"].lower(): t for t in src_tables}
    tgt_by_name = {t["table_name"].lower(): t for t in tgt_tables}
    all_names = sorted(set(src_by_name.keys()) | set(tgt_by_name.keys()))

    table_rows: list[dict] = []
    object_rows: list[dict] = []

    common = {"source_env": src_env, "source_project": src_project,
              "target_env": tgt_env, "target_project": tgt_project}

    for name in all_names:
        src = src_by_name.get(name)
        tgt = tgt_by_name.get(name)

        if src and not tgt:
            # ── Source only ───────────────────────────────────────────────
            table_rows.append(
                {
                    "table_name": src["table_name"],
                    "source_table_id": src["table_id"],
                    "target_table_id": "",
                    "source_is_locked": src["is_logical_size_locked"],
                    "target_is_locked": "",
                    "locked_match": "",
                    "source_logical_size": src["logical_size"],
                    "target_logical_size": "",
                    "size_match": "",
                    **common,
                    "status": "source_only",
                }
            )
            for obj in src.get("objects", []):
                object_rows.append(
                    {
                        "table_name": src["table_name"],
                        "object_type": obj["object_type"],
                        "object_name": obj["object_name"],
                        "source_object_id": obj["object_id"],
                        "target_object_id": "",
                        "source_is_key": obj["is_key"],
                        "target_is_key": "",
                        "is_key_match": "",
                        **common,
                        "status": "source_only",
                    }
                )

        elif tgt and not src:
            # ── Target only ───────────────────────────────────────────────
            table_rows.append(
                {
                    "table_name": tgt["table_name"],
                    "source_table_id": "",
                    "target_table_id": tgt["table_id"],
                    "source_is_locked": "",
                    "target_is_locked": tgt["is_logical_size_locked"],
                    "locked_match": "",
                    "source_logical_size": "",
                    "target_logical_size": tgt["logical_size"],
                    "size_match": "",
                    **common,
                    "status": "target_only",
                }
            )
            for obj in tgt.get("objects", []):
                object_rows.append(
                    {
                        "table_name": tgt["table_name"],
                        "object_type": obj["object_type"],
                        "object_name": obj["object_name"],
                        "source_object_id": "",
                        "target_object_id": obj["object_id"],
                        "source_is_key": "",
                        "target_is_key": obj["is_key"],
                        "is_key_match": "",
                        **common,
                        "status": "target_only",
                    }
                )

        else:
            # ── Both exist — compare properties ───────────────────────────
            locked_match = (
                src["is_logical_size_locked"] == tgt["is_logical_size_locked"]
            )
            size_match = src["logical_size"] == tgt["logical_size"]

            if locked_match and size_match:
                tbl_status = "match"
            else:
                tbl_status = "differs"

            if show_all or tbl_status != "match":
                table_rows.append(
                    {
                        "table_name": src["table_name"],
                        "source_table_id": src["table_id"],
                        "target_table_id": tgt["table_id"],
                        "source_is_locked": src["is_logical_size_locked"],
                        "target_is_locked": tgt["is_logical_size_locked"],
                        "locked_match": locked_match,
                        "source_logical_size": src["logical_size"],
                        "target_logical_size": tgt["logical_size"],
                        "size_match": size_match,
                        **common,
                        "status": tbl_status,
                    }
                )

            # ── Compare objects within this table ─────────────────────────
            src_objs = {
                (o["object_type"].lower(), o["object_name"].lower()): o
                for o in src.get("objects", [])
            }
            tgt_objs = {
                (o["object_type"].lower(), o["object_name"].lower()): o
                for o in tgt.get("objects", [])
            }
            all_obj_keys = sorted(
                set(src_objs.keys()) | set(tgt_objs.keys())
            )

            for ok in all_obj_keys:
                so = src_objs.get(ok)
                to = tgt_objs.get(ok)

                if so and not to:
                    object_rows.append(
                        {
                            "table_name": src["table_name"],
                            "object_type": so["object_type"],
                            "object_name": so["object_name"],
                            "source_object_id": so["object_id"],
                            "target_object_id": "",
                            "source_is_key": so["is_key"],
                            "target_is_key": "",
                            "is_key_match": "",
                            **common,
                            "status": "source_only",
                        }
                    )
                elif to and not so:
                    object_rows.append(
                        {
                            "table_name": src["table_name"],
                            "object_type": to["object_type"],
                            "object_name": to["object_name"],
                            "source_object_id": "",
                            "target_object_id": to["object_id"],
                            "source_is_key": "",
                            "target_is_key": to["is_key"],
                            "is_key_match": "",
                            **common,
                            "status": "target_only",
                        }
                    )
                else:
                    # Both exist
                    if so["object_type"] == "Attribute":
                        key_match = so["is_key"] == to["is_key"]
                        obj_status = "match" if key_match else "key_differs"
                    else:
                        key_match = ""
                        obj_status = "match"

                    if show_all or obj_status != "match":
                        object_rows.append(
                            {
                                "table_name": src["table_name"],
                                "object_type": so["object_type"],
                                "object_name": so["object_name"],
                                "source_object_id": so["object_id"],
                                "target_object_id": to["object_id"],
                                "source_is_key": so["is_key"],
                                "target_is_key": to["is_key"],
                                "is_key_match": key_match,
                                **common,
                                "status": obj_status,
                            }
                        )

    return table_rows, object_rows


# ── Output helpers ────────────────────────────────────────────────────────────


def _write_output(
    table_rows: list[dict],
    table_cols: list[str],
    object_rows: list[dict],
    object_cols: list[str],
    fmt: str,
    stem: str,
    out: Path,
    *,
    json_data: list[dict] | None = None,
) -> None:
    """
    Write table and object rows in the requested format.

    Args:
        table_rows / object_rows: Flat dicts for each output set.
        table_cols / object_cols: Column order.
        fmt:       "csv", "json", or "excel".
        stem:      Base filename without extension.
        out:       Output directory.
        json_data: Nested structure for JSON output (uses table_rows if None).
    """
    if fmt == "csv":
        tp = out / f"{stem}_tables.csv"
        write_csv(
            _dicts_to_rows(table_rows, table_cols),
            columns=table_cols,
            path=tp,
        )
        op = out / f"{stem}_objects.csv"
        write_csv(
            _dicts_to_rows(object_rows, object_cols),
            columns=object_cols,
            path=op,
        )
        logger.success(
            "Wrote {tn} table rows → {tp}  |  {on} object rows → {op}",
            tn=len(table_rows),
            tp=tp,
            on=len(object_rows),
            op=op,
        )

    elif fmt == "json":
        jp = out / f"{stem}.json"
        payload = json_data if json_data is not None else table_rows
        jp.write_text(
            _json.dumps(payload, indent=2, default=str), encoding="utf-8"
        )
        logger.success("JSON written → {path}", path=jp)

    elif fmt == "excel":
        ep = out / f"{stem}.xlsx"
        _write_excel_multi(
            {
                "Tables": (
                    _dicts_to_rows(table_rows, table_cols),
                    table_cols,
                ),
                "TableObjects": (
                    _dicts_to_rows(object_rows, object_cols),
                    object_cols,
                ),
            },
            ep,
        )
        logger.success(
            "Excel written: {tn} table rows + {on} object rows → {path}",
            tn=len(table_rows),
            on=len(object_rows),
            path=ep,
        )

    else:
        raise ValueError(
            f"Unsupported format {fmt!r}. Use 'csv', 'json', or 'excel'."
        )


# ── Operations ────────────────────────────────────────────────────────────────


def export_tables(
    env: str,
    project: str,
    fmt: str = "csv",
    output_dir: Path | None = None,
) -> None:
    """Export all logical tables and their mapped objects from a project."""
    config = _make_config(env)

    with MstrRestSession(config) as session:
        session.set_project(name=project)
        tables = _fetch_all_tables(session, project)

    if not tables:
        logger.warning(
            "No logical tables found in {project!r}.", project=project
        )
        return

    table_rows = _build_table_rows(tables)
    object_rows = _build_object_rows(tables)

    logger.info(
        "{tn} tables, {on} mapped objects",
        tn=len(table_rows),
        on=len(object_rows),
    )

    safe_project = project.replace(" ", "_").replace("/", "-")
    out = _out_dir(config, output_dir)
    stem = f"logical_tables_{safe_project}_{env}"

    _write_output(
        table_rows,
        _TABLES_CSV_COLS,
        object_rows,
        _OBJECTS_CSV_COLS,
        fmt,
        stem,
        out,
        json_data=tables,  # nested for JSON
    )


def compare_tables(
    src_env: str,
    src_project: str,
    tgt_env: str,
    tgt_project: str,
    show_all: bool = False,
    fmt: str = "csv",
    output_dir: Path | None = None,
) -> None:
    """
    Compare logical tables between two projects.

    Compares table properties (is_logical_size_locked, logical_size) and
    mapped objects (attributes/facts, is_key for attributes).
    """
    same_env = src_env == tgt_env

    # ── Source ─────────────────────────────────────────────────────────────
    src_config = _make_config(src_env)
    with MstrRestSession(src_config) as session:
        session.set_project(name=src_project)
        src_tables = _fetch_all_tables(session, src_project)

        # If same environment, fetch target on the same session
        if same_env:
            session.set_project(name=tgt_project)
            tgt_tables = _fetch_all_tables(session, tgt_project)

    # ── Target (cross-environment) ────────────────────────────────────────
    if not same_env:
        tgt_config = _make_config(tgt_env)
        with MstrRestSession(tgt_config) as session:
            session.set_project(name=tgt_project)
            tgt_tables = _fetch_all_tables(session, tgt_project)

    # ── Diff ──────────────────────────────────────────────────────────────
    logger.info(
        "Source: {sn} tables  |  Target: {tn} tables",
        sn=len(src_tables),
        tn=len(tgt_tables),
    )

    table_diff, object_diff = _diff_tables(
        src_tables,
        tgt_tables,
        src_env,
        src_project,
        tgt_env,
        tgt_project,
        show_all=show_all,
    )

    if not table_diff and not object_diff:
        logger.info("No differences found between the projects.")
        return

    # ── Summarise ─────────────────────────────────────────────────────────
    tbl_src_only = sum(1 for r in table_diff if r["status"] == "source_only")
    tbl_tgt_only = sum(1 for r in table_diff if r["status"] == "target_only")
    tbl_differs = sum(1 for r in table_diff if r["status"] == "differs")
    tbl_match = sum(1 for r in table_diff if r["status"] == "match")
    obj_src_only = sum(1 for r in object_diff if r["status"] == "source_only")
    obj_tgt_only = sum(1 for r in object_diff if r["status"] == "target_only")
    obj_key_diff = sum(1 for r in object_diff if r["status"] == "key_differs")

    logger.info(
        "Tables — source_only: {so}, target_only: {to}, differs: {d}, match: {m}",
        so=tbl_src_only,
        to=tbl_tgt_only,
        d=tbl_differs,
        m=tbl_match,
    )
    logger.info(
        "Objects — source_only: {so}, target_only: {to}, key_differs: {kd}",
        so=obj_src_only,
        to=obj_tgt_only,
        kd=obj_key_diff,
    )

    # ── Write output ──────────────────────────────────────────────────────
    safe_src = src_project.replace(" ", "_").replace("/", "-")
    safe_tgt = tgt_project.replace(" ", "_").replace("/", "-")
    suffix = "all" if show_all else "diff"
    out = _out_dir(src_config, output_dir)
    stem = f"logical_tables_compare_{safe_src}_{src_env}_vs_{safe_tgt}_{tgt_env}_{suffix}"

    _write_output(
        table_diff,
        _CMP_TABLES_CSV_COLS,
        object_diff,
        _CMP_OBJECTS_CSV_COLS,
        fmt,
        stem,
        out,
    )


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export and compare MicroStrategy logical table definitions.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            '  python LogicalTables.py export dev "My Project"\n'
            '  python LogicalTables.py export dev "My Project" --format excel\n'
            '  python LogicalTables.py compare dev "My Project" qa\n'
            '  python LogicalTables.py compare prod "Project A" prod "Project B"\n'
            '  python LogicalTables.py compare dev "My Project" qa --all\n'
        ),
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # ── export ────────────────────────────────────────────────────────────
    exp = sub.add_parser(
        "export",
        help="Document all logical tables in a project.",
    )
    exp.add_argument("env", choices=ENVS, help="Environment (dev, qa, prod).")
    exp.add_argument("project", help="Project name.")
    exp.add_argument(
        "--format",
        choices=["csv", "json", "excel"],
        default="csv",
        help="Output format (default: csv).",
    )
    exp.add_argument("--output-dir", type=Path, default=None, metavar="PATH")

    # ── compare ───────────────────────────────────────────────────────────
    cmp = sub.add_parser(
        "compare",
        help="Compare logical tables between two projects.",
    )
    cmp.add_argument("env", choices=ENVS, help="Source environment.")
    cmp.add_argument("project", help="Source project name.")
    cmp.add_argument("env2", choices=ENVS, help="Target environment.")
    cmp.add_argument(
        "project2",
        nargs="?",
        default=None,
        help="Target project name (default: same as source).",
    )
    cmp.add_argument(
        "--all",
        dest="show_all",
        action="store_true",
        default=False,
        help="Include matching tables/objects (default: differences only).",
    )
    cmp.add_argument(
        "--format",
        choices=["csv", "json", "excel"],
        default="csv",
        help="Output format (default: csv).",
    )
    cmp.add_argument("--output-dir", type=Path, default=None, metavar="PATH")

    # ── Dispatch ──────────────────────────────────────────────────────────
    args = parser.parse_args()

    if args.command == "export":
        export_tables(
            env=args.env,
            project=args.project,
            fmt=args.format,
            output_dir=args.output_dir,
        )
    elif args.command == "compare":
        compare_tables(
            src_env=args.env,
            src_project=args.project,
            tgt_env=args.env2,
            tgt_project=args.project2 or args.project,
            show_all=args.show_all,
            fmt=args.format,
            output_dir=args.output_dir,
        )
