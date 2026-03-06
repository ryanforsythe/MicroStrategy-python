"""
DatabaseInstances.py — Export MicroStrategy database instance definitions to CSV.

Retrieves each database instance from the Intelligence Server, along with the
underlying datasource connection (ODBC data source / connection string) and
the default database login.

Output columns:
    instance_id        – Database instance GUID
    instance_name      – Database instance name
    description        – Database instance description
    dbms_id            – DBMS GUID
    dbms_name          – DBMS display name (e.g. "Microsoft SQL Server 2022")
    database_type      – Database type string on the instance
    database_version   – Database version string on the instance
    datasource_type    – DatasourceType (normal, data_import, etc.)
    connection_id      – Datasource connection GUID
    connection_name    – Datasource connection name
    connection_string  – ODBC data source / connection string
    login_id           – Default database login GUID
    login_name         – Default database login name
    login_username     – Default database login username (the DB user)

Usage:
    python DatabaseInstances.py <env> [--output-dir PATH]

    python DatabaseInstances.py dev
    python DatabaseInstances.py prod --output-dir c:/reports

Notes:
    Database instances are server-level objects; no project selection is needed.
    The datasource_connection and its login are embedded in the instance
    response — no additional API calls are made per instance.
    If datasource_login is returned as a bare ID string rather than a full
    object, login_name and login_username will be empty; only login_id is
    populated.
"""

import argparse
from pathlib import Path

from loguru import logger
from mstrio.datasources import list_datasource_instances

from mstrio_core import MstrConfig, get_mstrio_connection, write_csv
from mstrio_core.config import MstrEnvironment

# ── Constants ─────────────────────────────────────────────────────────────────

ENVS = [e.value for e in MstrEnvironment]
OUTPUT_FILENAME = "database_instances.csv"
COLUMNS = [
    "instance_id",
    "instance_name",
    "description",
    "dbms_id",
    "dbms_name",
    "database_type",
    "database_version",
    "datasource_type",
    "connection_id",
    "connection_name",
    "connection_string",
    "login_id",
    "login_name",
    "login_username",
]

# ── Helpers ───────────────────────────────────────────────────────────────────


def _safe(obj, *attrs, default="") -> str:
    """
    Safely traverse a chain of attribute accesses.

    Returns *default* (empty string) instead of raising AttributeError
    when any intermediate attribute is None or absent.
    """
    for attr in attrs:
        if obj is None:
            return default
        obj = getattr(obj, attr, None)
    return str(obj) if obj is not None else default


def _instance_row(inst) -> list:
    """
    Flatten a DatasourceInstance into a CSV row.

    Handles the case where datasource_login is a full DatasourceLogin
    object (has .id / .name / .username) or a bare ID string.
    """
    dbms = getattr(inst, "dbms", None)
    dc = getattr(inst, "datasource_connection", None)
    dl = getattr(dc, "datasource_login", None) if dc else None

    # datasource_login can be a DatasourceLogin object or a bare ID string.
    if dl is not None and not hasattr(dl, "name"):
        # Bare ID string — populate only login_id
        login_id = str(dl)
        login_name = ""
        login_username = ""
    else:
        login_id = _safe(dl, "id")
        login_name = _safe(dl, "name")
        login_username = _safe(dl, "username")

    return [
        inst.id,
        inst.name,
        getattr(inst, "description", "") or "",
        _safe(dbms, "id"),
        _safe(dbms, "name"),
        str(getattr(inst, "database_type", "") or ""),
        str(getattr(inst, "database_version", "") or ""),
        str(getattr(inst, "datasource_type", "") or ""),
        _safe(dc, "id"),
        _safe(dc, "name"),
        _safe(dc, "connection_string"),
        login_id,
        login_name,
        login_username,
    ]


# ── Main ──────────────────────────────────────────────────────────────────────


def main(env: str, output_dir: Path | None = None) -> None:
    """
    Export all database instance definitions from the given environment to CSV.

    Args:
        env:        Environment to connect to ("dev", "qa", or "prod").
        output_dir: Output directory.  Defaults to MstrConfig.output_dir
                    (MSTR_OUTPUT_DIR env var, c:/tmp).
    """
    config = MstrConfig(environment=MstrEnvironment(env))
    out_path = (output_dir or config.output_dir) / OUTPUT_FILENAME
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    conn = get_mstrio_connection(config=config)
    try:
        logger.info(
            "Fetching database instances from {url} ({env})",
            url=config.base_url,
            env=env,
        )
        instances = list_datasource_instances(conn)
        logger.info("Retrieved {n} database instance(s).", n=len(instances))

        rows: list[list] = []
        for inst in instances:
            try:
                rows.append(_instance_row(inst))
            except Exception as exc:
                logger.warning(
                    "Skipping instance {name!r} ({id}): {exc}",
                    name=getattr(inst, "name", "?"),
                    id=getattr(inst, "id", "?"),
                    exc=exc,
                )

        write_csv(rows, columns=COLUMNS, path=out_path)
        logger.success(
            "Exported {n} database instance(s) → {path}",
            n=len(rows),
            path=out_path,
        )

    finally:
        conn.close()


# ── CLI ───────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export MicroStrategy database instance definitions to CSV."
    )
    parser.add_argument(
        "env",
        choices=ENVS,
        help="Environment to connect to.",
    )
    parser.add_argument(
        "--output-dir",
        metavar="PATH",
        help="Directory for the output CSV (default: MSTR_OUTPUT_DIR or c:/tmp).",
    )
    args = parser.parse_args()
    main(args.env, Path(args.output_dir) if args.output_dir else None)
