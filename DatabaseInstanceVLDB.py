"""
DatabaseInstanceVLDB.py — Export and modify VLDB settings on MicroStrategy
database instances.

Subcommands
───────────
  export  — Document VLDB settings for a single or all database instances.
             By default shows only non-default settings; pass --all to include
             every setting.

  alter   — Change a VLDB setting on one or more database instances.
             Accepts a single instance (--instance) or a list from CSV
             (--csv with an instance_id column).  Dry-run by default; --apply
             to execute.

Usage
─────
  python DatabaseInstanceVLDB.py export <env>
      [--instance NAME_OR_ID] [--all] [--include-all-types]
      [--format csv|json] [--output-dir PATH]

  python DatabaseInstanceVLDB.py alter <env>
      --setting <name> --value <value>
      [--instance NAME_OR_ID | --csv PATH]
      [--apply] [--format csv|json] [--output-dir PATH]

Examples
────────
  # Export non-default VLDB settings for all database instances on dev
  python DatabaseInstanceVLDB.py export dev

  # Export ALL VLDB settings for all instances on dev
  python DatabaseInstanceVLDB.py export dev --all

  # Export VLDB settings for a specific instance by name
  python DatabaseInstanceVLDB.py export dev --instance "Warehouse"

  # Export VLDB settings for a specific instance by GUID
  python DatabaseInstanceVLDB.py export prod --instance ABC123DEF456

  # Dry-run: preview changing a VLDB setting on one instance
  python DatabaseInstanceVLDB.py alter dev --instance "Warehouse" \\
      --setting "Intermediate Table Prefix" --value "ZZQL"

  # Apply the change
  python DatabaseInstanceVLDB.py alter dev --instance "Warehouse" \\
      --setting "Intermediate Table Prefix" --value "ZZQL" --apply

  # Change a VLDB setting on multiple instances from a CSV
  python DatabaseInstanceVLDB.py alter prod \\
      --csv instances.csv --setting "Query Timeout" --value 600 --apply
"""

import argparse
import csv
import json as _json
from pathlib import Path

from loguru import logger
from mstrio.datasources import DatasourceInstance, list_datasource_instances

from mstrio_core import MstrConfig, get_mstrio_connection, write_csv
from mstrio_core.config import MstrEnvironment

# ── Constants ─────────────────────────────────────────────────────────────────

ENVS = [e.value for e in MstrEnvironment]

# database_type values that represent non-database connectors (cloud services,
# social media, big data engines, etc.).  Excluded by default; pass
# --include-all-types to include them.
_EXCLUDED_DATABASE_TYPES: set[str] = {
    "big_data_engine",
    "cloud_element",
    "dropbox",
    "facebook",
    "generic_data_connector",
    "google_analytics",
    "google_big_query",
    "google_drive",
    "salesforce",
    "spark_config",
    "twitter",
    "url_auth",
}

_EXPORT_CSV_COLS = [
    "instance_id",
    "instance_name",
    "property_set",
    "group_name",
    "setting_name",
    "display_name",
    "value",
    "default_value",
    "is_default",
    "resolved_location",
    "is_inherited",
]

_ALTER_CSV_COLS = [
    "instance_id",
    "instance_name",
    "setting_name",
    "old_value",
    "new_value",
    "is_default_before",
    "status",
]


# ── Internal helpers ──────────────────────────────────────────────────────────


def _make_config(env: str) -> MstrConfig:
    """Build a MstrConfig pinned to a specific environment."""
    return MstrConfig(environment=MstrEnvironment(env))


def _out_dir(config: MstrConfig, output_dir: Path | None) -> Path:
    d = output_dir or config.output_dir
    d.mkdir(parents=True, exist_ok=True)
    return d


def _filter_database_types(instances, include_all_types: bool) -> list:
    """
    Filter out non-database connector types unless include_all_types is True.

    Returns a (possibly shorter) list of instances.
    """
    if include_all_types:
        return instances

    filtered = []
    skipped = 0
    for inst in instances:
        db_type = str(getattr(inst, "database_type", "") or "").lower()
        if db_type in _EXCLUDED_DATABASE_TYPES:
            skipped += 1
            logger.debug(
                "Excluding non-database type: {name} ({id}, type={db_type})",
                name=getattr(inst, "name", "?"),
                id=getattr(inst, "id", "?"),
                db_type=db_type,
            )
            continue
        filtered.append(inst)

    if skipped:
        logger.info(
            "Excluded {n} non-database connector instance(s). "
            "Use --include-all-types to include them.",
            n=skipped,
        )
    return filtered


def _resolve_instance(conn, name_or_id: str) -> DatasourceInstance:
    """
    Resolve a database instance by name or GUID.

    Tries ID first (32-char hex), then falls back to name.
    """
    if len(name_or_id) == 32 and name_or_id.isalnum():
        try:
            inst = DatasourceInstance(connection=conn, id=name_or_id)
            logger.debug(
                "Resolved instance by ID: {name} ({id})",
                name=inst.name, id=inst.id,
            )
            return inst
        except Exception:
            logger.debug("ID lookup failed for {val}, trying as name.", val=name_or_id)

    try:
        inst = DatasourceInstance(connection=conn, name=name_or_id)
        logger.debug(
            "Resolved instance by name: {name} ({id})",
            name=inst.name, id=inst.id,
        )
        return inst
    except Exception:
        pass

    # Fall back to listing all and matching
    all_instances = list_datasource_instances(conn)
    for inst in all_instances:
        if inst.name == name_or_id or inst.id == name_or_id:
            return inst

    available = [f"{i.name} ({i.id})" for i in all_instances]
    raise ValueError(
        f"Database instance not found: {name_or_id!r}. "
        f"Available: {available}"
    )


def _flatten_vldb(instance: DatasourceInstance, show_all: bool) -> list[dict]:
    """
    Flatten VLDB settings for a database instance into a list of dicts.

    Args:
        instance:  DatasourceInstance with VLDB settings loaded.
        show_all:  True → all settings; False → non-default only.

    Returns:
        List of dicts matching _EXPORT_CSV_COLS field names.
    """
    settings = instance.vldb_settings
    if not settings:
        logger.warning(
            "No VLDB settings for '{name}' ({id})",
            name=instance.name, id=instance.id,
        )
        return []

    rows = []
    for key, s in sorted(settings.items(), key=lambda kv: (kv[1].property_set, kv[1].name)):
        is_default = str(s.value) == str(s.default_value)

        if not show_all and is_default:
            continue

        rows.append({
            "instance_id": instance.id,
            "instance_name": instance.name,
            "property_set": s.property_set,
            "group_name": s.group_name,
            "setting_name": s.name,
            "display_name": s.display_name,
            "value": str(s.value),
            "default_value": str(s.default_value),
            "is_default": is_default,
            "resolved_location": str(s.resolved_location.value) if s.resolved_location else "",
            "is_inherited": s.is_inherited,
        })

    return rows


def _read_instance_ids_from_csv(csv_path: str) -> list[str]:
    """
    Read instance IDs from a CSV file with an 'instance_id' column.

    Supports both comma and semicolon delimiters.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    text = path.read_text(encoding="utf-8")
    delimiter = ";" if ";" in text.splitlines()[0] else ","
    reader = csv.DictReader(text.splitlines(), delimiter=delimiter)

    if "instance_id" not in (reader.fieldnames or []):
        raise ValueError(
            f"CSV must contain an 'instance_id' column. "
            f"Found columns: {reader.fieldnames}"
        )

    ids = []
    for row in reader:
        val = row["instance_id"].strip().strip('"').strip("'")
        if val:
            ids.append(val)

    logger.info("Read {n} instance ID(s) from {path}", n=len(ids), path=path)
    return ids


def _dicts_to_rows(dicts: list[dict], columns: list[str]) -> list[list]:
    """Convert a list of dicts to a list of lists ordered by columns."""
    return [[d.get(c, "") for c in columns] for d in dicts]


def _write_json(data, path):
    """Write data as formatted JSON."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json.dumps(data, indent=2, default=str), encoding="utf-8")
    logger.success("JSON written: {path} ({n} items)", path=path, n=len(data))


# ── Subcommand: export ───────────────────────────────────────────────────────


def cmd_export(
    env: str,
    instance_name: str | None = None,
    show_all: bool = False,
    include_all_types: bool = False,
    fmt: str = "csv",
    output_dir: Path | None = None,
) -> None:
    """
    Export VLDB settings for database instances.

    Args:
        env:                Environment to connect to.
        instance_name:      Name or GUID of a specific instance.
                            None → export all instances.
        show_all:           True → include settings at default values;
                            False → non-default only.
        include_all_types:  When False, non-database connector types are
                            excluded from the "all instances" list.
        fmt:                Output format: "csv" or "json".
        output_dir:         Output directory.
    """
    config = _make_config(env)
    out = _out_dir(config, output_dir)

    conn = get_mstrio_connection(config=config)
    try:
        if instance_name:
            inst = _resolve_instance(conn, instance_name)
            instances = [inst]
            logger.info(
                "Exporting VLDB settings for '{name}' ({id}) on {env}",
                name=inst.name, id=inst.id, env=env,
            )
        else:
            instances = list_datasource_instances(conn)
            logger.info(
                "Retrieved {n} total database instance(s).",
                n=len(instances),
            )
            instances = _filter_database_types(instances, include_all_types)
            logger.info(
                "Exporting VLDB settings for {n} database instance(s) on {env}",
                n=len(instances), env=env,
            )

        all_rows: list[dict] = []
        for inst in instances:
            try:
                rows = _flatten_vldb(inst, show_all)
                all_rows.extend(rows)
                n_non_default = sum(1 for r in rows if not r["is_default"])
                logger.info(
                    "  {name}: {total} setting(s) ({nd} non-default)",
                    name=inst.name,
                    total=len(rows),
                    nd=n_non_default,
                )
            except Exception as exc:
                logger.warning(
                    "Skipping '{name}' ({id}): {exc}",
                    name=getattr(inst, "name", "?"),
                    id=getattr(inst, "id", "?"),
                    exc=exc,
                )

        if not all_rows:
            mode_label = "all" if show_all else "non-default"
            logger.info("No {mode} VLDB settings found.", mode=mode_label)
            return

        # Build filename
        if instance_name and len(instances) == 1:
            safe = instances[0].name.replace(" ", "_").replace("/", "-")[:50]
            suffix = "all" if show_all else "nondefault"
            stem = f"vldb_dbinstance_{safe}_{env}_{suffix}"
        else:
            suffix = "all" if show_all else "nondefault"
            stem = f"vldb_dbinstances_{env}_{suffix}"

        if fmt == "csv":
            write_csv(
                _dicts_to_rows(all_rows, _EXPORT_CSV_COLS),
                columns=_EXPORT_CSV_COLS,
                path=out / f"{stem}.csv",
            )
        else:
            _write_json(all_rows, out / f"{stem}.json")

        logger.success(
            "Exported {n} VLDB setting(s) across {inst} instance(s).",
            n=len(all_rows), inst=len(instances),
        )

    finally:
        conn.close()


# ── Subcommand: alter ────────────────────────────────────────────────────────


def cmd_alter(
    env: str,
    setting_name: str,
    setting_value: str,
    instance_name: str | None = None,
    csv_path: str | None = None,
    apply: bool = False,
    fmt: str = "csv",
    output_dir: Path | None = None,
) -> None:
    """
    Change a VLDB setting on one or more database instances.

    Args:
        env:            Environment to connect to.
        setting_name:   VLDB setting name or display name.
        setting_value:  New value to set.
        instance_name:  Name or GUID of a single instance.
        csv_path:       Path to CSV with 'instance_id' column.
        apply:          If False, dry-run only (no changes).
        fmt:            Output format for the results report.
        output_dir:     Output directory.
    """
    if not instance_name and not csv_path:
        raise ValueError("Provide --instance or --csv to specify target instance(s).")

    config = _make_config(env)
    out = _out_dir(config, output_dir)
    mode = "APPLY" if apply else "DRY-RUN"
    logger.info("Mode: {mode}", mode=mode)

    conn = get_mstrio_connection(config=config)
    try:
        # Resolve target instances
        if csv_path:
            instance_ids = _read_instance_ids_from_csv(csv_path)
            instances = []
            for iid in instance_ids:
                try:
                    instances.append(_resolve_instance(conn, iid))
                except Exception as exc:
                    logger.warning("Could not resolve instance {id}: {exc}", id=iid, exc=exc)
        else:
            inst = _resolve_instance(conn, instance_name)
            instances = [inst]

        if not instances:
            logger.warning("No instances resolved. Nothing to do.")
            return

        logger.info(
            "Setting '{setting}' = '{value}' on {n} instance(s)",
            setting=setting_name, value=setting_value, n=len(instances),
        )

        results: list[dict] = []

        for inst in instances:
            try:
                # Read current value
                vldb = inst.vldb_settings
                current = _find_setting(vldb, setting_name)

                if current is None:
                    logger.warning(
                        "Setting '{setting}' not found on '{name}' ({id})",
                        setting=setting_name, name=inst.name, id=inst.id,
                    )
                    results.append({
                        "instance_id": inst.id,
                        "instance_name": inst.name,
                        "setting_name": setting_name,
                        "old_value": "",
                        "new_value": setting_value,
                        "is_default_before": "",
                        "status": "NOT FOUND",
                    })
                    continue

                old_value = str(current.value)
                is_default_before = str(old_value) == str(current.default_value)

                logger.info(
                    "  {name}: '{setting}' current={old} (default={dflt}, "
                    "is_default={is_dflt})",
                    name=inst.name,
                    setting=current.display_name or current.name,
                    old=old_value,
                    dflt=current.default_value,
                    is_dflt=is_default_before,
                )

                if apply:
                    # Convert value to the appropriate type
                    typed_value = _convert_value(setting_value, current)
                    inst.alter_vldb_settings({setting_name: typed_value})
                    logger.success(
                        "  {name}: '{setting}' changed {old} → {new}",
                        name=inst.name,
                        setting=current.display_name or current.name,
                        old=old_value,
                        new=setting_value,
                    )
                    status = "CHANGED"
                else:
                    status = "DRY-RUN"

                results.append({
                    "instance_id": inst.id,
                    "instance_name": inst.name,
                    "setting_name": current.display_name or current.name,
                    "old_value": old_value,
                    "new_value": setting_value,
                    "is_default_before": is_default_before,
                    "status": status,
                })

            except Exception as exc:
                logger.error(
                    "Failed on '{name}' ({id}): {exc}",
                    name=inst.name, id=inst.id, exc=exc,
                )
                results.append({
                    "instance_id": inst.id,
                    "instance_name": inst.name,
                    "setting_name": setting_name,
                    "old_value": "",
                    "new_value": setting_value,
                    "is_default_before": "",
                    "status": f"FAILED ({exc})",
                })

        # Write results
        if results:
            stem = f"vldb_alter_{env}"
            if fmt == "csv":
                write_csv(
                    _dicts_to_rows(results, _ALTER_CSV_COLS),
                    columns=_ALTER_CSV_COLS,
                    path=out / f"{stem}.csv",
                )
            else:
                _write_json(results, out / f"{stem}.json")

        # Summary
        changed = sum(1 for r in results if r["status"] == "CHANGED")
        dryrun = sum(1 for r in results if r["status"] == "DRY-RUN")
        failed = sum(1 for r in results if r["status"].startswith("FAILED"))
        not_found = sum(1 for r in results if r["status"] == "NOT FOUND")
        logger.info(
            "Results: {changed} changed, {dryrun} dry-run, "
            "{failed} failed, {notfound} not found",
            changed=changed, dryrun=dryrun,
            failed=failed, notfound=not_found,
        )

    finally:
        conn.close()


def _find_setting(vldb_settings, name: str):
    """
    Look up a VLDB setting by key name or display name.

    Returns the VldbSetting object, or None if not found.
    """
    if not vldb_settings:
        return None

    # Try direct key lookup first
    if name in vldb_settings:
        return vldb_settings[name]

    # Try case-insensitive key match
    name_lower = name.lower()
    for key, setting in vldb_settings.items():
        if key.lower() == name_lower:
            return setting

    # Try display name match
    for key, setting in vldb_settings.items():
        if (setting.display_name or "").lower() == name_lower:
            return setting

    return None


def _convert_value(value_str: str, setting):
    """
    Convert a string value to the appropriate type based on the existing
    setting's type.
    """
    current_type = type(setting.default_value)

    if current_type == bool:
        return value_str.lower() in ("true", "1", "yes")
    elif current_type == int:
        return int(value_str)
    elif current_type == float:
        return float(value_str)
    else:
        return value_str


# ── CLI ───────────────────────────────────────────────────────────────────────


def _add_output_args(sub, default_fmt="csv"):
    """Add --format and --output-dir arguments."""
    sub.add_argument(
        "--format",
        dest="fmt",
        choices=["csv", "json"],
        default=default_fmt,
        metavar="csv|json",
        help=f"Output format (default: {default_fmt}).",
    )
    sub.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        metavar="PATH",
        help="Output directory (default: MSTR_OUTPUT_DIR or c:/tmp).",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export and modify VLDB settings on MicroStrategy database instances.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # ── export ───────────────────────────────────────────────────────────
    p_exp = subparsers.add_parser(
        "export",
        help="Export VLDB settings for one or all database instances.",
    )
    p_exp.add_argument("env", choices=ENVS, help="Environment.")
    p_exp.add_argument(
        "--instance",
        dest="instance_name",
        default=None,
        metavar="NAME_OR_ID",
        help="Database instance name or GUID (default: all instances).",
    )
    p_exp.add_argument(
        "--all",
        dest="show_all",
        action="store_true",
        default=False,
        help="Include settings at default values (default: non-default only).",
    )
    p_exp.add_argument(
        "--include-all-types",
        action="store_true",
        default=False,
        help=(
            "Include all datasource types. By default, non-database connectors "
            "(cloud, social media, big data engines) are excluded."
        ),
    )
    _add_output_args(p_exp)

    # ── alter ────────────────────────────────────────────────────────────
    p_alt = subparsers.add_parser(
        "alter",
        help="Change a VLDB setting on one or more database instances.",
    )
    p_alt.add_argument("env", choices=ENVS, help="Environment.")
    p_alt.add_argument(
        "--setting",
        required=True,
        dest="setting_name",
        metavar="NAME",
        help="VLDB setting name or display name.",
    )
    p_alt.add_argument(
        "--value",
        required=True,
        dest="setting_value",
        metavar="VALUE",
        help="New value to set.",
    )

    target_group = p_alt.add_mutually_exclusive_group(required=True)
    target_group.add_argument(
        "--instance",
        dest="instance_name",
        default=None,
        metavar="NAME_OR_ID",
        help="Single database instance name or GUID.",
    )
    target_group.add_argument(
        "--csv",
        dest="csv_path",
        default=None,
        metavar="PATH",
        help="CSV file with an 'instance_id' column.",
    )

    p_alt.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="Execute changes. Without this flag, runs in dry-run mode.",
    )
    _add_output_args(p_alt)

    # ── Dispatch ─────────────────────────────────────────────────────────
    args = parser.parse_args()

    if args.subcommand == "export":
        cmd_export(
            env=args.env,
            instance_name=args.instance_name,
            show_all=args.show_all,
            include_all_types=args.include_all_types,
            fmt=args.fmt,
            output_dir=args.output_dir,
        )
    elif args.subcommand == "alter":
        cmd_alter(
            env=args.env,
            setting_name=args.setting_name,
            setting_value=args.setting_value,
            instance_name=args.instance_name,
            csv_path=args.csv_path,
            apply=args.apply,
            fmt=args.fmt,
            output_dir=args.output_dir,
        )
