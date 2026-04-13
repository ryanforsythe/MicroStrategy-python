"""
ReportVLDBCompare.py — Compare VLDB settings between two MicroStrategy reports.

Compares VLDB property values across two reports on the same or different
environments.  Includes each setting's default status (whether the current
value is the default) and always includes the setting info even when it is
at its default value.

Usage
─────
  python ReportVLDBCompare.py compare <src_env> <src_report_id> <tgt_env> <tgt_report_id>
                                      [--src-project NAME] [--tgt-project NAME]
                                      [--all] [--format csv|json] [--output-dir PATH]

  python ReportVLDBCompare.py export  <env> <report_id>
                                      [--project NAME]
                                      [--format csv|json] [--output-dir PATH]

Examples
────────
  # Compare a report's VLDB settings between dev and prod (differences only)
  python ReportVLDBCompare.py compare dev ABC123 prod ABC123

  # Full comparison including matching settings
  python ReportVLDBCompare.py compare dev ABC123 prod DEF456 --all

  # Compare two reports on the same environment
  python ReportVLDBCompare.py compare dev ABC123 dev DEF456

  # Override project names when reports are in different projects
  python ReportVLDBCompare.py compare dev ABC123 prod DEF456 \\
      --src-project "Finance" --tgt-project "Finance"

  # Export VLDB settings for a single report
  python ReportVLDBCompare.py export dev ABC123
"""

import argparse
import json as _json
from pathlib import Path

from loguru import logger
from mstrio.project_objects.report import Report

from mstrio_core import MstrConfig, MstrRestSession, write_csv
from mstrio_core.config import MstrEnvironment

# ── Constants ─────────────────────────────────────────────────────────────────

ENVS = [e.value for e in MstrEnvironment]

_EXPORT_CSV_COLS = [
    "report_id",
    "report_name",
    "env",
    "property_set",
    "setting_name",
    "value",
    "is_default",
]

_COMPARE_CSV_COLS = [
    "property_set",
    "setting_name",
    "source_value",
    "source_is_default",
    "source_report_id",
    "source_report_name",
    "source_env",
    "target_value",
    "target_is_default",
    "target_report_id",
    "target_report_name",
    "target_env",
    "match",
]


# ── Internal helpers ──────────────────────────────────────────────────────────


def _make_config(env: str) -> MstrConfig:
    """Build a MstrConfig pinned to a specific environment."""
    return MstrConfig(environment=MstrEnvironment(env))


def _out_dir(config: MstrConfig, output_dir: Path | None) -> Path:
    d = output_dir or config.output_dir
    d.mkdir(parents=True, exist_ok=True)
    return d


def _set_project(session, config, project_name=None):
    """Set the active project on the session."""
    if project_name:
        session.set_project(name=project_name)
    elif config.project_name:
        session.set_project(name=config.project_name)
    elif config.project_id:
        session.set_project(project_id=config.project_id)
    else:
        raise ValueError(
            "No project specified. Use --project or set "
            "MSTR_PROJECT_NAME / MSTR_PROJECT_ID."
        )


def _fetch_vldb_settings(session, report_id, env_label):
    """
    Fetch VLDB settings for a report via mstrio-py.

    Returns:
        (report_name, settings_flat) where settings_flat is a list of dicts:
        [{"property_set": ..., "setting_name": ..., "value": ..., "is_default": ...}, ...]
    """
    conn = session.mstrio_conn
    rpt = Report(connection=conn, id=report_id)
    report_name = rpt.name or report_id

    logger.info(
        "Fetching VLDB settings for '{name}' ({id}) on {env}",
        name=report_name, id=report_id, env=env_label,
    )

    vldb = rpt.vldb_settings
    if vldb is None:
        logger.warning(
            "No VLDB settings returned for '{name}' ({id})",
            name=report_name, id=report_id,
        )
        return report_name, []

    settings = _flatten_vldb(vldb)
    logger.info(
        "Report '{name}': {n} VLDB setting(s) retrieved",
        name=report_name, n=len(settings),
    )
    return report_name, settings


def _flatten_vldb(vldb) -> list[dict]:
    """
    Flatten the VLDB settings structure into a list of dicts.

    The vldb_settings property returns a dict structured as:
        { "property_set_name": { "setting_name": { "value": ..., ... }, ... }, ... }
    or sometimes a list of property-set objects.  This function normalises
    both shapes into a flat list.
    """
    flat: list[dict] = []

    if isinstance(vldb, dict):
        for ps_name, ps_value in sorted(vldb.items()):
            if isinstance(ps_value, dict):
                # Could be a single setting or a group of settings
                if "value" in ps_value:
                    # Single setting at this level
                    flat.append(_extract_setting(ps_name, ps_name, ps_value))
                else:
                    # Group of settings
                    for s_name, s_value in sorted(ps_value.items()):
                        if isinstance(s_value, dict):
                            flat.append(_extract_setting(ps_name, s_name, s_value))
                        else:
                            flat.append({
                                "property_set": ps_name,
                                "setting_name": s_name,
                                "value": str(s_value),
                                "is_default": "",
                            })
            elif isinstance(ps_value, list):
                for item in ps_value:
                    if isinstance(item, dict):
                        s_name = item.get("name", item.get("propertyName", ""))
                        flat.append(_extract_setting(ps_name, s_name, item))
            else:
                flat.append({
                    "property_set": "",
                    "setting_name": ps_name,
                    "value": str(ps_value),
                    "is_default": "",
                })

    elif isinstance(vldb, list):
        for item in vldb:
            if isinstance(item, dict):
                ps_name = item.get("name", item.get("propertySetName", ""))
                properties = item.get("properties", item.get("settings", []))
                if isinstance(properties, dict):
                    for s_name, s_value in sorted(properties.items()):
                        if isinstance(s_value, dict):
                            flat.append(_extract_setting(ps_name, s_name, s_value))
                        else:
                            flat.append({
                                "property_set": ps_name,
                                "setting_name": s_name,
                                "value": str(s_value),
                                "is_default": "",
                            })
                elif isinstance(properties, list):
                    for prop in properties:
                        if isinstance(prop, dict):
                            s_name = prop.get("name", prop.get("propertyName", ""))
                            flat.append(_extract_setting(ps_name, s_name, prop))

    return flat


def _extract_setting(property_set, setting_name, data):
    """Extract value and default status from a setting dict."""
    value = data.get("value", data.get("resolvedLocation", ""))
    is_default = data.get("isDefault", data.get("is_default", ""))

    # Normalise value to string for consistent comparison
    if isinstance(value, (dict, list)):
        value = _json.dumps(value, sort_keys=True)
    else:
        value = str(value)

    if isinstance(is_default, bool):
        is_default = str(is_default)
    elif is_default == "":
        # Try to infer from resolvedLocation or type field
        resolved = data.get("resolvedLocation", "")
        if resolved == "default":
            is_default = "True"

    return {
        "property_set": property_set,
        "setting_name": setting_name,
        "value": value,
        "is_default": is_default,
    }


def _settings_to_dict(settings_flat):
    """
    Convert a flat settings list to a lookup dict keyed by
    (property_set, setting_name).
    """
    return {
        (s["property_set"], s["setting_name"]): s
        for s in settings_flat
    }


def _merge_setting_keys(src_settings, tgt_settings):
    """
    Return a sorted superset of all (property_set, setting_name) keys
    from both setting lists.
    """
    src_keys = {(s["property_set"], s["setting_name"]) for s in src_settings}
    tgt_keys = {(s["property_set"], s["setting_name"]) for s in tgt_settings}
    return sorted(src_keys | tgt_keys)


def _build_compare_rows(
    src_report_id, src_report_name, src_env,
    tgt_report_id, tgt_report_name, tgt_env,
    src_settings, tgt_settings,
    show_all,
):
    """
    Build comparison rows between source and target VLDB settings.

    Returns a list of dicts matching _COMPARE_CSV_COLS.
    """
    src_map = _settings_to_dict(src_settings)
    tgt_map = _settings_to_dict(tgt_settings)
    all_keys = _merge_setting_keys(src_settings, tgt_settings)

    rows = []
    for key in all_keys:
        ps, sn = key
        src = src_map.get(key, {})
        tgt = tgt_map.get(key, {})

        src_val = src.get("value", "(not present)")
        tgt_val = tgt.get("value", "(not present)")
        src_def = src.get("is_default", "")
        tgt_def = tgt.get("is_default", "")

        match = src_val == tgt_val

        if not show_all and match:
            continue

        rows.append({
            "property_set": ps,
            "setting_name": sn,
            "source_value": src_val,
            "source_is_default": src_def,
            "source_report_id": src_report_id,
            "source_report_name": src_report_name,
            "source_env": src_env,
            "target_value": tgt_val,
            "target_is_default": tgt_def,
            "target_report_id": tgt_report_id,
            "target_report_name": tgt_report_name,
            "target_env": tgt_env,
            "match": match,
        })

    # Sort: differences first, then by property set and setting name
    rows.sort(key=lambda r: (r["match"], r["property_set"].lower(), r["setting_name"].lower()))
    return rows


def _dicts_to_rows(dicts, columns):
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
    env,
    report_id,
    project=None,
    fmt="csv",
    output_dir=None,
):
    """Export VLDB settings for a single report."""
    config = _make_config(env)
    out = _out_dir(config, output_dir)

    with MstrRestSession(config) as session:
        _set_project(session, config, project)
        report_name, settings = _fetch_vldb_settings(session, report_id, env)

    if not settings:
        logger.warning("No VLDB settings to export.")
        return

    safe_name = report_name.replace(" ", "_").replace("/", "-")[:50]
    stem = f"vldb_settings_{safe_name}_{env}"

    if fmt == "csv":
        rows = []
        for s in settings:
            rows.append([
                report_id, report_name, env,
                s["property_set"], s["setting_name"],
                s["value"], s["is_default"],
            ])
        write_csv(rows, columns=_EXPORT_CSV_COLS, path=out / f"{stem}.csv")
    else:
        data = [{
            "report_id": report_id,
            "report_name": report_name,
            "env": env,
            **s,
        } for s in settings]
        _write_json(data, out / f"{stem}.json")

    logger.success(
        "Exported {n} VLDB setting(s) for '{name}' ({env})",
        n=len(settings), name=report_name, env=env,
    )


# ── Subcommand: compare ─────────────────────────────────────────────────────


def cmd_compare(
    src_env,
    src_report_id,
    tgt_env,
    tgt_report_id,
    src_project=None,
    tgt_project=None,
    show_all=False,
    fmt="csv",
    output_dir=None,
):
    """Compare VLDB settings between two reports (same or different environments)."""
    same_env = src_env == tgt_env

    # ── Source ────────────────────────────────────────────────────────────
    src_config = _make_config(src_env)
    out = _out_dir(src_config, output_dir)

    with MstrRestSession(src_config) as src_session:
        _set_project(src_session, src_config, src_project)
        src_name, src_settings = _fetch_vldb_settings(
            src_session, src_report_id, src_env,
        )

        # If same environment, fetch target on the same session
        if same_env:
            tgt_name, tgt_settings = _fetch_vldb_settings(
                src_session, tgt_report_id, tgt_env,
            )

    # ── Target (cross-environment only) ──────────────────────────────────
    if not same_env:
        tgt_config = _make_config(tgt_env)
        with MstrRestSession(tgt_config) as tgt_session:
            _set_project(tgt_session, tgt_config, tgt_project or src_project)
            tgt_name, tgt_settings = _fetch_vldb_settings(
                tgt_session, tgt_report_id, tgt_env,
            )

    # ── Compare ──────────────────────────────────────────────────────────
    rows = _build_compare_rows(
        src_report_id, src_name, src_env,
        tgt_report_id, tgt_name, tgt_env,
        src_settings, tgt_settings,
        show_all,
    )

    # ── Summary ──────────────────────────────────────────────────────────
    all_keys = _merge_setting_keys(src_settings, tgt_settings)
    n_diff = sum(1 for r in rows if not r["match"])
    n_match = len(all_keys) - n_diff

    logger.info(
        "Source: '{src}' ({src_env}) — {sn} setting(s)",
        src=src_name, src_env=src_env, sn=len(src_settings),
    )
    logger.info(
        "Target: '{tgt}' ({tgt_env}) — {tn} setting(s)",
        tgt=tgt_name, tgt_env=tgt_env, tn=len(tgt_settings),
    )
    logger.info(
        "{diff} difference(s), {match} match(es) across {total} setting(s)",
        diff=n_diff, match=n_match, total=len(all_keys),
    )

    if not rows:
        logger.info("No differences found — VLDB settings are identical.")
        return

    # ── Write output ─────────────────────────────────────────────────────
    suffix = "all" if show_all else "diff"
    src_safe = src_name.replace(" ", "_").replace("/", "-")[:30]
    tgt_safe = tgt_name.replace(" ", "_").replace("/", "-")[:30]
    stem = f"vldb_compare_{src_safe}_{src_env}_vs_{tgt_safe}_{tgt_env}_{suffix}"

    if fmt == "csv":
        write_csv(
            _dicts_to_rows(rows, _COMPARE_CSV_COLS),
            columns=_COMPARE_CSV_COLS,
            path=out / f"{stem}.csv",
        )
    else:
        _write_json(rows, out / f"{stem}.json")

    logger.success(
        "Comparison complete: {n} row(s) written.",
        n=len(rows),
    )


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
        description="Compare VLDB settings between MicroStrategy reports.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # ── compare ──────────────────────────────────────────────────────────
    p_cmp = subparsers.add_parser(
        "compare",
        help="Compare VLDB settings between two reports.",
    )
    p_cmp.add_argument("src_env", choices=ENVS, help="Source environment.")
    p_cmp.add_argument("src_report_id", help="Source report GUID.")
    p_cmp.add_argument("tgt_env", choices=ENVS, help="Target environment.")
    p_cmp.add_argument("tgt_report_id", help="Target report GUID.")
    p_cmp.add_argument(
        "--src-project",
        default=None,
        metavar="NAME",
        help="Source project name (overrides MSTR_PROJECT_NAME).",
    )
    p_cmp.add_argument(
        "--tgt-project",
        default=None,
        metavar="NAME",
        help="Target project name (overrides MSTR_PROJECT_NAME; "
             "defaults to --src-project if omitted).",
    )
    p_cmp.add_argument(
        "--all",
        dest="show_all",
        action="store_true",
        default=False,
        help="Show all settings (default: differences only).",
    )
    _add_output_args(p_cmp)

    # ── export ───────────────────────────────────────────────────────────
    p_exp = subparsers.add_parser(
        "export",
        help="Export VLDB settings for a single report.",
    )
    p_exp.add_argument("env", choices=ENVS, help="Environment.")
    p_exp.add_argument("report_id", help="Report GUID.")
    p_exp.add_argument(
        "--project",
        default=None,
        metavar="NAME",
        help="Project name (overrides MSTR_PROJECT_NAME).",
    )
    _add_output_args(p_exp)

    # ── Dispatch ─────────────────────────────────────────────────────────
    args = parser.parse_args()

    if args.subcommand == "compare":
        cmd_compare(
            src_env=args.src_env,
            src_report_id=args.src_report_id,
            tgt_env=args.tgt_env,
            tgt_report_id=args.tgt_report_id,
            src_project=args.src_project,
            tgt_project=args.tgt_project,
            show_all=args.show_all,
            fmt=args.fmt,
            output_dir=args.output_dir,
        )
    elif args.subcommand == "export":
        cmd_export(
            env=args.env,
            report_id=args.report_id,
            project=args.project,
            fmt=args.fmt,
            output_dir=args.output_dir,
        )
