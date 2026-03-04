"""
CompareServerSettings.py — Compare, export, or apply MicroStrategy I-Server
settings across environments (dev, qa, prod).

Usage:
    python CompareServerSettings.py compare <source> <target>  [--format csv|json] [--all]
    python CompareServerSettings.py export  <env>              [--format csv|json] [--description]
    python CompareServerSettings.py apply   <source> <target>

Examples:
    python CompareServerSettings.py compare dev prod
    python CompareServerSettings.py compare dev prod --format json --all
    python CompareServerSettings.py export  qa  --format json --description
    python CompareServerSettings.py apply   dev prod

Operations
──────────
  compare  — Diff source and target settings.  By default only differing rows
             are written.  Pass --all to include identical rows too.

  export   — Fetch settings from one environment and write CSV or JSON.

  apply    — Copy source settings to the target server.  Snapshots the target
             before writing for an audit trail.  Prompts for confirmation.

Credentials
────────────
  Each environment reads from prefixed env vars, e.g.:
      MSTR_DEV_BASE_URL   / MSTR_DEV_USERNAME  / MSTR_DEV_PASSWORD
      MSTR_PROD_BASE_URL  / MSTR_PROD_USERNAME / MSTR_PROD_PASSWORD
  Bare MSTR_BASE_URL / MSTR_USERNAME / MSTR_PASSWORD act as fallback.
  See CLAUDE.md §Multi-Environment Pattern for full details.
"""

import argparse
import os
import tempfile
from pathlib import Path

from loguru import logger
from mstrio.server.server import ServerSettings

from mstrio_core import MstrConfig, get_mstrio_connection
from mstrio_core.config import MstrEnvironment

# ── Constants ─────────────────────────────────────────────────────────────────

ENVS = [e.value for e in MstrEnvironment]

# ── Internal helpers ───────────────────────────────────────────────────────────


def _make_config(env: str) -> MstrConfig:
    """Build a MstrConfig pinned to a specific environment."""
    return MstrConfig(environment=MstrEnvironment(env))


def _out_dir(config: MstrConfig, output_dir: Path | None) -> Path:
    d = output_dir or config.output_dir
    d.mkdir(parents=True, exist_ok=True)
    return d


def _temp_csv() -> str:
    """
    Return a path for a temporary CSV file.  Uses mkstemp so the fd is
    immediately closed — avoids Windows file-lock issues when mstrio-py
    opens the same path for writing.
    """
    fd, path = tempfile.mkstemp(suffix=".csv", prefix="mstr_settings_")
    os.close(fd)
    return path


# ── Operations ────────────────────────────────────────────────────────────────


def export_settings(
    env: str,
    fmt: str = "csv",
    show_description: bool = False,
    output_dir: Path | None = None,
) -> None:
    """
    Fetch and export I-Server settings for one environment.

    Args:
        env:              Environment to export ("dev", "qa", or "prod").
        fmt:              Output format: "csv" or "json".
        show_description: Include human-readable setting descriptions (CSV only).
        output_dir:       Output directory.  Defaults to MstrConfig.output_dir.
    """
    config = _make_config(env)
    conn = get_mstrio_connection(config=config)
    try:
        logger.info("Fetching {env} server settings ...", env=env)
        settings = ServerSettings(conn)

        out = _out_dir(config, output_dir)
        path = out / f"server_settings_{env}.{fmt}"

        if fmt == "csv":
            settings.to_csv(str(path), show_description=show_description)
        elif fmt == "json":
            settings.to_json(str(path))
        else:
            raise ValueError(f"Unsupported format {fmt!r}. Use 'csv' or 'json'.")

        logger.success("Exported {env} settings → {path}", env=env, path=path)
    finally:
        conn.close()


def compare_settings(
    source_env: str,
    target_env: str,
    fmt: str = "csv",
    show_diff_only: bool = True,
    output_dir: Path | None = None,
) -> None:
    """
    Diff source vs target server settings and write the result to file.

    Args:
        source_env:    Environment to treat as the reference / source of truth.
        target_env:    Environment to compare against the source.
        fmt:           Output format: "csv" or "json".
        show_diff_only: True → only rows that differ; False → all rows.
        output_dir:    Output directory.  Defaults to MstrConfig.output_dir.

    Strategy
    ─────────
    1. Connect to source → export to a temp CSV.
    2. Connect to target → fetch ServerSettings.
    3. target.compare_with_files(source_temp) returns a DataFrame.
    4. Write the DataFrame to the output file.
    """
    out = _out_dir(_make_config(source_env), output_dir)
    tmp_path = _temp_csv()

    # ── 1. Fetch source and export to temp ────────────────────────────────────
    src_conn = get_mstrio_connection(config=_make_config(source_env))
    try:
        logger.info("Fetching {env} (source) settings ...", env=source_env)
        src_settings = ServerSettings(src_conn)
        src_settings.to_csv(tmp_path)
        logger.debug("Source settings written to temp file: {p}", p=tmp_path)
    finally:
        src_conn.close()

    # ── 2. Fetch target and compare ───────────────────────────────────────────
    tgt_conn = get_mstrio_connection(config=_make_config(target_env))
    try:
        logger.info("Fetching {env} (target) settings ...", env=target_env)
        tgt_settings = ServerSettings(tgt_conn)
        diff_df = tgt_settings.compare_with_files(
            tmp_path, show_diff_only=show_diff_only
        )
    finally:
        tgt_conn.close()

    Path(tmp_path).unlink(missing_ok=True)

    # ── 3. Write diff ─────────────────────────────────────────────────────────
    if diff_df is None or diff_df.empty:
        logger.info(
            "No differences found between {src} and {tgt}.",
            src=source_env,
            tgt=target_env,
        )
        return

    n = len(diff_df)
    logger.info(
        "{n} setting(s) differ between {src} and {tgt}.",
        n=n,
        src=source_env,
        tgt=target_env,
    )

    out_path = out / f"server_settings_diff_{source_env}_vs_{target_env}.{fmt}"

    if fmt == "csv":
        diff_df.to_csv(out_path, index=False)
    elif fmt == "json":
        diff_df.to_json(out_path, orient="records", indent=2)
    else:
        raise ValueError(f"Unsupported format {fmt!r}. Use 'csv' or 'json'.")

    logger.success("Diff ({n} rows) written → {path}", n=n, path=out_path)


def apply_settings(
    source_env: str,
    target_env: str,
    output_dir: Path | None = None,
) -> None:
    """
    Copy source I-Server settings to the target server.

    Args:
        source_env: Environment to read settings from.
        target_env: Environment to push settings to.
        output_dir: Directory for the pre-apply snapshot.  Defaults to
                    MstrConfig.output_dir.

    Steps
    ──────
    1. Prompt for confirmation.
    2. Snapshot the target before writing (audit trail).
    3. Export source settings to a temp CSV.
    4. Import into target → update().
    """
    print(
        f"\n  !! WARNING: you are about to overwrite {target_env.upper()} "
        f"server settings with values from {source_env.upper()} !!\n"
    )
    answer = input("  Type 'yes' to confirm, anything else to abort: ").strip().lower()
    if answer != "yes":
        logger.warning("Apply aborted by user.")
        return

    tgt_config = _make_config(target_env)
    out = _out_dir(tgt_config, output_dir)

    # ── 1. Snapshot target before ─────────────────────────────────────────────
    tgt_conn_before = get_mstrio_connection(config=tgt_config)
    try:
        logger.info("Snapshotting {env} (target) before apply ...", env=target_env)
        tgt_before = ServerSettings(tgt_conn_before)
        before_path = out / f"server_settings_{target_env}_BEFORE.csv"
        tgt_before.to_csv(str(before_path))
        logger.info("Pre-apply snapshot → {path}", path=before_path)
    finally:
        tgt_conn_before.close()

    # ── 2. Export source to temp ──────────────────────────────────────────────
    tmp_path = _temp_csv()
    src_conn = get_mstrio_connection(config=_make_config(source_env))
    try:
        logger.info("Fetching {env} (source) settings ...", env=source_env)
        src_settings = ServerSettings(src_conn)
        src_settings.to_csv(tmp_path)
        logger.debug("Source settings written to temp file: {p}", p=tmp_path)
    finally:
        src_conn.close()

    # ── 3. Import into target and push ────────────────────────────────────────
    tgt_conn = get_mstrio_connection(config=tgt_config)
    try:
        logger.info("Applying source settings to {env} (target) ...", env=target_env)
        tgt_settings = ServerSettings(tgt_conn)
        tgt_settings.import_from(tmp_path)
        tgt_settings.update()
        logger.success(
            "Applied {src} settings to {tgt}.",
            src=source_env,
            tgt=target_env,
        )
    finally:
        tgt_conn.close()
        Path(tmp_path).unlink(missing_ok=True)


# ── CLI ───────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compare, export, or apply MicroStrategy I-Server settings.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python CompareServerSettings.py compare dev prod\n"
            "  python CompareServerSettings.py compare dev prod --format json --all\n"
            "  python CompareServerSettings.py export  qa  --description\n"
            "  python CompareServerSettings.py apply   dev prod\n"
        ),
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # ── compare ──────────────────────────────────────────────────────────────
    cmp = sub.add_parser("compare", help="Diff settings between two environments.")
    cmp.add_argument("source", choices=ENVS, help="Source (reference) environment.")
    cmp.add_argument("target", choices=ENVS, help="Target environment to compare.")
    cmp.add_argument(
        "--format", choices=["csv", "json"], default="csv",
        help="Output format (default: csv).",
    )
    cmp.add_argument(
        "--all",
        dest="show_diff_only",
        action="store_false",
        default=True,
        help="Include identical rows (default: diff rows only).",
    )
    cmp.add_argument("--output-dir", type=Path, default=None, metavar="PATH")

    # ── export ────────────────────────────────────────────────────────────────
    exp = sub.add_parser("export", help="Export settings for one environment.")
    exp.add_argument("env", choices=ENVS, help="Environment to export.")
    exp.add_argument(
        "--format", choices=["csv", "json"], default="csv",
        help="Output format (default: csv).",
    )
    exp.add_argument(
        "--description",
        dest="show_description",
        action="store_true",
        default=False,
        help="Include human-readable setting descriptions (CSV only).",
    )
    exp.add_argument("--output-dir", type=Path, default=None, metavar="PATH")

    # ── apply ─────────────────────────────────────────────────────────────────
    apl = sub.add_parser("apply", help="Push source settings to the target server.")
    apl.add_argument("source", choices=ENVS, help="Environment to read settings from.")
    apl.add_argument("target", choices=ENVS, help="Environment to overwrite.")
    apl.add_argument("--output-dir", type=Path, default=None, metavar="PATH")

    # ── Dispatch ──────────────────────────────────────────────────────────────
    args = parser.parse_args()

    if args.command == "compare":
        compare_settings(
            source_env=args.source,
            target_env=args.target,
            fmt=args.format,
            show_diff_only=args.show_diff_only,
            output_dir=args.output_dir,
        )
    elif args.command == "export":
        export_settings(
            env=args.env,
            fmt=args.format,
            show_description=args.show_description,
            output_dir=args.output_dir,
        )
    elif args.command == "apply":
        apply_settings(
            source_env=args.source,
            target_env=args.target,
            output_dir=args.output_dir,
        )
