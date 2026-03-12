"""
ProjectDuplicate.py — Duplicate a MicroStrategy project within or across
environments.

Reads duplication parameters from a YAML config file and uses the mstrio-py
SDK to perform either a same-environment or cross-environment project copy.

Same-environment:  Project.duplicate()                        + DuplicationConfig
Cross-environment: Project.duplicate_to_other_environment()   + CrossDuplicationConfig

The operation is asynchronous — the script polls for completion and logs
progress at a configurable interval.

Usage:
    python ProjectDuplicate.py [--config PATH] [--apply] [--output-dir PATH]

    python ProjectDuplicate.py                          # dry run, default config
    python ProjectDuplicate.py --apply                  # execute duplication
    python ProjectDuplicate.py --config custom.yaml     # custom config file

Notes:
    - Credentials are read from environment variables via mstrio_core (.env).
    - import_description is auto-populated from the source project's description.
    - Dry-run mode (default) shows what would be done without making changes.
"""

import argparse
import sys
import time
from pathlib import Path

import yaml
from loguru import logger
from mstrio.server import Project
from mstrio.server.project import CrossDuplicationConfig, DuplicationConfig

from mstrio_core import MstrConfig, get_mstrio_connection
from mstrio_core.config import MstrEnvironment

# ── Constants ─────────────────────────────────────────────────────────────────

DEFAULT_CONFIG = "project_duplicate_config.yaml"
ENVS = [e.value for e in MstrEnvironment]

DEFAULT_POLL_INTERVAL = 15      # seconds
DEFAULT_POLL_TIMEOUT = 3600     # seconds (1 hour)

# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_config(env: str) -> MstrConfig:
    """Build a MstrConfig pinned to a specific environment."""
    return MstrConfig(environment=MstrEnvironment(env))


def _load_config(path: Path) -> dict:
    """Load and validate the YAML configuration file."""
    if not path.exists():
        logger.error("Config file not found: {path}", path=path)
        sys.exit(1)

    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if not isinstance(cfg, dict):
        logger.error("Config file must be a YAML mapping: {path}", path=path)
        sys.exit(1)

    # Validate required fields
    source_env = cfg.get("source_env")
    if source_env not in ENVS:
        logger.error(
            "source_env must be one of {envs}, got {val!r}",
            envs=ENVS,
            val=source_env,
        )
        sys.exit(1)

    target_env = cfg.get("target_env")
    if target_env not in ENVS:
        logger.error(
            "target_env must be one of {envs}, got {val!r}",
            envs=ENVS,
            val=target_env,
        )
        sys.exit(1)

    if not cfg.get("project_name") and not cfg.get("project_id"):
        logger.error("Config must specify project_name or project_id.")
        sys.exit(1)

    return cfg


def _resolve_project(conn, cfg: dict) -> Project:
    """Resolve the source project by ID or name."""
    project_id = cfg.get("project_id")
    project_name = cfg.get("project_name")

    if project_id:
        logger.info("Resolving project by ID: {id}", id=project_id)
        return Project(connection=conn, id=project_id)

    logger.info("Resolving project by name: {name!r}", name=project_name)
    return Project(connection=conn, name=project_name)


def _build_duplication_config(cfg: dict, description: str) -> DuplicationConfig:
    """Build a DuplicationConfig from YAML 'duplication' section."""
    dup = cfg.get("duplication", {}) or {}
    return DuplicationConfig(
        schema_objects_only=dup.get("schema_objects_only", False),
        skip_empty_profile_folders=dup.get("skip_empty_profile_folders", True),
        skip_all_profile_folders=dup.get("skip_all_profile_folders", False),
        include_user_subscriptions=dup.get("include_user_subscriptions", True),
        include_contact_subscriptions=dup.get(
            "include_contact_subscriptions", True
        ),
        include_contacts_and_contact_groups=dup.get(
            "include_contacts_and_contact_groups", True
        ),
        import_description=description or "Project Duplication",
        import_default_locale=dup.get("import_default_locale", 0),
        import_locales=dup.get("import_locales"),
    )


def _build_cross_duplication_config(
    cfg: dict, description: str
) -> CrossDuplicationConfig:
    """Build a CrossDuplicationConfig from YAML 'duplication' + 'cross_duplication'."""
    dup = cfg.get("duplication", {}) or {}
    cross = cfg.get("cross_duplication", {}) or {}
    return CrossDuplicationConfig(
        # Base DuplicationConfig fields
        schema_objects_only=dup.get("schema_objects_only", False),
        skip_empty_profile_folders=dup.get("skip_empty_profile_folders", True),
        skip_all_profile_folders=dup.get("skip_all_profile_folders", False),
        include_user_subscriptions=dup.get("include_user_subscriptions", True),
        include_contact_subscriptions=dup.get(
            "include_contact_subscriptions", True
        ),
        include_contacts_and_contact_groups=dup.get(
            "include_contacts_and_contact_groups", True
        ),
        import_description=description or "Project Duplication",
        import_default_locale=dup.get("import_default_locale", 0),
        import_locales=dup.get("import_locales"),
        # CrossDuplicationConfig-specific fields
        include_all_user_groups=cross.get("include_all_user_groups", True),
        match_users_by_login=cross.get("match_users_by_login", False),
        match_by_name=cross.get("match_by_name"),
        admin_objects=cross.get("admin_objects"),
        admin_objects_rules=cross.get("admin_objects_rules"),
    )


def _log_config_summary(cfg: dict, description: str, is_cross: bool) -> None:
    """Log the duplication configuration for review."""
    dup = cfg.get("duplication", {}) or {}
    cross = cfg.get("cross_duplication", {}) or {}

    logger.info("─── Duplication Configuration ───")
    logger.info("  schema_objects_only:                {v}", v=dup.get("schema_objects_only", False))
    logger.info("  skip_empty_profile_folders:         {v}", v=dup.get("skip_empty_profile_folders", True))
    logger.info("  skip_all_profile_folders:           {v}", v=dup.get("skip_all_profile_folders", False))
    logger.info("  include_user_subscriptions:         {v}", v=dup.get("include_user_subscriptions", True))
    logger.info("  include_contact_subscriptions:      {v}", v=dup.get("include_contact_subscriptions", True))
    logger.info("  include_contacts_and_contact_groups:{v}", v=dup.get("include_contacts_and_contact_groups", True))
    logger.info("  import_description:                 {v!r}", v=description or "Project Duplication")
    logger.info("  import_default_locale:              {v}", v=dup.get("import_default_locale", 0))
    logger.info("  import_locales:                     {v}", v=dup.get("import_locales", "all"))

    if is_cross:
        logger.info("─── Cross-Environment Parameters ───")
        logger.info("  include_all_user_groups:  {v}", v=cross.get("include_all_user_groups", True))
        logger.info("  match_users_by_login:     {v}", v=cross.get("match_users_by_login", False))
        logger.info("  match_by_name:            {v}", v=cross.get("match_by_name", "none (GUID matching)"))


def _poll_project_ready(project: Project, interval: int, timeout: int) -> bool:
    """
    Poll until the duplicated project is loaded and ready.

    Returns True if the project becomes ready within the timeout,
    False otherwise.
    """
    start = time.time()
    elapsed = 0

    while elapsed < timeout:
        try:
            project.fetch()
            status = getattr(project, "status", None)
            logger.info(
                "Project status: {status} (elapsed {sec:.0f}s)",
                status=status,
                sec=elapsed,
            )
            # A successfully loaded project typically has status 0 or is
            # simply accessible.  If fetch() succeeds without error the
            # project exists on the server.
            if status is not None and status == 0:
                return True
        except Exception as exc:
            logger.debug(
                "Waiting for project (elapsed {sec:.0f}s): {exc}",
                sec=elapsed,
                exc=exc,
            )

        time.sleep(interval)
        elapsed = time.time() - start

    logger.warning(
        "Polling timed out after {sec}s — the duplication may still be in progress.",
        sec=timeout,
    )
    return False


# ── Main ──────────────────────────────────────────────────────────────────────


def main(
    config_path: Path,
    apply: bool = False,
    output_dir: Path | None = None,
) -> None:
    """
    Duplicate a MicroStrategy project based on YAML configuration.

    Args:
        config_path: Path to the YAML config file.
        apply:       If False (default), dry-run only.  If True, execute.
        output_dir:  Output directory (unused currently; reserved for future
                     duplication reports).
    """
    cfg = _load_config(config_path)

    source_env = cfg["source_env"]
    target_env = cfg["target_env"]
    is_cross = source_env != target_env

    polling_cfg = cfg.get("polling", {}) or {}
    poll_interval = polling_cfg.get("interval_seconds", DEFAULT_POLL_INTERVAL)
    poll_timeout = polling_cfg.get("timeout_seconds", DEFAULT_POLL_TIMEOUT)

    mode = "CROSS-ENVIRONMENT" if is_cross else "SAME-ENVIRONMENT"
    logger.info(
        "Duplication mode: {mode} ({src} → {tgt})",
        mode=mode,
        src=source_env,
        tgt=target_env,
    )

    # ── Connect to source and resolve project ─────────────────────────────
    src_config = _make_config(source_env)
    src_conn = get_mstrio_connection(config=src_config)
    tgt_conn = None

    try:
        project = _resolve_project(src_conn, cfg)
        project_name = project.name
        project_id = project.id
        description = getattr(project, "description", "") or ""

        logger.info(
            "Source project: {name} ({id})",
            name=project_name,
            id=project_id,
        )
        if description:
            logger.info("  Description: {desc!r}", desc=description)
        else:
            logger.info("  Description: (empty)")

        target_name = cfg.get("target_project_name") or project_name
        logger.info("Target project name: {name!r}", name=target_name)

        # ── Log configuration summary ─────────────────────────────────────
        _log_config_summary(cfg, description, is_cross)

        # ── Dry-run gate ──────────────────────────────────────────────────
        if not apply:
            logger.warning(
                "DRY RUN — no changes made.  Pass --apply to execute duplication."
            )
            return

        # ── Execute duplication ───────────────────────────────────────────
        if is_cross:
            tgt_config = _make_config(target_env)
            tgt_conn = get_mstrio_connection(config=tgt_config)

            cross_config = _build_cross_duplication_config(cfg, description)
            logger.info(
                "Starting cross-environment duplication: {src} → {tgt} ...",
                src=source_env,
                tgt=target_env,
            )
            new_project = project.duplicate_to_other_environment(
                target_name=target_name,
                target_env=tgt_conn,
                cross_duplication_config=cross_config,
            )
        else:
            dup_config = _build_duplication_config(cfg, description)
            logger.info("Starting same-environment duplication ...")
            new_project = project.duplicate(
                target_name=target_name,
                duplication_config=dup_config,
            )

        logger.info(
            "Duplication initiated.  New project: {name} ({id})",
            name=getattr(new_project, "name", target_name),
            id=getattr(new_project, "id", "pending"),
        )

        # ── Poll for completion ───────────────────────────────────────────
        logger.info(
            "Polling for completion (interval={interval}s, timeout={timeout}s) ...",
            interval=poll_interval,
            timeout=poll_timeout,
        )
        ready = _poll_project_ready(new_project, poll_interval, poll_timeout)

        if ready:
            logger.success(
                "Duplication complete.  Project {name!r} ({id}) is ready on {env}.",
                name=new_project.name,
                id=new_project.id,
                env=target_env,
            )
        else:
            logger.warning(
                "Duplication may still be in progress.  Check the target "
                "environment ({env}) manually.",
                env=target_env,
            )

    finally:
        src_conn.close()
        if tgt_conn is not None:
            tgt_conn.close()


# ── CLI ───────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Duplicate a MicroStrategy project within or across environments.",
    )
    parser.add_argument(
        "--config",
        metavar="PATH",
        default=DEFAULT_CONFIG,
        help=(
            f"Path to YAML config file (default: {DEFAULT_CONFIG} "
            f"in working directory)."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="Execute the duplication (default: dry run).",
    )
    parser.add_argument(
        "--output-dir",
        metavar="PATH",
        help="Output directory (reserved for future reports).",
    )
    args = parser.parse_args()
    main(
        config_path=Path(args.config),
        apply=args.apply,
        output_dir=Path(args.output_dir) if args.output_dir else None,
    )
