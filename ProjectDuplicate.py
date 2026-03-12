"""
ProjectDuplicate.py — Duplicate a MicroStrategy project within or across
environments.

Reads duplication parameters from a YAML config file and uses the MicroStrategy
REST API (/api/projectDuplications) to perform either a same-environment or
cross-environment project copy.

Same-environment:  POST on source → poll until "completed"
Cross-environment: POST on source → poll until "exported" →
                   PUT on target  → poll until "completed"

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

REST API reference:
    https://microstrategy.github.io/rest-api-docs/common-workflows/administration/project-duplication/
    https://microstrategy.github.io/rest-api-docs/common-workflows/administration/project-duplication/cross-env-project-duplication
"""

import argparse
import json
import sys
import time
from pathlib import Path

import yaml
from loguru import logger
from mstrio.server import Project

from mstrio_core import MstrConfig, MstrRestSession
from mstrio_core.config import MstrEnvironment

# ── Constants ─────────────────────────────────────────────────────────────────

DEFAULT_CONFIG = "project_duplicate_config.yaml"
ENVS = [e.value for e in MstrEnvironment]

DEFAULT_POLL_INTERVAL = 15      # seconds
DEFAULT_POLL_TIMEOUT = 3600     # seconds (1 hour)

# Status keyword matching (lowercase)
FAILURE_KEYWORDS = ("failed", "error", "cancel")
EXPORT_SUCCESS = ("exported",)
FINAL_SUCCESS = ("completed",)

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
    """Resolve the source project by ID or name via mstrio-py."""
    project_id = cfg.get("project_id")
    project_name = cfg.get("project_name")

    if project_id:
        logger.info("Resolving project by ID: {id}", id=project_id)
        return Project(connection=conn, id=project_id)

    logger.info("Resolving project by name: {name!r}", name=project_name)
    return Project(connection=conn, name=project_name)


# ── REST API Request Body Builders ───────────────────────────────────────────


def _build_request_body(
    cfg: dict,
    src_base_url: str,
    tgt_base_url: str,
    project_id: str,
    project_name: str,
    target_name: str,
    description: str,
    is_cross: bool,
) -> dict:
    """
    Build the REST API request body for POST /api/projectDuplications.

    Maps YAML configuration fields to the REST API JSON schema:
      duplication.*             → settings.export / settings.import
      cross_duplication.*       → settings.export.configurationObjects /
                                  settings.import.configurationObjects
    """
    dup = cfg.get("duplication", {}) or {}
    cross = cfg.get("cross_duplication", {}) or {}

    body: dict = {
        "source": {
            "environment": {
                "id": src_base_url,
                "name": cfg["source_env"],
            },
            "project": {
                "id": project_id,
                "name": project_name,
            },
        },
        "target": {
            "environment": {
                "id": tgt_base_url,
                "name": cfg["target_env"],
            },
            "project": {
                "name": target_name,
            },
        },
        "settings": {
            "export": {
                "projectObjectsPreference": {
                    "schemaObjectsOnly": dup.get("schema_objects_only", False),
                    "skipEmptyProfileFolders": dup.get(
                        "skip_empty_profile_folders", True
                    ),
                },
                "subscriptionPreferences": {
                    "includeUserSubscriptions": dup.get(
                        "include_user_subscriptions", True
                    ),
                    "includeContactSubscriptions": dup.get(
                        "include_contact_subscriptions", True
                    ),
                },
            },
            "import": {
                "description": description or "Project Duplication",
                "defaultLocale": dup.get("import_default_locale", 0),
            },
        },
    }

    # Import locales — REST API requires a non-empty list.
    # null / omitted in YAML → [0] (default locale = all languages).
    locales = dup.get("import_locales")
    if locales is not None:
        body["settings"]["import"]["locales"] = locales
    else:
        body["settings"]["import"]["locales"] = [0]

    # Cross-environment specific: export configurationObjects
    if is_cross:
        export_config_objects: dict = {
            "includeAllUserGroups": cross.get("include_all_user_groups", True),
        }
        rules = cross.get("admin_objects_rules")
        if rules is not None:
            export_config_objects["rules"] = rules
        objects = cross.get("admin_objects")
        if objects is not None:
            export_config_objects["objects"] = objects
        body["settings"]["export"]["configurationObjects"] = export_config_objects

        # Cross-environment specific: import configurationObjects
        import_config_objects: dict = {
            "matchUsersByLogin": cross.get("match_users_by_login", False),
        }
        conflict_rules = cross.get("conflict_rules")
        if conflict_rules is not None:
            import_config_objects["conflictRules"] = conflict_rules
        body["settings"]["import"]["configurationObjects"] = import_config_objects

    return body


def _build_target_body(
    cfg: dict,
    src_base_url: str,
    tgt_base_url: str,
    project_id: str,
    project_name: str,
    target_name: str,
) -> dict:
    """
    Build the REST API request body for PUT /api/projectDuplications/{id}
    on the target environment (cross-environment import phase).

    The PUT body contains source/target identification but settings are empty —
    they were already captured in the POST on the source environment.
    """
    return {
        "source": {
            "environment": {
                "id": src_base_url,
                "name": cfg["source_env"],
            },
            "project": {
                "id": project_id,
                "name": project_name,
            },
        },
        "target": {
            "environment": {
                "id": tgt_base_url,
                "name": cfg["target_env"],
            },
            "project": {
                "name": target_name,
            },
        },
        "settings": {},
    }


# ── Logging Helpers ──────────────────────────────────────────────────────────


def _log_config_summary(cfg: dict, description: str, is_cross: bool) -> None:
    """Log the duplication configuration for review."""
    dup = cfg.get("duplication", {}) or {}
    cross = cfg.get("cross_duplication", {}) or {}

    logger.info("─── Duplication Configuration ───")
    logger.info(
        "  schema_objects_only:           {v}",
        v=dup.get("schema_objects_only", False),
    )
    logger.info(
        "  skip_empty_profile_folders:    {v}",
        v=dup.get("skip_empty_profile_folders", True),
    )
    logger.info(
        "  include_user_subscriptions:    {v}",
        v=dup.get("include_user_subscriptions", True),
    )
    logger.info(
        "  include_contact_subscriptions: {v}",
        v=dup.get("include_contact_subscriptions", True),
    )
    logger.info(
        "  import_description:            {v!r}",
        v=description or "Project Duplication",
    )
    logger.info(
        "  import_default_locale:         {v}",
        v=dup.get("import_default_locale", 0),
    )
    logger.info(
        "  import_locales:                {v}",
        v=dup.get("import_locales", "all"),
    )

    if is_cross:
        logger.info("─── Cross-Environment Parameters ───")
        logger.info(
            "  include_all_user_groups:  {v}",
            v=cross.get("include_all_user_groups", True),
        )
        logger.info(
            "  match_users_by_login:     {v}",
            v=cross.get("match_users_by_login", False),
        )
        conflict_rules = cross.get("conflict_rules")
        if conflict_rules:
            logger.info("  conflict_rules:           {v}", v=conflict_rules)


def _log_failure_detail(data: dict) -> None:
    """Log all available detail from a failed duplication REST API response."""

    def _get(d: dict, *keys: str, default: str = "(not available)") -> str:
        """Safely traverse nested dict keys."""
        current = d
        for k in keys:
            if isinstance(current, dict):
                current = current.get(k)
            else:
                return default
        return str(current) if current is not None else default

    logger.error("─── Failure Detail ───")
    logger.error("  message:          {v}", v=_get(data, "message"))
    logger.error("  progress:         {v}", v=_get(data, "progress"))
    logger.error("  duplication_id:   {v}", v=_get(data, "id"))
    logger.error(
        "  source_project:   {v} ({id})",
        v=_get(data, "source", "project", "name"),
        id=_get(data, "source", "project", "id"),
    )
    logger.error(
        "  target_project:   {v}",
        v=_get(data, "target", "project", "name"),
    )
    logger.error(
        "  source_env:       {v}",
        v=_get(data, "source", "environment", "name"),
    )
    logger.error(
        "  target_env:       {v}",
        v=_get(data, "target", "environment", "name"),
    )
    logger.error("  created:          {v}", v=_get(data, "createdDate"))
    logger.error("  last_updated:     {v}", v=_get(data, "lastUpdatedDate"))


# ── Polling ──────────────────────────────────────────────────────────────────


def _poll_duplication(
    session: MstrRestSession,
    dup_id: str,
    interval: int,
    timeout: int,
    success_keywords: tuple[str, ...],
    phase_label: str = "",
) -> dict | None:
    """
    Poll GET /api/projectDuplications/{id} until a terminal state is reached.

    Args:
        session:          Active MstrRestSession to poll against.
        dup_id:           Duplication job ID from POST/PUT response.
        interval:         Seconds between polls.
        timeout:          Maximum seconds to poll.
        success_keywords: Status substrings indicating success (e.g. "exported",
                          "completed").
        phase_label:      Label for log messages (e.g. "Export", "Import").

    Returns:
        The duplication JSON on success, or None on failure/timeout.
    """
    label = f" [{phase_label}]" if phase_label else ""
    start = time.time()
    elapsed = 0.0

    while elapsed < timeout:
        try:
            r = session.get(
                f"/projectDuplications/{dup_id}", scope="server"
            )
            if r.ok:
                data = r.json()
                status = data.get("status", "unknown")
                progress = data.get("progress")
                message = data.get("message", "")

                progress_str = (
                    f"  progress: {progress}%" if progress is not None else ""
                )
                logger.info(
                    "Duplication status{label}: {status}{progress}"
                    " (elapsed {sec:.0f}s)",
                    label=label,
                    status=status,
                    progress=progress_str,
                    sec=elapsed,
                )

                status_lower = status.lower()

                # ── Terminal failure ──────────────────────────────────────
                if any(f in status_lower for f in FAILURE_KEYWORDS):
                    logger.error(
                        "Duplication{label} ended with failure: {status}",
                        label=label,
                        status=status,
                    )
                    if message:
                        logger.error("  message: {msg}", msg=message)
                    _log_failure_detail(data)
                    return None

                # ── Terminal success ──────────────────────────────────────
                if any(s in status_lower for s in success_keywords):
                    return data

            else:
                logger.debug(
                    "Status poll returned HTTP {code} (elapsed {sec:.0f}s)",
                    code=r.status_code,
                    sec=elapsed,
                )

        except Exception as exc:
            logger.debug(
                "Waiting for duplication (elapsed {sec:.0f}s): {exc}",
                sec=elapsed,
                exc=exc,
            )

        time.sleep(interval)
        elapsed = time.time() - start

    logger.warning(
        "Polling timed out after {sec}s — the duplication may still be "
        "in progress.",
        sec=timeout,
    )
    return None


# ── Duplication Execution ────────────────────────────────────────────────────


def _initiate_duplication(
    session: MstrRestSession, body: dict
) -> dict | None:
    """
    POST /api/projectDuplications to start the duplication job.

    Returns the response JSON (contains the duplication ID), or None on failure.
    """
    logger.debug(
        "POST /projectDuplications body:\n{body}",
        body=json.dumps(body, indent=2),
    )

    r = session.post(
        "/projectDuplications",
        scope="project",
        headers={"Prefer": "respond-async"},
        json=body,
    )

    if r.status_code not in (200, 201, 202):
        logger.error(
            "Failed to initiate duplication: HTTP {status} {body}",
            status=r.status_code,
            body=r.text[:1000],
        )
        return None

    data = r.json()
    logger.info(
        "Duplication initiated — ID: {id}, status: {status}",
        id=data.get("id", "unknown"),
        status=data.get("status", "unknown"),
    )
    return data


def _trigger_target_import(
    session: MstrRestSession, dup_id: str, body: dict
) -> dict | None:
    """
    PUT /api/projectDuplications/{id} on the target environment to start
    the import phase of a cross-environment duplication.

    Returns the response JSON, or None on failure.
    """
    logger.debug(
        "PUT /projectDuplications/{id} body:\n{body}",
        id=dup_id,
        body=json.dumps(body, indent=2),
    )

    r = session.put(
        f"/projectDuplications/{dup_id}",
        scope="server",
        headers={"Prefer": "respond-async"},
        json=body,
    )

    if r.status_code not in (200, 201, 202):
        logger.error(
            "Failed to trigger target import: HTTP {status} {body}",
            status=r.status_code,
            body=r.text[:1000],
        )
        return None

    data = r.json()
    logger.info(
        "Target import triggered — ID: {id}, status: {status}",
        id=data.get("id", "unknown"),
        status=data.get("status", "unknown"),
    )
    return data


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
    tgt_config = _make_config(target_env) if is_cross else src_config

    src_session: MstrRestSession | None = None
    tgt_session: MstrRestSession | None = None

    try:
        src_session = MstrRestSession(src_config)
        src_session.login()

        conn = src_session.mstrio_conn
        project = _resolve_project(conn, cfg)
        project_name = project.name
        project_id = project.id
        description = getattr(project, "description", "") or ""

        # Set the source project on the session for project-scoped calls
        src_session.set_project(project_id=project_id)

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

        logger.info(
            "Source Library URL: {url}", url=src_config.base_url
        )
        logger.info(
            "Target Library URL: {url}", url=tgt_config.base_url
        )

        # ── Log configuration summary ─────────────────────────────────────
        _log_config_summary(cfg, description, is_cross)

        # ── Dry-run gate ──────────────────────────────────────────────────
        if not apply:
            logger.warning(
                "DRY RUN — no changes made.  "
                "Pass --apply to execute duplication."
            )
            return

        # ── Build request body ────────────────────────────────────────────
        body = _build_request_body(
            cfg=cfg,
            src_base_url=src_config.base_url,
            tgt_base_url=tgt_config.base_url,
            project_id=project_id,
            project_name=project_name,
            target_name=target_name,
            description=description,
            is_cross=is_cross,
        )

        # ── Execute duplication ───────────────────────────────────────────
        if is_cross:
            # ── Phase 1: Export on source ─────────────────────────────────
            logger.info(
                "Starting cross-environment duplication: {src} → {tgt} ...",
                src=source_env,
                tgt=target_env,
            )
            dup_data = _initiate_duplication(src_session, body)
            if dup_data is None:
                logger.error("Failed to initiate duplication on source.")
                return

            dup_id = dup_data["id"]

            logger.info(
                "Polling source for export completion "
                "(interval={interval}s, timeout={timeout}s) ...",
                interval=poll_interval,
                timeout=poll_timeout,
            )
            export_result = _poll_duplication(
                session=src_session,
                dup_id=dup_id,
                interval=poll_interval,
                timeout=poll_timeout,
                success_keywords=EXPORT_SUCCESS,
                phase_label="Export",
            )
            if export_result is None:
                logger.error(
                    "Export phase failed — aborting cross-environment "
                    "duplication."
                )
                return

            logger.success("Export phase complete.")

            # ── Phase 2: Import on target ────────────────────────────────
            tgt_session = MstrRestSession(tgt_config)
            tgt_session.login()

            target_body = _build_target_body(
                cfg=cfg,
                src_base_url=src_config.base_url,
                tgt_base_url=tgt_config.base_url,
                project_id=project_id,
                project_name=project_name,
                target_name=target_name,
            )

            import_data = _trigger_target_import(
                tgt_session, dup_id, target_body
            )
            if import_data is None:
                logger.error("Failed to trigger import on target.")
                return

            logger.info(
                "Polling target for import completion "
                "(interval={interval}s, timeout={timeout}s) ...",
                interval=poll_interval,
                timeout=poll_timeout,
            )
            final_result = _poll_duplication(
                session=tgt_session,
                dup_id=dup_id,
                interval=poll_interval,
                timeout=poll_timeout,
                success_keywords=FINAL_SUCCESS,
                phase_label="Import",
            )

        else:
            # ── Same-environment: single POST + poll ─────────────────────
            logger.info("Starting same-environment duplication ...")
            dup_data = _initiate_duplication(src_session, body)
            if dup_data is None:
                logger.error("Failed to initiate duplication.")
                return

            dup_id = dup_data["id"]

            logger.info(
                "Polling for completion "
                "(interval={interval}s, timeout={timeout}s) ...",
                interval=poll_interval,
                timeout=poll_timeout,
            )
            final_result = _poll_duplication(
                session=src_session,
                dup_id=dup_id,
                interval=poll_interval,
                timeout=poll_timeout,
                success_keywords=FINAL_SUCCESS,
                phase_label="Duplication",
            )

        # ── Report result ─────────────────────────────────────────────────
        if final_result is not None:
            tgt_project_name = (
                final_result.get("target", {})
                .get("project", {})
                .get("name", target_name)
            )
            tgt_project_id = (
                final_result.get("target", {})
                .get("project", {})
                .get("id", "unknown")
            )
            logger.success(
                "Duplication complete.  Project {name!r} ({id}) is ready "
                "on {env}.",
                name=tgt_project_name,
                id=tgt_project_id,
                env=target_env,
            )
        else:
            logger.warning(
                "Duplication may still be in progress.  Check the target "
                "environment ({env}) manually.",
                env=target_env,
            )

    finally:
        if src_session is not None:
            src_session.logout()
        if tgt_session is not None:
            tgt_session.logout()


# ── CLI ───────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Duplicate a MicroStrategy project within or across environments."
        ),
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
