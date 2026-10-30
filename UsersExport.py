"""
UsersExport.py — Export all MicroStrategy users from a server to CSV.

Output columns:
    base_url              – Environment URL (for traceability)
    guid                  – User object GUID
    name                  – Display name
    id                    – Login username / account ID
    trusted_auth          – Trust ID (SAML/trusted-auth); empty string if not set
    status                – Enabled / Disabled
    standard_auth_enabled – True / False (standard authentication allowed)
    email_address         – The user's DEFAULT email address
    created_ts            – Created timestamp (as the server returns it)
    modified_ts           – Last modified timestamp
    description           – User description
    group_membership      – JSON array: [{"id": "...", "name": "..."}, ...]

Usage:
    python UsersExport.py <env>  [--output-dir PATH] [--concurrency N]

    python UsersExport.py dev
    python UsersExport.py prod --output-dir c:/reports

Output: users_export_{env}_{YYYYMMDD_HHMM}.csv

Performance note:
    list_users() returns lightweight objects: status, standard auth, the default
    email address and both timestamps are NOT in that payload, so each user is
    fetched once via User(conn, id=) (a single GET /api/users/{id} covers all of
    them) on a ThreadPoolExecutor — --concurrency, default 10. Memberships may
    also cost a call per user in some mstrio-py versions. For very large
    environments consider REST directly:
    GET /api/users?fields=id,name,username,trustId,memberships
"""

import argparse
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

from loguru import logger
from mstrio.users_and_groups import User, list_users

from mstrio_core import MstrConfig, get_mstrio_connection, write_csv
from mstrio_core.config import MstrEnvironment

# ── Constants ─────────────────────────────────────────────────────────────────

ENVS = [e.value for e in MstrEnvironment]
COLUMNS = ["base_url", "guid", "name", "id", "trusted_auth", "status",
           "standard_auth_enabled", "email_address", "created_ts", "modified_ts",
           "description", "group_membership"]
DEFAULT_CONCURRENCY = 10

# ── Helpers ───────────────────────────────────────────────────────────────────


def _memberships_json(user) -> str:
    """
    Serialize a user's group memberships to a JSON array string.

    Handles UserGroup objects (with .id / .name attributes) and plain dicts.
    Returns "[]" on any access failure so a single bad user does not abort
    the export.
    """
    try:
        raw = user.memberships or []
        groups: list[dict] = []
        for m in raw:
            if hasattr(m, "id"):
                groups.append({"id": m.id, "name": getattr(m, "name", "")})
            elif isinstance(m, dict):
                groups.append({"id": m.get("id", ""), "name": m.get("name", "")})
        return json.dumps(groups)
    except Exception as exc:
        logger.warning(
            "Could not read memberships for user {uid}: {err}",
            uid=getattr(user, "id", "?"),
            err=exc,
        )
        return "[]"


def _timestamp(value) -> str:
    """A server timestamp as a string, whatever type mstrio hands back."""
    if not value:
        return ""
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _details(conn, user_id: str) -> dict:
    """Fetch the fields list_users() leaves out: status, standard auth, the
    default email address, both timestamps and the description.

    One GET /api/users/{id} covers all of them (mstrio's User._API_GETTERS maps
    them to the same getter). A failure for one user is logged and leaves that
    user's extra columns blank rather than aborting the export.
    """
    try:
        user = User(conn, id=user_id)
        enabled = user.enabled
        return {
            "status": "Enabled" if enabled else "Disabled" if enabled is not None else "",
            "standard_auth_enabled": "" if user.standard_auth is None
            else str(bool(user.standard_auth)),
            "email_address": user.default_email_address or "",
            "created_ts": _timestamp(user.date_created),
            "modified_ts": _timestamp(user.date_modified),
            "description": getattr(user, "description", None) or "",
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not read details for user {uid}: {err}", uid=user_id, err=exc)
        return {"status": "", "standard_auth_enabled": "", "email_address": "",
                "created_ts": "", "modified_ts": "", "description": ""}


# ── Main ──────────────────────────────────────────────────────────────────────


def main(env: str, output_dir: Path | None = None,
         concurrency: int = DEFAULT_CONCURRENCY) -> None:
    """
    Export all users from the given environment to CSV.

    Args:
        env:         Environment to connect to ("dev", "qa", or "prod").
        output_dir:  Directory for the output CSV.  Defaults to
                     MstrConfig.output_dir (MSTR_OUTPUT_DIR env var, c:/tmp).
        concurrency: Parallel per-user detail fetches (default 10).
    """
    config = MstrConfig(environment=MstrEnvironment(env))
    filename = f"users_export_{env}_{datetime.now():%Y%m%d_%H%M}.csv"
    out_path = (output_dir or config.output_dir) / filename
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    conn = get_mstrio_connection(config=config)
    try:
        logger.info("Fetching users from {url} ({env})", url=config.base_url, env=env)
        users = list_users(conn)
        logger.info("Retrieved {n} user(s).", n=len(users))

        logger.info("Fetching status, standard auth, email, timestamps and description "
                    "for {n} user(s) ({c} at a time)...", n=len(users), c=concurrency)
        with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
            details = list(pool.map(lambda u: _details(conn, u.id), users))

        rows: list[list] = []
        for user, detail in zip(users, details):
            rows.append([
                config.base_url,
                user.id,
                user.name,
                user.username,
                user.trust_id or "",
                detail["status"],
                detail["standard_auth_enabled"],
                detail["email_address"],
                detail["created_ts"],
                detail["modified_ts"],
                detail["description"],
                _memberships_json(user),
            ])
        disabled = sum(1 for d in details if d["status"] == "Disabled")
        no_email = sum(1 for d in details if not d["email_address"])
        logger.info("{disabled} disabled; {no_email} without a default email address.",
                    disabled=disabled, no_email=no_email)

        write_csv(rows, columns=COLUMNS, path=out_path)
        logger.success("Exported {n} users → {path}", n=len(rows), path=out_path)

    finally:
        conn.close()


# ── CLI ───────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export all MicroStrategy users from a server to CSV."
    )
    parser.add_argument(
        "env",
        choices=ENVS,
        help="Environment to export users from.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        metavar="PATH",
        help="Output directory (default: MSTR_OUTPUT_DIR or c:/tmp).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        metavar="N",
        help=f"Parallel per-user detail fetches (default: {DEFAULT_CONCURRENCY}).",
    )

    args = parser.parse_args()
    main(env=args.env, output_dir=args.output_dir, concurrency=args.concurrency)
