"""
UsersExport.py — Export all MicroStrategy users from a server to CSV.

Output columns:
    base_url         – Environment URL (for traceability)
    guid             – User object GUID
    name             – Display name
    id               – Login username / account ID
    trusted_auth     – Trust ID (SAML/trusted-auth); empty string if not set
    group_membership – JSON array: [{"id": "...", "name": "..."}, ...]

Usage:
    python UsersExport.py <env>  [--output-dir PATH]

    python UsersExport.py dev
    python UsersExport.py prod --output-dir c:/reports

Performance note:
    Accessing user.memberships may trigger a separate API call per user in some
    mstrio-py versions. For large environments (1000+ users), consider the REST
    API approach: GET /api/users?fields=id,name,username,trustId,memberships
    which returns everything in a single paginated request.
"""

import argparse
import json
from pathlib import Path

from loguru import logger
from mstrio.users_and_groups import list_users

from mstrio_core import MstrConfig, get_mstrio_connection, write_csv
from mstrio_core.config import MstrEnvironment

# ── Constants ─────────────────────────────────────────────────────────────────

ENVS = [e.value for e in MstrEnvironment]
OUTPUT_FILENAME = "users_export.csv"
COLUMNS = ["base_url", "guid", "name", "id", "trusted_auth", "group_membership"]

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


# ── Main ──────────────────────────────────────────────────────────────────────


def main(env: str, output_dir: Path | None = None) -> None:
    """
    Export all users from the given environment to CSV.

    Args:
        env:        Environment to connect to ("dev", "qa", or "prod").
        output_dir: Directory for the output CSV.  Defaults to
                    MstrConfig.output_dir (MSTR_OUTPUT_DIR env var, c:/tmp).
    """
    config = MstrConfig(environment=MstrEnvironment(env))
    out_path = (output_dir or config.output_dir) / OUTPUT_FILENAME
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    conn = get_mstrio_connection(config=config)
    try:
        logger.info("Fetching users from {url} ({env})", url=config.base_url, env=env)
        users = list_users(conn)
        logger.info("Retrieved {n} user(s).", n=len(users))

        rows: list[list] = []
        for user in users:
            rows.append([
                config.base_url,
                user.id,
                user.name,
                user.username,
                user.trust_id or "",
                _memberships_json(user),
            ])

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

    args = parser.parse_args()
    main(env=args.env, output_dir=args.output_dir)
