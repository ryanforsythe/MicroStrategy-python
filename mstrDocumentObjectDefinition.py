"""
mstrDocumentObjectDefinition.py
─────────────────────────────────────────────────────────────────────────────
Extract Dossier (Document) object definitions from a source project and
write the metadata to a Platform Analytics dataset.

Steps
─────
1.  Connect to MicroStrategy — MSTR_ENV selects dev / qa / prod.
2.  Set project context to the source project (MSTR_PROJECT_ID).
3.  Search all Dossier objects (type=55) in the project via REST API.
4.  Fetch the detailed definition of each Dossier via GET /api/v2/documents/{id}.
5.  Write a summary CSV to MSTR_OUTPUT_DIR as a backup / audit trail.
6.  Switch project context to Platform Analytics (MSTR_PA_PROJECT_ID).
7.  Write metadata rows to the PA dataset/cube (stub — see write_to_pa TODO).

Environment variables
─────────────────────
MSTR_ENV             dev | qa | prod  — selects the target environment (default: dev)
MSTR_PROJECT_ID      Source project GUID  (Dossiers extracted from this project)
MSTR_PA_PROJECT_ID   Platform Analytics project GUID
MSTR_PA_DATASET_ID   PA dataset/cube GUID to update (leave blank to skip PA write)

All standard MSTR_* connection variables apply; prefix with MSTR_{ENV}_ to
target a specific environment, e.g. MSTR_QA_BASE_URL, MSTR_QA_USERNAME.

Usage
─────
python mstrDocumentObjectDefinition.py
MSTR_ENV=qa python mstrDocumentObjectDefinition.py
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone

import jmespath as jq
from loguru import logger

from mstrio_core import (
    MstrConfig,
    MstrRestSession,
    write_csv,
    object_location,
)

# ─────────────────────────────────────────────────────────────────────────────
# Script-level configuration
# ─────────────────────────────────────────────────────────────────────────────

# MicroStrategy object type code for Dossiers (same integer as Documents)
DOSSIER_TYPE: int = 55

# Number of objects to retrieve per API page
PAGE_SIZE: int = 200

# Output columns for the CSV backup and PA dataset
CSV_COLUMNS: list[str] = [
    "ID",
    "Name",
    "Description",
    "Subtype",
    "Location",
    "DateCreated",
    "DateModified",
    "Owner",
    "DatasetCount",
    "FilterCount",
    "PromptCount",
    "ChapterCount",
]


# ─────────────────────────────────────────────────────────────────────────────
# REST API helpers
# ─────────────────────────────────────────────────────────────────────────────


def list_dossiers(session: MstrRestSession) -> list[dict]:
    """
    Return all Dossier objects in the session's active project.

    Calls GET /api/searches/results?type=55 with automatic offset pagination
    until all objects have been retrieved.

    Args:
        session: Authenticated MstrRestSession with a project already set.

    Returns:
        List of dossier result dicts from the search API.
    """
    dossiers: list[dict] = []
    offset = 0

    while True:
        r = session.get(
            "/searches/results",
            scope="project",
            params={
                "type": DOSSIER_TYPE,
                "limit": PAGE_SIZE,
                "offset": offset,
                "includeAncestors": "true",
                "fields": (
                    "id,name,description,type,subtype,"
                    "dateCreated,dateModified,owner,ancestors"
                ),
            },
        )
        r.raise_for_status()
        payload = r.json()

        page: list[dict] = payload.get("result", [])
        dossiers.extend(page)
        total: int = payload.get("totalItems", len(page))
        offset += PAGE_SIZE

        logger.debug(
            "Fetched {fetched}/{total} Dossiers (offset={offset})",
            fetched=len(dossiers),
            total=total,
            offset=offset,
        )

        if len(dossiers) >= total or not page:
            break

    logger.info("Found {count} Dossier objects in project.", count=len(dossiers))
    return dossiers


def get_dossier_definition(session: MstrRestSession, dossier_id: str) -> dict:
    """
    Fetch the detailed v2 definition of a single Dossier.

    Calls GET /api/v2/documents/{id} which returns datasets, chapters,
    filters, and prompts in addition to the base metadata.

    Args:
        session:    Authenticated MstrRestSession scoped to the source project.
        dossier_id: GUID of the Dossier to fetch.

    Returns:
        Full definition dict, or an empty dict if the request fails.
    """
    r = session.get(f"/v2/documents/{dossier_id}", scope="project")
    if not r.ok:
        logger.warning(
            "Could not retrieve definition for Dossier {id}: HTTP {status}",
            id=dossier_id,
            status=r.status_code,
        )
        return {}
    return r.json()


def build_row(dossier: dict, definition: dict) -> list:
    """
    Build a single CSV / PA row from a search result and its v2 definition.

    Args:
        dossier:    Entry from GET /api/searches/results (id, name, ancestors, …).
        definition: Full v2 definition from GET /api/v2/documents/{id}.

    Returns:
        List of values aligned to CSV_COLUMNS.
    """
    location = object_location(dossier.get("ancestors", []))

    datasets = jq.search("datasets", definition) or []
    filters  = jq.search("filters",  definition) or []
    prompts  = jq.search("prompts",  definition) or []
    chapters = jq.search("chapters", definition) or []

    return [
        jq.search("id",          dossier) or "",
        jq.search("name",        dossier) or "",
        jq.search("description", dossier) or "",
        jq.search("subtype",     dossier) or "",
        location,
        jq.search("dateCreated",  dossier) or "",
        jq.search("dateModified", dossier) or "",
        jq.search("owner.name",   dossier) or "",
        len(datasets),
        len(filters),
        len(prompts),
        len(chapters),
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Platform Analytics write
# ─────────────────────────────────────────────────────────────────────────────


def write_to_pa(
    session: MstrRestSession,
    config: MstrConfig,
    rows: list[list],
    columns: list[str],
) -> None:
    """
    Write metadata rows to a Platform Analytics dataset/cube.

    The PA project must already be set on the session before calling this
    function. Call session.set_project(project_id=config.pa_project_id) first.

    Uses session.mstrio_conn to access the live mstrio-py Connection — the
    same authenticated session as the REST API calls, no second login needed.

    Args:
        session: Authenticated MstrRestSession, already scoped to the PA project.
        config:  MstrConfig instance (provides pa_dataset_id).
        rows:    Data rows to write, aligned to columns.
        columns: Column names matching the PA cube table definition.

    TODO: Implement one of the two approaches below.
    """
    if not config.pa_dataset_id:
        logger.warning(
            "MSTR_PA_DATASET_ID is not set — skipping Platform Analytics write. "
            "Set MSTR_PA_DATASET_ID (or MSTR_{ENV}_PA_DATASET_ID) to enable."
        )
        return

    # ── TODO: Option A — mstrio-py OlapCube (preferred for bulk publishes) ───
    #
    # import pandas as pd
    # from mstrio.datasets import OlapCube
    #
    # conn = session.mstrio_conn   # already authenticated, project already set
    # cube = OlapCube(conn, id=config.pa_dataset_id)
    # df   = pd.DataFrame(rows, columns=columns)
    # cube.update(data={"DossierMetadata": df}, update_policy="replace")
    # cube.publish()
    # logger.success("Published {count} rows to PA cube {id}.", count=len(rows), id=config.pa_dataset_id)
    #
    # ── TODO: Option B — REST API dataset publish ─────────────────────────────
    #
    # session is already scoped to the PA project.
    # POST /api/datasets/{pa_dataset_id}/tables/{table_id}/data
    # with body: {"data": rows, "updatePolicy": "replace"}
    #
    # ─────────────────────────────────────────────────────────────────────────

    logger.info(
        "PA write stub: {count} rows ready for dataset {id} — implement TODO above.",
        count=len(rows),
        id=config.pa_dataset_id,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────


def main() -> int:
    # ── Bootstrap ─────────────────────────────────────────────────────────────
    # MstrConfig reads MSTR_ENV (dev/qa/prod) and resolves MSTR_{ENV}_*
    # variables automatically, falling back to the bare MSTR_* variables.
    # Logging is configured automatically — no separate setup_logging() needed.
    config = MstrConfig()

    logger.info(
        "Starting Dossier definition extraction "
        "(env={env}, base_url={url}, project={pid})",
        env=config.environment.value,
        url=config.base_url,
        pid=config.project_id or "(not set)",
    )

    if not config.project_id:
        logger.error(
            "Source project GUID is required. "
            "Set MSTR_PROJECT_ID or MSTR_{ENV}_PROJECT_ID.",
            ENV=config.environment.value.upper(),
        )
        return 1

    rows: list[list] = []
    errors: int = 0

    # ── MstrRestSession handles login, project scope, headers, and logout ─────
    with MstrRestSession(config) as session:

        # ── Step 1: Set source project context ────────────────────────────────
        session.set_project(project_id=config.project_id)

        # ── Step 2: List all Dossier objects in the project ───────────────────
        # GET /api/searches/results?type=55&limit=200&includeAncestors=true
        dossiers = list_dossiers(session)

        if not dossiers:
            logger.warning(
                "No Dossier objects found in project {pid}.",
                pid=config.project_id,
            )
            return 0

        # ── Step 3: Fetch v2 definition for each Dossier ─────────────────────
        # GET /api/v2/documents/{id} — returns datasets, chapters, filters, prompts
        for dossier in dossiers:
            did  = jq.search("id",   dossier) or ""
            name = jq.search("name", dossier) or "(unknown)"

            logger.debug(
                "Fetching v2 definition for '{name}' ({id})",
                name=name,
                id=did,
            )

            definition = get_dossier_definition(session, did)

            if not definition:
                logger.warning(
                    "Skipping '{name}' ({id}) — definition unavailable.",
                    name=name,
                    id=did,
                )
                errors += 1
                continue

            rows.append(build_row(dossier, definition))

        # ── Step 4: Write CSV backup ───────────────────────────────────────────
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        csv_path = (
            config.output_dir
            / f"dossier_definitions_{config.environment.value}_{ts}.csv"
        )
        write_csv(rows, columns=CSV_COLUMNS, path=csv_path)

        # ── Step 5: Switch to Platform Analytics and write ────────────────────
        if config.pa_project_id:
            logger.info(
                "Switching to Platform Analytics project ({pid}).",
                pid=config.pa_project_id,
            )
            session.set_project(project_id=config.pa_project_id)
            write_to_pa(session, config, rows, CSV_COLUMNS)
        else:
            logger.warning(
                "MSTR_PA_PROJECT_ID is not set — skipping Platform Analytics write."
            )

    # ── Summary ───────────────────────────────────────────────────────────────
    logger.info(
        "Extraction complete: {ok} definitions written, {err} errors.",
        ok=len(rows),
        err=errors,
    )
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
