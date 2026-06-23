# MicroStrategy Python Management Scripts

## Project Overview

A collection of Python utility scripts for managing the MicroStrategy (Strategy) platform. Scripts handle metadata extraction, object management, user administration, content group management, and configuration updates via both the MicroStrategy REST API and the `mstrio-py` Python library.

## Quick Start

**Python 3.9+** required.

```bash
pip install -r requirements.txt
cp .env.example .env        # fill in MSTR_BASE_URL, MSTR_USERNAME, MSTR_PASSWORD
python ListProjects.py      # run any script directly from CLI
```

**Development environments:** PyCharm (primary IDE), Jupyter Notebooks, and CLI are all used.
- PyCharm: open the repo root as a project; set the `.env` file in the run configuration's "EnvFile" field or via the EnvFile plugin
- Jupyter: `%load_ext dotenv` + `%dotenv` at the top of a notebook, or set env vars in the kernel launch config
- For Workstation-embedded scripts, `workstationData` is injected automatically by the host environment

## Documentation & References

- **mstrio-py Docs:** https://www2.microstrategy.com/producthelp/current/mstrio-py/index.html
- **mstrio-py API Reference:** https://www2.microstrategy.com/producthelp/Current/mstrio-py/mstrio.html
- **mstrio-py GitHub:** https://github.com/MicroStrategy/mstrio-py
- **REST API Docs:** https://microstrategy.github.io/rest-api-docs/
- **REST API Interactive:** https://demo.microstrategy.com/MicroStrategyLibrary/api-docs/index.html

## Technology Stack

| Package | Purpose |
|---------|---------|
| `mstrio-py` | MicroStrategy Python SDK — object-oriented interface |
| `requests` | HTTP client for direct REST API calls |
| `pandas` | Data manipulation and DataFrame output |
| `jmespath` | JSON querying for REST API responses |
| `openpyxl` | Excel read/write |
| `loguru` | Structured logging — use `setup_logging()` from `mstrio_core` |
| `IPython` | Jupyter notebook display support |

## Architecture: REST API vs mstrio-py

MicroStrategy introduces new features via REST API first, then the Python library. Use this decision framework:

### Prefer mstrio-py when:
- The operation is well-supported and the library returns sufficient data
- Performing standard operations: user management, project settings, listing objects
- You need object-oriented access with automatic type handling
- Example: `ListProjects.py`, `add_trustedauth_internal_users.py`, `UpdateProjectSettings.py`

### Prefer REST API (via `requests`) when:
- mstrio-py doesn't expose the needed data or returns insufficient detail
- Performing complex metadata extraction (report templates, metric definitions, object expressions)
- Working with changesets, content groups, migration packages, or new/unreleased features
- Example: `GetReportDefs_OutputCSV_SearchObject.py`, `UpdateMetric_ReportDataSource_ChangeSet.py`

### Hybrid approach is common and acceptable — use whichever gives the needed data.

## mstrio_core Module

All new scripts should import from `mstrio_core` instead of duplicating auth/connection logic.

### Setup

```bash
pip install -r requirements.txt
cp .env.example .env   # then fill in your values
```

### Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `MSTR_ENV` | No | `dev` | Active environment: `dev`, `qa`, or `prod` |
| `MSTR_BASE_URL` | Yes* | — | Full Library URL, e.g. `https://server.cloud.microstrategy.com/MicroStrategyLibrary` |
| `MSTR_USERNAME` | Yes* | — | Login username |
| `MSTR_PASSWORD` | Yes* | — | Login password |
| `MSTR_LOGIN_MODE` | No | `1` | `1`=Standard, `16`=SAML, `64`=LDAP |
| `MSTR_PROJECT_ID` | No | — | Default project GUID |
| `MSTR_PROJECT_NAME` | No | — | Default project name (fallback if no ID) |
| `MSTR_PA_PROJECT_ID` | No | — | Platform Analytics project GUID |
| `MSTR_PA_DATASET_ID` | No | — | Platform Analytics dataset/cube GUID for metadata writes |
| `MSTR_OUTPUT_DIR` | No | `c:/tmp` | Default output directory |
| `MSTR_LOG_DIR` | No | `logs` | Log file directory |
| `MSTR_LOG_LEVEL` | No | `INFO` | `DEBUG`/`INFO`/`WARNING`/`ERROR` |
| `MSTR_SSL_VERIFY` | No | `true` | `true`/`false` — disable for self-signed or internal CA certs |

\* Required, but can be supplied as `MSTR_{ENV}_{VAR}` instead (see Multi-Environment below).

### Multi-Environment Pattern

`MstrConfig` supports dev / qa / prod environments via a prefix lookup chain:

```
MSTR_{ENV}_{VAR}  →  MSTR_{VAR}  →  built-in default
```

Set `MSTR_ENV` once and define per-environment variables — no code changes needed to switch environments:

```ini
# .env
MSTR_ENV=qa                          # switch to qa
MSTR_DEV_BASE_URL=https://dev.example.com/MicroStrategyLibrary
MSTR_QA_BASE_URL=https://qa.example.com/MicroStrategyLibrary
MSTR_PROD_BASE_URL=https://prod.example.com/MicroStrategyLibrary
```

Every variable supports the prefix (e.g., `MSTR_QA_LOGIN_MODE`, `MSTR_PROD_PROJECT_ID`). The bare `MSTR_*` variables serve as a shared fallback for any environment.

```python
from mstrio_core import MstrConfig, MstrEnvironment

# Reads MSTR_ENV from environment — no argument needed
config = MstrConfig()

# Force a specific environment programmatically
config = MstrConfig(environment=MstrEnvironment.PROD)

print(config.environment.value)  # "prod"
print(config.base_url)           # resolved from MSTR_PROD_BASE_URL → MSTR_BASE_URL
```

### REST API session (new pattern)

```python
from mstrio_core import MstrConfig, MstrRestSession

config = MstrConfig()    # also configures logging

with MstrRestSession(config) as session:
    # Server-scoped (no project header)
    r = session.get("/migrations")

    # Set project, then project-scoped calls auto-include X-MSTR-ProjectID
    session.set_project(name="Platform Analytics")
    r = session.get("/reports/" + guid + "/instances")

    # Force server scope even when project is set
    r = session.get("/contentGroups/" + cg_id, scope="server")

    # Changeset — commits on success, rolls back on exception
    with session.changeset() as cs_id:
        session.put("/model/metrics/" + guid, json=body, changeset_id=cs_id)
```

### mstrio-py SDK via session (hybrid — one login for REST + SDK)

When a script uses both raw REST calls and mstrio-py SDK objects, use
`session.mstrio_conn` — the same authenticated connection, no second login:

```python
from mstrio_core import MstrConfig, MstrRestSession
from mstrio.datasets import OlapCube

config = MstrConfig()

with MstrRestSession(config) as session:
    session.set_project(project_id=config.project_id)

    conn = session.mstrio_conn          # live Connection, already authenticated
    cube = OlapCube(conn, id=dataset_id)
    cube.publish()

    r = session.get("/reports/" + guid)  # raw REST on the same session
```

### mstrio-py connection (standalone — no raw REST needed)

```python
from mstrio_core import get_mstrio_connection

conn = get_mstrio_connection()                          # standard auth (env vars)
conn = get_mstrio_connection(workstation_data=wd)       # Workstation auth
```

### Output helpers

```python
from mstrio_core import write_csv, write_excel, read_excel, object_location

# CSV (semicolon-delimited)
write_csv(rows, columns=["GUID", "Name", "Location"], path=config.output_dir / "reports.csv")

# Excel
write_excel(rows, columns=["GUID", "Name"], path=config.output_dir / "reports.xlsx")
df = read_excel(config.output_dir / "input.xlsx")

# Folder path from ancestors list (from REST API includeAncestors=true)
location = object_location(search_result["ancestors"])  # → "/Shared Reports/Finance"
```

### Search & folder utilities

```python
from mstrio_core import (
    PredefinedFolder,
    OBJECT_TYPE_MAP, OBJECT_TYPE_ID_MAP, OBJECT_TYPE_CATEGORY,
    folder_contents, folder_path_to_guid,
    get_predefined_folder, get_object_type_info,
)

with MstrRestSession(config) as session:
    session.set_project(project_id=config.project_id)

    # List items in a folder (all types)
    items = folder_contents(session, folder_id="ABC123")

    # List only reports (type=3) in a folder, paginated
    reports = folder_contents(session, folder_id="ABC123", object_type=3, limit=200)

    # Resolve a backslash-delimited folder path → GUID
    guid = folder_path_to_guid(session, r"Shared Reports\Finance\Monthly")

    # Resolve a predefined system folder → GUID (three equivalent forms)
    guid = get_predefined_folder(session, PredefinedFolder.PUBLIC_REPORTS)
    guid = get_predefined_folder(session, "PUBLIC_REPORTS")
    guid = get_predefined_folder(session, 7)

    # With ancestors for breadcrumb navigation
    guid = get_predefined_folder(session, PredefinedFolder.PUBLIC_METRICS, include_ancestors=True)

    # Look up type/subtype/exttype for any object GUID
    info = get_object_type_info(session, object_id="DEF456")
    print(info["object_type_name"])     # e.g. "REPORT_DEFINITION"
    print(info["object_subtype_name"])  # e.g. "REPORT_GRID"
    print(info["status_code"])          # 200 on success, negative on error

# Reference dicts (no session needed)
type_id = OBJECT_TYPE_MAP["metric"]           # → 4
type_name = OBJECT_TYPE_ID_MAP[4]             # → "Metric"
category = OBJECT_TYPE_CATEGORY[4]            # → "PublicObject"
```

**Predefined folder names** (use with `get_predefined_folder`):
`PUBLIC_OBJECTS`, `PUBLIC_FILTERS`, `PUBLIC_METRICS`, `PUBLIC_PROMPTS`, `PUBLIC_REPORTS`,
`PUBLIC_TEMPLATES`, `SCHEMA_OBJECTS`, `SCHEMA_ATTRIBUTES`, `SCHEMA_FACTS`,
`SCHEMA_HIERARCHIES`, `SCHEMA_TABLES`, `ROOT`, `SYSTEM_MD_SECURITY_FILTERS`

### Module structure

```
mstrio_core/
├── __init__.py        # public exports
├── config.py          # MstrConfig + MstrEnvironment + LoginMode (env var → dataclass)
├── connection.py      # MstrRestSession + get_mstrio_connection()
├── output.py          # write_csv, write_excel, read_excel, object_location
├── search.py          # folder_contents, folder_path_to_guid, get_predefined_folder,
│                      # get_object_type_info, PredefinedFolder, OBJECT_TYPE_* dicts
└── logging_setup.py   # setup_logging() via loguru
```

## Legacy Connection Patterns (existing scripts only)

These patterns exist in older scripts. New scripts should use `mstrio_core` above.

### mstrio-py (workstation-based)
```python
from mstrio.connection import get_connection
conn = get_connection(workstationData)
```

### REST API (manual token-based — legacy)
```python
def login(baseURL, username, password):
    header = {'username': username, 'password': password, 'loginMode': 1}
    r = requests.post(baseURL + '/auth/login', data=header)
    authToken = r.headers["x-mstr-authtoken"]
    cookies = dict(r.cookies)
    headers_svr = {
        'X-MSTR-AuthToken': authToken,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    return authToken, cookies, headers_svr
```

### Changeset pattern (legacy — use `session.changeset()` in new scripts)
```python
changeset = requests.post(api_url + '/model/changesets?schemaEdit=false', headers=headers_prj, cookies=cookies)
changesetId = jq.search('id', changeset.json())
headers_chgset = {**headers_prj, 'X-MSTR-MS-Changeset': changesetId}
# ... PUT operations ...
requests.post(api_url + f'/model/changesets/{changesetId}/commit', headers=headers_chgset, cookies=cookies)
```

## Logging Standard (loguru)

All scripts should use `loguru` for structured logging. Do not use bare `print()` for operational messages.

```python
from loguru import logger

# Standard setup
logger.add("logs/{time:YYYY-MM-DD}.log", rotation="1 day", retention="30 days", level="DEBUG")

# Usage
logger.info("Connected to {env}", env=baseURL)
logger.debug("Response: {status} {reason}", status=r.status_code, reason=r.reason)
logger.warning("No objects found for search: {term}", term=searchTerm)
logger.error("HTTP {status}: {msg}", status=r.status_code, msg=r.text)
logger.success("Exported {count} records to {file}", count=len(rows), file=output_path)
```

- Use structured keyword arguments, not f-strings or `.format()` in log calls
- Log HTTP status for every REST API call
- Log record counts on successful exports
- Log object GUIDs alongside names for traceability

## Script Conventions

- Each script handles **one task** — keep scripts focused
- Hardcoded configuration (URLs, project GUIDs, output paths) goes at the **top of the file** in clearly named variables
- Use `jmespath` (imported as `jq`) for navigating JSON responses: `jq.search('path.to.field', response_json)`
- Output files default to `c:/tmp/` — make the path configurable at the top of the script
- Credentials must **never** be committed to the repo — use environment variables or prompt at runtime

## Common mstrio-py Imports

```python
from mstrio.connection import get_connection, Connection
from mstrio.server import Environment, Project
from mstrio.object_management import (
    full_search, list_objects, SearchObject, SearchPattern,
    SearchResultsFormat, ObjectTypes
)
from mstrio.users_and_groups import list_users, User, UserGroup
from mstrio.types import ObjectTypes, ObjectSubTypes
```

## Current Scripts

| Script | Method | Purpose |
|--------|--------|---------|
| `mstrDocumentObjectDefinition.py` | REST API + mstrio_core | **Pattern script** — extract Dossier definitions → CSV + PA dataset |
| `ListProjects.py` | mstrio-py | List loaded projects as DataFrame |
| `add_trustedauth_internal_users.py` | mstrio-py | Add trusted auth to users (SAML migration) |
| `UpdateProjectSettings.py` | mstrio-py | Configure project-level settings |
| `SearchDependents.py` | mstrio-py | Find object dependents (transformations) |
| `MIgrationPackageInfo.py` | REST API | Query migration package details |
| `GetReportDefs_OutputCSV_SearchObject.py` | REST API | Extract report definitions → CSV |
| `get_MetricDef_SearchResultsObject_folderPathFunction_RestAPI_CSVOutput.py` | REST API | Extract metric definitions → CSV |
| `Addto_ContentGroup_RestAPI_ReportData_JSON.py` | REST API | Add dossiers to content groups |
| `ShortcutCreateFromExcel.py` | REST API | Create shortcuts from Excel GUID list |
| `UpdateMetric_ReportDataSource_ChangeSet.py` | REST API + Changeset | Update metric data source via changeset |
| `UsersExport.py` | mstrio-py + mstrio_core | Export all users → CSV (GUID, login ID, trust ID, group membership JSON) |
| `ServerSettingsCompare.py` | mstrio-py + mstrio_core | Compare, export, or apply I-Server settings across environments (dev/qa/prod) |
| `SchedulesExpire.py` | mstrio-py + mstrio_core | Set stop_date=today on schedules with no stop_date or a future stop_date; uses `full_search(domain=CONFIGURATION, object_types=51)` to retrieve all schedules including hidden; uses `list_related_subscriptions(to_dictionary=True)` to count subscriptions (active/inactive when API exposes it); renames zero-subscription orphans to "DEPRECATE-" prefix |
| `SchedulesActivate.py` | mstrio-py + mstrio_core | Clear stop_date for schedules whose stop_date falls within a given date range (YYYY-MM-DD to YYYY-MM-DD, inclusive); uses `full_search(domain=CONFIGURATION, object_types=51)` to retrieve all schedules including hidden; optionally restores "DEPRECATE-" prefixed names set by SchedulesExpire.py |
| `UserGroups.py` | mstrio-py + mstrio_core | Audit, export, and document user groups. Subcommands: `audit` (empty groups + directly-assigned privileges → separate files), `export` (all groups with members + privileges), `privileges` (direct privileges only, inherited excluded), `members` (direct members; `--resolve` for recursive effective-user expansion). All subcommands support `--format csv\|json`. |
| `DatabaseInstances.py` | mstrio-py + mstrio_core | Export all database instance definitions → CSV: instance name/GUID, DBMS type (name + GUID), datasource connection (name, GUID, connection string / ODBC DSN), and default database login (name, GUID, username). Server-level; no project required. |
| `ProjectDuplicate.py` | REST API + mstrio-py + mstrio_core | Duplicate a project within or across environments (dev/qa/prod). Uses REST API `POST/PUT /api/projectDuplications` for duplication execution; mstrio-py `Project` for name/ID resolution only. Reads parameters from a YAML config file (`project_duplicate_config.yaml`). Supports same-environment and cross-environment (two-phase export→import) duplication with async status polling. Dry-run by default; `--apply` to execute. |
| `SecurityRoles.py` | mstrio-py + mstrio_core | Export and compare security role privileges. Subcommands: `list-all` (export privileges for **every** role on an environment — enabled only by default, `--all` for full list with enabled/disabled status; output: `security_roles_{env}_{suffix}.csv`), `export` (same output for a single named role), `compare` (diff privileges between two roles on same or different environments — differences only by default, `--all` for full comparison with match status). Uses `SecurityRole`, `Privilege.list_privileges()` for the master catalog, and `list_security_roles()` for name resolution. Supports `--format csv\|json`. |
| `ProjectSecurityCompare.py` | mstrio-py + mstrio_core | Compare project-level security between two projects (same or different environments). Subcommands: `roles` (diff security role assignments — shows members in source only, target only, or with different roles), `filters` (diff security filter assignments — shows filter+member pairs in source only or target only), `apply-roles` (read a roles diff CSV and grant/revoke role assignments on the target), `apply-filters` (read a filters diff CSV and apply/revoke filter assignments on the target). CSV output includes `target_action` column (Apply/Remove) for review before applying. Dry-run by default; `--apply` to execute. Uses `SecurityRole.list_members(project_name=)`, `SecurityRole.grant_to()`/`revoke_from()`, `list_security_filters(project_name=)`, `SecurityFilter.members`, `SecurityFilter.apply()`/`revoke()`. Supports `--format csv\|json`. |
| `LogicalTables.py` | mstrio-py + REST API + mstrio_core | Export and compare logical table definitions. Subcommands: `export` (document all logical tables in a project — table metadata, physical table, logical size, plus mapped attributes/facts with key indicators; output: `*_tables.csv` + `*_objects.csv`, or nested JSON, or multi-sheet Excel), `compare` (diff tables between two projects — checks `is_logical_size_locked`, `logical_size` at table level and object membership + `is_key` at attribute level; differences only by default, `--all` for full view). Uses `list_logical_tables()` and `LogicalTable` from `mstrio.modeling.schema`; `table_key` property determines `is_key` for attributes. Falls back to REST API (`GET /searches/results?type=15` + `GET /model/tables/{id}`) when mstrio-py fails (older I-Server versions). Supports `--format csv\|json\|excel`. |
| `DatabaseInstanceVLDB.py` | mstrio-py + mstrio_core | Export and modify VLDB settings on database instances. Subcommands: `export` (document VLDB settings for a single `--instance` or all database instances — non-default only by default, `--all` for every setting; output includes property set, group, setting name/display name, value, default value, is_default, resolved location, is_inherited), `alter` (change a VLDB setting on a single `--instance` or multiple instances via `--csv` with an `instance_id` column; shows old/new values and default status). Uses `DatasourceInstance.vldb_settings`, `DatasourceInstance.alter_vldb_settings()`. Dry-run by default; `--apply` to execute. Supports `--format csv\|json`. |
| `ReportVLDBCompare.py` | mstrio-py + mstrio_core | Compare VLDB settings between two reports (same or different environments). Subcommands: `compare` (diff VLDB property values between a source and target report — differences only by default, `--all` for full view with match status; includes default status for every setting), `export` (dump all VLDB settings for a single report). Uses `Report(connection, id=).vldb_settings` from `mstrio.project_objects.report`. Cross-environment support with `--src-project` / `--tgt-project` overrides. Supports `--format csv\|json`. |
| `ContentGroupAdd.py` | mstrio-py + REST API + mstrio_core | Add objects to a content group. Subcommands: `csv` (read GUIDs from a CSV file with a `GUID` column — additional columns ignored; resolves each GUID via `get_object_type_info()` for name/type), `folder` (read all non-hidden, non-folder contents from a folder GUID; resolves shortcuts to their target objects via `GET /objects/{id}?type=18`). Uses `ContentGroup.update_contents(content_to_add=[...])` with `Dashboard`, `Document`, `Report` objects. Dry-run by default; `--apply` to execute. Content group specified by name or GUID. Writes results CSV with object GUID, name, type, source, and status. |
| `SecurityRoleMembers.py` | mstrio-py + mstrio_core | Export and manage security role member assignments across projects. Subcommands: `export` (list all role→project→member assignments; filter by `--role-id` and/or `--project-id`; `--format csv\|json`), `remove-all` (revoke every member from specified roles/projects), `add` (grant assignments from CSV/Excel), `remove` (revoke assignments from CSV/Excel). File-based commands require `role_id`, `project_id`, plus a member identifier: `user_id` (always User), `user_group_id` (always UserGroup), or `member_id` + `is_group` (generic). Per-row priority: user_id → user_group_id → member_id. Uses `SecurityRole.list_members(project_name=)`, `SecurityRole.grant_to([member], project=)`, `SecurityRole.revoke_from([member], project=)`. CSV delimiter auto-detected with comma fallback; Excel (.xlsx) via `read_excel()`. Dry-run by default; `--apply` to execute. |
| `SecurityRoleEveryoneRemove.py` | mstrio-py + mstrio_core | Remove a user group (default: "Everyone") from all security role assignments across every loaded project. Scans each project × each security role for the target group; revokes any found assignments. Uses `UserGroup(conn, name=)`, `list_security_roles()`, `Environment.list_projects()`, `SecurityRole.list_members(project_name=)`, `SecurityRole.revoke_from([group], project=)`. `--group NAME` to target a different group. Dry-run by default; `--apply` to execute. |
| `StandardAuthManage.py` | mstrio-py + REST API + mstrio_core | Manage standard authentication based on user group membership. Disables `User.standard_auth` for users NOT in an excepted user group (default: "Function Access: Standard Authentication"); group specified by GUID in script-specific `StandardAuthManage.env` (`STANDARD_AUTH_GROUP_ID`), overridable via `--group-id`. Resolves flat (recursive) membership via REST API `GET /usergroups/{id}/members?flatMembers=true`. `list_users()` returns lightweight objects without `standard_auth`; each user is fetched individually via `User(conn, id=)` then altered via `User.alter(standard_auth=)`. Fetch and apply phases use `ThreadPoolExecutor`; concurrency set via `CONCURRENCY` in `.env` (default 10) or `--concurrency`. `--enabled-only` uses `list_users(conn, enabled=True)` API filter. `--enable-excepted` to also set `standard_auth=True` for group members. Tracks last-run timestamp per environment in `StandardAuthManage.env` (`LAST_RUN_DEV`/`QA`/`PROD`); `--since-last-run` filters to recently modified users (default: all users, since migrated users may retain source-environment timestamps). Dry-run by default; `--apply` to execute. |
| `UserGroupMemberManage.py` | mstrio-py + mstrio_core | Bulk add or remove users from user groups. Subcommands: `add` (add users to groups), `remove` (remove users from groups). Input via `--users LOGIN_OR_ID [...]`, `--csv PATH`, or `--excel PATH` (mutually exclusive); groups via `--group NAME_OR_ID [...]` (required with `--users`; optional with `--csv`/`--excel` if file has a group column). Auto-detects GUID vs login (32-hex = GUID, else username). Fetches all users once via `list_users()` for in-memory lookup; resolves groups via `UserGroup(conn, id=)` or `UserGroup(conn, name=)`. CSV accepts flexible column names (user/login/username/user_id/id/guid for users; group_id/group/user_group/user_group_id for groups); delimiter auto-detected with comma fallback for Excel "Save As CSV". Excel (.xlsx) via `read_excel()`. Deduplicates (user, group) pairs. Uses `UserGroup.add_users([user])` / `UserGroup.remove_users([user])` with `ThreadPoolExecutor` for concurrent execution; `--concurrency` (default 10). Dry-run by default; `--apply` to execute. |
| `AttributeFormHtmlManage.py` | REST API + mstrio_core | Find and migrate HTML / HTML Tag AttributeForm expressions from the legacy `?evt=3140&documentID=...` URL pattern to a modern relative `?prompts=...` JSON URL. Subcommands: `export` (scan every loaded project or `--project GUID ...` for attributes whose forms have an HTML expression, optional `--modified-since YYYY-MM-DD`; for each match parses out `documentID`, the source attribute id from `elementsPromptAnswers=`, the `ToString(...)` value form, the `title=""",...,"""` display form; looks up the matching prompt on the target document via `GET /v2/documents/{id}/prompts` → falls back to `GET /documents/{id}/prompts?closed=false` matching `source.id == elementTargetAttributeID`; builds a suggested `NewFormExpression` using `Concat("<a title=""",NAME,""" href=""../{TargetDocID}?prompts=<URL_ENCODED_JSON>"" target=""_blank"">",NAME,"</a>")` with the JSON shape `[[{"key":"...","values":["ATTRID:<ToString(value)>"],"useDefault":false}]]`; uses relative URL `../{docID}` so the browser keeps host/app/project context; leaves `NewFormExpression` blank when any piece is missing), `apply` (re-reads the CSV, for every row with `NewFormExpression` populated opens a schema changeset `POST /model/changesets?schemaEdit=true`, GETs `/model/attributes/{id}`, locates the form by id, replaces the HTML expression's text (drops cached `tree`/`tokens`), PUTs back, commits; rolls back changeset on error; dry-run by default, `--apply` to commit). CSV columns (PascalCase per spec): ProjectID, ProjectName, AttributeID, AttributeName, AttributeLocation, AttributeFormName, AttributeFormID, AttributeFormExpression, AttributeFormTargetDocumentID, AttributeFormElementTargetAttributeID, AttributeFormTargetValueID, AttributeFormTargetValueName, AttributeFormTargetDocumentPromptID, AttributeFormTargetDocumentPromptKey, NewFormExpression. CSV read uses case-insensitive header matching. HTML detection: form expression's `displayFormat` in {HTML_TAG, HTML, URL} OR text matches `<a\s|<img\s|<iframe|href=`. Concurrent attribute fetches via `ThreadPoolExecutor` (`--concurrency`, default 10). Single shared `MstrRestSession` with `X-MSTR-ProjectID` header per request to avoid `set_project()` races. |
| `ExecuteObjects.py` | REST API + mstrio-py + mstrio_core | Concurrently execute MicroStrategy objects (Reports, Documents, Dossiers, Intelligent Cubes) for testing/validation. Two-phase: (1) **pre-flight** reads input CSV (`project_id`, `object_id`, optional `prompt_answers_json`), resolves name/type/subtype/folder location via `get_object_type_info()` + `GET /objects/{id}?type=&includeAncestors=true`, classifies as Report/Document/Dossier/Cube, fetches prompt definitions via kind-specific path (`/v2/reports\|/v2/dossiers\|/v2/documents/{id}/prompts`) using raw `session._session.get()` to suppress expected 404-warning noise (objects without prompts return 404), and writes a JSON template to `prompt_answers_json` (or `Prompts:None`); (2) **execute** schedules every non-success row on a `ThreadPoolExecutor` (default 10, `--concurrency`). Worker creates an instance via kind-specific endpoint: `POST /v2/reports/{id}/instances` for reports, `POST /v2/dossiers/{id}/instances` for dossiers (NOT `/documents/` — returns 404 even though dossiers have metadata type=55), `POST /v2/documents/{id}/instances` for documents; if prompts are pending, sends `closeAllPrompts:true` (uses defaults) merged with any user-supplied answers from `prompt_answers_json` (via `PUT /reports\|documents/{id}/instances/{iid}/prompts/answers`); cubes execute via `OlapCube(conn, id=).publish()` with REST fallback. CSV updated atomically (temp + rename) on every state change so the file is a live progress snapshot. **Resume mode**: re-feeding the output CSV preserves `success` rows untouched and re-runs `error`/`running`/empty rows. Concurrency-safe by passing `X-MSTR-ProjectID` header per-request rather than calling `session.set_project()` in workers. CSV columns: `project_id`, `project_name`, `object_id`, `object_name`, `object_location`, `object_type`, `start_time`, `end_time`, `status`, `status_details`, `prompt_answers_json`. Status values: empty, `running`, `success`, `error`, `skipped`. `--preflight-only` to gather definitions + prompt templates without executing. |
| `SystemManager_ClearCache.py` | REST API + mstrio-py + mstrio_core | Python conversion of the legacy `MarketIntelligence_ClearCache` System Manager workflow, designed for the MicroStrategy Cloud Environment (MCE) Python runtime. Steps: (1) `CheckIfMIDWHRefreshHappened` → exits cleanly if no refresh outstanding; (2) `RetrieveIDs` → comma-separated `MIDWHLoadHistoryIds`; (3) Invalidate all report caches in the project; (4) `EXEC IndicateCacheDropHappened @ids`; (5) Trigger `Refresh-Cubes` event; (6) Trigger `NotifyClearCache` event (best-effort). Reads YAML config (`system_manager_clear_cache_config.yaml`) — supports `--config PATH` so the same script template runs against multiple projects/databases by swapping report GUIDs and event names. SQL is executed via **pre-created Freeform SQL reports** (no `pyodbc`/`pymssql` needed, MSTR datasource manages DB credentials), called through REST API `POST /v2/reports/{id}/instances` + `PUT /reports/{id}/instances/{iid}/prompts/answers` for the prompt-driven `IndicateCacheDropHappened` wrapper. Cache invalidation uses `ContentCache.list_caches(conn, project_id=, status="ready")` + `ContentCache.invalidate_caches(conn, cache_ids=[...])` from `mstrio.project_objects.content_cache` (mstrio-py 11.4+) — `status="ready"` skips already-invalid caches; one bulk call replaces per-cache PATCHes. Events fired via `mstrio.distribution_services.Event(conn, name=).trigger()`. Failure email via `smtplib` (high-importance) with graceful logging fallback when MCE outbound SMTP is blocked. Dry-run by default; `--apply` to execute mutating steps (cache invalidate, SP, event triggers). |
| `ACE_Folder.py` | mstrio-py + mstrio_core | Audit and synchronize **folder ACLs** without impacting underlying objects (always `inheritable=False`, never propagates to children). Subcommands: `export` (all folders via `full_search(object_types=8)`, excluding profile folders — any folder whose name or ancestor name is "Profiles", overridable with `--include-profiles`; one row per ACE: folder GUID/name, full path from `ancestors` excluding project root + own name, trustee id/name/type/subtype, deny, rights int + decoded `rights_names`, inheritable; `--format csv\|excel`; parallel `Folder(conn,id=)` fetch for `.acl`+`.ancestors`), `copy` (`--source-folder FID --targets CSV/XLSX` with a folder-id column → full-sync each target to the source ACL: `acl_alter` REPLACE each source ACE + `acl_remove` target trustees not on source), `apply` (`--input CSV/XLSX` ACE rows: folder_id, trustee_id, rights[int], deny, inheritable, remove; `remove`=true removes that trustee entry; `--remove-unlisted` also removes folder trustees absent from input). ACL ops use `Folder.acl_alter` (op REPLACE/upsert), `acl_remove` (op REMOVE); trustees passed as **id strings** (no User/UserGroup objects needed); `rights` is the int bitmask (255=Full Control; flags BROWSE=1/USE_EXECUTE=2/READ=4/WRITE=8/DELETE=16/CONTROL=32/USE=64/EXECUTE=128). Dry-run by default for copy/apply; `--apply` to execute. `--project` accepts GUID or name. Writes a per-op results CSV (folder_id, trustee_id, deny, rights, action, status, details). |
| `SubscriptionFileMigration.py` | mstrio-py + mstrio_core | **One-time** orchestrator migrating file-subscription delivery from on-prem UNC paths to the MCE local dir (`/opt/mstr/ContainerState/FileSubscriptions`, synced onward by GoAnywhere). Driven by `subscription_file_path_mapping.xlsx` (sheets: `DistinctSubscriptionLocations` = mapping driver with Device/Distinct Subscription Locations/GoAnywhereSyncBase/DeviceBase/SubscriptionsUserAppend/NewDevice; `subscriptions_export_dev`; `Devices`; `UserAddresses`). Selectable steps (`--steps all` or comma list, canonical order enforced): `validate` (read-only pre-flight — flags every active FILE-subscription `(device_name, normalized path)` NOT covered by `DistinctSubscriptionLocations` so it would deliver to a rewritten path without the correct append; also flags blank-physical_address FILE recipients; surfaces only problems), `update-devices` (existing mapped device `file_path = LINUX_BASE + DeviceBase`), `create-devices` (new devices clone `FileDeviceProperties` from the `Device`-column source device via `from_dict(to_dict())`, override file_path), `update-addresses` (re-point matched addresses to target device + set `physical_address = SubscriptionsUserAppend` via `User.update_address(id=, address=, device_id=)`; subscriptions follow since they reference address_id; match key = (device_name, normalized physical_address) from subscriptions_export FILE rows), `clean-addresses` (remove **FILE-delivery** addresses (`delivery_type=='file'`) whose address_id ∉ workbook used-set; scans all users in parallel; email/FTP/etc. addresses never touched), `clean-devices` (remove **FILE** devices (`deviceType=='file'`) whose device_id ∉ workbook used-set, protecting all mapping target/clone-source names; email/FTP/printer devices never touched), `admin-subs` (validate Administrator subs vs target by name/project; recreate missing under user `subscription_admin` BFF662B4C6451F461A754CAEC51B1036 keeping original recipients. **Source = the comprehensive JSON from `SubscriptionsExport.py <env> --format json`** via `--subscriptions-json PATH` — if omitted, the script `input()`-prompts for the path. Builds `Content.from_dict`/`Delivery.from_dict` from the JSON and calls EmailSubscription/FileSubscription/FTPSubscription/HistoryListSubscription/CacheUpdateSubscription `.create`; then **replays captured prompt answers** via `subscription.answer_prompts([Prompt(connection, id, key, type, answers, use_default)], force=True)` built from the JSON `prompts[].key/.answers` — prompts with no saved answer use_default), `json-map` (device→GoAnywhere JSON grouped by SyncBase). Key decisions: device path = `LINUX_BASE + DeviceBase` only (SyncBase is JSON grouping, NOT in path); used-set from workbook not live target. Writes a per-step dry-run/apply Excel report (`subscription_migration_{mode}_{env}_{ts}.xlsx`, one sheet per step + Summary) in both modes. Dry-run by default; `--apply` to execute. `device delete force=True`; `list_devices(to_dictionary=True)` to dodge `DeviceType` enum errors (e.g. 'gcs'). |
| `UserAddressUpdate.py` | mstrio-py + mstrio_core | Add new or update existing ContactAddresses for MicroStrategy users from an Excel file. Input columns: UserID, Name, Physical Address, Delivery Type (Email/File/FTP/…), Device (name). Pre-loads all devices via `list_devices()` to build a name→ID lookup; fetches each unique UserID via `User(conn, id=)` in parallel (`ThreadPoolExecutor`). Matching key: (name_lower, device_id) against existing `user.addresses`. 0 matches → `add_address(name, address, default=False, delivery_type, device_id)`; 1 match + same path → skip; 1 match + different path → `update_address(id, address)`; 2+ matches → ambiguous, skip. `delivery_type` mapped: Email→`email`, File→`file`, FTP→`ftp`, Unknown→`unsupported`. Address index rebuilt after each write so subsequent rows for the same user see fresh state. Results CSV: user_id, name, physical_address, delivery_type, device, status (added/updated/skipped/ambiguous/error/dry-run), status_details. Dry-run by default; `--apply` to commit. |
| `SubscriptionsExport.py` | mstrio-py + mstrio_core | Export all subscription information across every loaded project. One row per recipient. Uses `list_subscriptions(conn, project_id=, last_run=True, to_dictionary=True)` per project; iterates `Environment.list_projects()`. Flattens the recipients list so each row is one subscription × one recipient. Columns: project_id/name, subscription_id/name, delivery_mode, delivery_filename (filename template), email_subject, schedule_ids/names, content_ids/names/types, owner_id/name, date_created, date_modified, last_run (requires server ≥ 11.4.0600), recipient_id/name/type/include_type, address_id/address_name (from `addressId`/`addressName` in each recipient dict), physical_address + device_id/device_name (resolved via `User(conn, id=).addresses` lookup — `ContactAddress.device.id`/`.name`; email address or file path; blank for group recipients; `device_id` is the join key to the device→GoAnywhere mapping). Three-pass: (1) collect all subscription dicts + build set of unique recipient user IDs; (2) resolve `User.addresses` in parallel via `ThreadPoolExecutor` into `{user_id: {addr_id: {physical_address, device_id, device_name}}}`; (3) flatten to rows using address lookup. `--concurrency N` (default 10). Supports `--format csv\|json` and `--project-id GUID [...]` to limit scope. CSV is semicolon-delimited per project convention. **`--format json` is COMPREHENSIVE** (diverges from the flat CSV): one full nested object per subscription (raw definition: contents+personalization, delivery, recipients, schedules), each non-group recipient enriched with resolved physical_address/device_id/device_name, and for prompted subscriptions the prompt definitions + **saved answers** fetched from `get_subscription_prompts(conn, subscription_id=, project_id=)` (`/subscriptions/{id}/prompts`) and attached as `sub['prompts']`. Only prompted subs (any content `personalization.prompt.enabled`) trigger the extra call, fetched in parallel. The per-prompt `key` + `answers` (element IDs like `h2022;ATTRID`) are exactly what a recreation step needs to re-apply answers on a target env. |

## Known Gaps / Improvement Areas

- Existing scripts not yet migrated to `mstrio_core` — still use inline `login()` and manual headers
- `MIgrationPackageInfo.py` contains plaintext credentials — must be moved to `.env`
- No error handling in existing scripts — add `try/except` with `logger.error()` for HTTP failures
- `SearchDependents.py` uses `Connection(base_url, user, pwd)` directly — migrate to `get_mstrio_connection()`
