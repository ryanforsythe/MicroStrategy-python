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
| `SecurityRoleEveryoneRemove.py` | mstrio-py + mstrio_core | Remove a user group (default: "Everyone") from all security role assignments across every loaded project. Scans each project × each security role for the target group; revokes any found assignments. Uses `UserGroup(conn, name=)`, `list_security_roles()`, `Environment.list_projects()`, `SecurityRole.list_members(project_name=)`, `SecurityRole.revoke_from([group], project=)`. `--group NAME` to target a different group. Dry-run by default; `--apply` to execute. |
| `StandardAuthManage.py` | mstrio-py + REST API + mstrio_core | Manage standard authentication based on user group membership. Disables `User.standard_auth` for users NOT in an excepted user group (default: "Function Access: Standard Authentication"); group specified by GUID in script-specific `StandardAuthManage.env` (`STANDARD_AUTH_GROUP_ID`), overridable via `--group-id`. Resolves flat (recursive) membership via REST API `GET /usergroups/{id}/members?flatMembers=true`. `list_users()` returns lightweight objects without `standard_auth`; each user is fetched individually via `User(conn, id=)` then altered via `User.alter(standard_auth=)`. Fetch and apply phases use `ThreadPoolExecutor`; concurrency set via `CONCURRENCY` in `.env` (default 10) or `--concurrency`. `--enabled-only` uses `list_users(conn, enabled=True)` API filter. `--enable-excepted` to also set `standard_auth=True` for group members. Tracks last-run timestamp per environment in `StandardAuthManage.env` (`LAST_RUN_DEV`/`QA`/`PROD`); `--since-last-run` filters to recently modified users (default: all users, since migrated users may retain source-environment timestamps). Dry-run by default; `--apply` to execute. |

## Known Gaps / Improvement Areas

- Existing scripts not yet migrated to `mstrio_core` — still use inline `login()` and manual headers
- `MIgrationPackageInfo.py` contains plaintext credentials — must be moved to `.env`
- No error handling in existing scripts — add `try/except` with `logger.error()` for HTTP failures
- `SearchDependents.py` uses `Connection(base_url, user, pwd)` directly — migrate to `get_mstrio_connection()`
