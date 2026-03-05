# MicroStrategy Python Management Scripts — Usage Guide

A collection of Python utilities for managing the MicroStrategy I-Server: user
administration, schedule management, server configuration comparison, metadata
extraction, and content-group operations.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Configuration](#configuration)
   - [Single-environment setup](#single-environment-setup)
   - [Multi-environment setup](#multi-environment-setup)
   - [Credential storage options](#credential-storage-options)
   - [Environment variable reference](#environment-variable-reference)
4. [CLI Scripts](#cli-scripts)
   - [UsersExport.py](#usersexportpy)
   - [SchedulesExpire.py](#schedulesexpirepy)
   - [SchedulesActivate.py](#schedulesactivatepy)
   - [ServerSettingsCompare.py](#serversettingscomparepy)
   - [UserGroups.py](#usergroupspy)
5. [Legacy Scripts](#legacy-scripts)
6. [Output Files](#output-files)
7. [Logging](#logging)

---

## Prerequisites

- **Python 3.9 or higher**
- Network access to your MicroStrategy Library server(s)
- A MicroStrategy account with sufficient privileges for the operations you intend to run

---

## Installation

```bash
# 1. Clone / download the repository
git clone <repo-url>
cd Python-MicroStrategy-Git

# 2. Install dependencies
pip install -r requirements.txt

# 3. Create your .env file
cp .env.example .env
# Then open .env and fill in your values (see Configuration below)
```

---

## Configuration

All scripts read credentials and settings from environment variables.
The recommended approach is a `.env` file in the project root — it is
git-ignored and never committed.

### Single-environment setup

If you work against a single MicroStrategy environment, fill in only the bare
`MSTR_*` variables and leave `MSTR_ENV=dev`:

```ini
# .env
MSTR_ENV=dev

MSTR_BASE_URL=https://yourserver.cloud.microstrategy.com/MicroStrategyLibrary
MSTR_USERNAME=your.username@company.com
MSTR_PASSWORD=YourPassword123

MSTR_OUTPUT_DIR=c:/tmp
```

### Multi-environment setup

Define per-environment URLs and credentials, then switch environments by
changing a single variable — no code changes required:

```ini
# .env
MSTR_ENV=qa                         # set once; change to dev / qa / prod to switch

# Dev
MSTR_DEV_BASE_URL=https://dev.cloud.microstrategy.com/MicroStrategyLibrary
MSTR_DEV_USERNAME=your.username@company.com
MSTR_DEV_PASSWORD=

# QA
MSTR_QA_BASE_URL=https://qa.cloud.microstrategy.com/MicroStrategyLibrary
MSTR_QA_USERNAME=your.username@company.com
MSTR_QA_PASSWORD=

# Prod
MSTR_PROD_BASE_URL=https://prod.cloud.microstrategy.com/MicroStrategyLibrary
MSTR_PROD_USERNAME=your.username@company.com
MSTR_PROD_PASSWORD=
```

**Resolution order** for every setting:
```
MSTR_{ENV}_{SETTING}  →  MSTR_{SETTING}  →  built-in default
```
For example, when `MSTR_ENV=qa`, `MSTR_QA_BASE_URL` is used before
`MSTR_BASE_URL`.

The CLI scripts (`UsersExport.py`, `SchedulesExpire.py`, `SchedulesActivate.py`,
`ServerSettingsCompare.py`) override `MSTR_ENV` at runtime by constructing
`MstrConfig(environment=MstrEnvironment(env))` from the environment argument
you pass on the command line — so the `.env`-level `MSTR_ENV` does not need
to match for those scripts.

### Credential storage options

Passwords can be kept out of `.env` entirely using your OS credential store
(Windows Credential Manager, macOS Keychain, or SecretService on Linux).
Leave the `*_PASSWORD` variable blank or absent and store the password once:

```bash
# Single-env or shared fallback
python -m keyring set mstrio your.username@company.com

# Per-environment (recommended for multi-env setups)
python -m keyring set mstrio-dev  your.username@company.com
python -m keyring set mstrio-qa   your.username@company.com
python -m keyring set mstrio-prod your.username@company.com
```

Verify a stored password:

```bash
python -m keyring get mstrio your.username@company.com
```

### Environment variable reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `MSTR_ENV` | No | `dev` | Active environment: `dev`, `qa`, or `prod` |
| `MSTR_BASE_URL` | Yes* | — | Full Library URL, e.g. `https://server.cloud.microstrategy.com/MicroStrategyLibrary` |
| `MSTR_USERNAME` | Yes* | — | Login username |
| `MSTR_PASSWORD` | Yes* | — | Login password (or use OS keyring — leave blank) |
| `MSTR_LOGIN_MODE` | No | `1` | `1`=Standard, `16`=SAML, `64`=LDAP |
| `MSTR_PROJECT_ID` | No | — | Default project GUID |
| `MSTR_PROJECT_NAME` | No | — | Default project name (fallback if no ID) |
| `MSTR_PA_PROJECT_ID` | No | — | Platform Analytics project GUID |
| `MSTR_PA_DATASET_ID` | No | — | Platform Analytics dataset/cube GUID |
| `MSTR_OUTPUT_DIR` | No | `c:/tmp` | Default output directory for CSV/Excel files |
| `MSTR_LOG_DIR` | No | `logs` | Log file directory |
| `MSTR_LOG_LEVEL` | No | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `MSTR_SSL_VERIFY` | No | `true` | Set `false` for self-signed or internal CA certificates |
| `MSTR_KEYRING_SERVICE` | No | `mstrio` | OS keyring service name |

\* Required, but can be supplied per-environment as `MSTR_{ENV}_{VAR}` instead.

Every variable supports the per-environment prefix:
`MSTR_DEV_*`, `MSTR_QA_*`, `MSTR_PROD_*`.

---

## CLI Scripts

These scripts accept the environment (and any other options) as command-line
arguments. Run any script with `--help` for the full option list.

---

### UsersExport.py

Export all MicroStrategy users from an environment to a CSV file.

**Output columns:** `base_url`, `guid`, `name`, `id` (login username),
`trusted_auth` (SAML trust ID), `group_membership` (JSON array).

#### Usage

```
python UsersExport.py <env> [--output-dir PATH]
```

| Argument | Required | Description |
|---|---|---|
| `env` | Yes | Environment to export: `dev`, `qa`, or `prod` |
| `--output-dir PATH` | No | Directory for the output CSV (default: `MSTR_OUTPUT_DIR` or `c:/tmp`) |

#### Examples

```bash
# Export users from dev (output → c:/tmp/users_export.csv)
python UsersExport.py dev

# Export users from prod to a specific directory
python UsersExport.py prod --output-dir c:/reports
```

#### Output file

`<output-dir>/users_export.csv`

---

### SchedulesExpire.py

Set `stop_date = today` on every schedule whose `stop_date` is either **null**
(runs forever) or **in the future**.  Schedules already expired are left
untouched.

Additionally, if a schedule has **no dependents**, it is renamed to
`DEPRECATE-<original name>` to flag it as an orphan.

**Safety default — dry run:** The script previews changes and writes a CSV
without modifying the server.  Pass `--apply` to commit the changes.

> **Recommended workflow:** run without `--apply` first, review the CSV, then
> re-run with `--apply`.

#### Usage

```
python SchedulesExpire.py <env> [--apply] [--output-dir PATH]
```

| Argument | Required | Description |
|---|---|---|
| `env` | Yes | Environment to process: `dev`, `qa`, or `prod` |
| `--apply` | No | Apply changes to the server (default: dry run — preview only) |
| `--output-dir PATH` | No | Directory for the preview CSV (default: `MSTR_OUTPUT_DIR` or `c:/tmp`) |

#### Examples

```bash
# Step 1 — preview what would change on prod (no server modifications)
python SchedulesExpire.py prod

# Review c:/tmp/expired_schedules.csv, then:

# Step 2 — apply the changes
python SchedulesExpire.py prod --apply

# Dry run against dev with a custom output directory
python SchedulesExpire.py dev --output-dir c:/reports/schedules
```

#### Output file

`<output-dir>/expired_schedules.csv`

**CSV columns:**

| Column | Description |
|---|---|
| `id` | Schedule GUID |
| `name` | Current schedule name |
| `schedule_type` | Schedule type (time-based, event-based, etc.) |
| `current_stop_date` | Existing stop date (blank = no stop date set) |
| `subscription_count` | Total number of subscriptions that reference this schedule |
| `active_subscriptions` | Number of active subscriptions, or `N/A` if the server does not expose that field |
| `inactive_subscriptions` | Number of inactive subscriptions, or `N/A` if the server does not expose that field |
| `actions` | Comma-separated list of changes that will be (or were) applied |

---

### SchedulesActivate.py

Clear the `stop_date` for schedules whose `stop_date` falls within a given
date range, reactivating those schedules so they resume running.  Pairs with
`SchedulesExpire.py` — pass `--restore-name` to also reverse the
`DEPRECATE-` rename applied by that script.

Schedules with no `stop_date`, or whose `stop_date` is outside the range,
are left untouched.

**Safety default — dry run:** The script previews changes and writes a CSV
without modifying the server.  Pass `--apply` to commit the changes.

#### Usage

```
python SchedulesActivate.py <env> <start_date> <end_date>
                            [--apply] [--restore-name] [--output-dir PATH]
```

| Argument | Required | Description |
|---|---|---|
| `env` | Yes | Environment to process: `dev`, `qa`, or `prod` |
| `start_date` | Yes | Earliest `stop_date` to match (YYYY-MM-DD, inclusive) |
| `end_date` | Yes | Latest `stop_date` to match (YYYY-MM-DD, inclusive). Use the same value as `start_date` to target a single date |
| `--apply` | No | Apply changes to the server (default: dry run — preview only) |
| `--restore-name` | No | Strip the `DEPRECATE-` prefix from schedule names when activating |
| `--output-dir PATH` | No | Directory for the output CSV (default: `MSTR_OUTPUT_DIR` or `c:/tmp`) |

#### Examples

```bash
# Preview schedules that expired on or between two dates (no server changes)
python SchedulesActivate.py dev 2025-03-01 2025-03-31

# Target a single specific date
python SchedulesActivate.py dev 2025-03-04 2025-03-04

# Apply — clear stop_date for all matches
python SchedulesActivate.py prod 2025-03-01 2025-03-31 --apply

# Apply and restore DEPRECATE- names at the same time
python SchedulesActivate.py prod 2025-03-01 2025-03-31 --apply --restore-name
```

#### Output file

`<output-dir>/activated_schedules.csv`

**CSV columns:**

| Column | Description |
|---|---|
| `id` | Schedule GUID |
| `name` | Current schedule name |
| `schedule_type` | Schedule type (time-based, event-based, etc.) |
| `current_stop_date` | The stop date that will be cleared |
| `subscription_count` | Total number of subscriptions that reference this schedule |
| `active_subscriptions` | Number of active subscriptions, or `N/A` if the server does not expose that field |
| `inactive_subscriptions` | Number of inactive subscriptions, or `N/A` if the server does not expose that field |
| `actions` | Comma-separated list of changes that will be (or were) applied |

---

### ServerSettingsCompare.py

Compare, export, or apply MicroStrategy I-Server settings across environments.

#### Subcommands

```
python ServerSettingsCompare.py compare <source> <target>  [--format csv|json] [--all]
python ServerSettingsCompare.py export  <env>              [--format csv|json] [--description]
python ServerSettingsCompare.py apply   <source> <target>  [--output-dir PATH]
```

---

#### `compare` — diff settings between two environments

Fetches I-Server settings from both environments and writes the differences to
a file.  By default only rows that differ are written; pass `--all` to include
identical rows as well.

| Argument | Required | Description |
|---|---|---|
| `source` | Yes | Reference environment: `dev`, `qa`, or `prod` |
| `target` | Yes | Environment to compare against the source |
| `--format csv\|json` | No | Output format (default: `csv`) |
| `--all` | No | Include rows where both environments are identical (default: diff only) |
| `--output-dir PATH` | No | Output directory (default: `MSTR_OUTPUT_DIR` or `c:/tmp`) |

```bash
# Compare dev vs prod, CSV output (diff rows only)
python ServerSettingsCompare.py compare dev prod

# Compare dev vs prod, JSON output, include all rows
python ServerSettingsCompare.py compare dev prod --format json --all

# Compare qa vs prod with a custom output directory
python ServerSettingsCompare.py compare qa prod --output-dir c:/reports
```

**Output file:** `<output-dir>/server_settings_diff_<source>_vs_<target>.<fmt>`

---

#### `export` — export settings for one environment

Fetches I-Server settings from a single environment and writes them to a file.

| Argument | Required | Description |
|---|---|---|
| `env` | Yes | Environment to export: `dev`, `qa`, or `prod` |
| `--format csv\|json` | No | Output format (default: `csv`) |
| `--description` | No | Include human-readable setting descriptions in the CSV (CSV only) |
| `--output-dir PATH` | No | Output directory (default: `MSTR_OUTPUT_DIR` or `c:/tmp`) |

```bash
# Export dev settings to CSV
python ServerSettingsCompare.py export dev

# Export QA settings to JSON
python ServerSettingsCompare.py export qa --format json

# Export prod settings with descriptions
python ServerSettingsCompare.py export prod --description
```

**Output file:** `<output-dir>/server_settings_<env>.<fmt>`

---

#### `apply` — push source settings to a target server

Copies I-Server settings from the source environment to the target server.

> **⚠ Warning:** This overwrites settings on the target server.
> A snapshot of the target is saved **before** any changes are made.
> The script prompts for confirmation — type `yes` to proceed.

| Argument | Required | Description |
|---|---|---|
| `source` | Yes | Environment to read settings from |
| `target` | Yes | Environment to overwrite |
| `--output-dir PATH` | No | Directory for the pre-apply snapshot (default: `MSTR_OUTPUT_DIR` or `c:/tmp`) |

```bash
# Copy dev settings to QA (will prompt for confirmation)
python ServerSettingsCompare.py apply dev qa

# Copy dev settings to prod with a custom snapshot directory
python ServerSettingsCompare.py apply dev prod --output-dir c:/reports/snapshots
```

**Files written:**

| File | Description |
|---|---|
| `<output-dir>/server_settings_<target>_BEFORE.csv` | Pre-apply snapshot of the target (audit trail) |

---

### UserGroups.py

Audit, export, and document MicroStrategy user groups.

#### Subcommands

```
python UserGroups.py audit      <env>  [--format csv|json] [--output-dir PATH]
python UserGroups.py export     <env>  [--format csv|json] [--output-dir PATH]
python UserGroups.py privileges <env>  [--format csv|json] [--output-dir PATH]
python UserGroups.py members    <env>  [--format csv|json] [--resolve]
                                       [--output-dir PATH]
```

---

#### `audit` — identify empty groups and privileged groups

Scans all user groups and writes **two separate output files**:

- **Empty groups** — groups with zero direct members.
- **Privileged groups** — groups with one or more privileges directly assigned
  to the group object (inherited privileges are excluded).

Groups outside both categories produce no output file for that category.

| Argument | Required | Description |
|---|---|---|
| `env` | Yes | Environment to audit: `dev`, `qa`, or `prod` |
| `--format csv\|json` | No | Output format (default: `csv`) |
| `--output-dir PATH` | No | Output directory (default: `MSTR_OUTPUT_DIR` or `c:/tmp`) |

```bash
# Audit dev — writes CSV files for empty and privileged groups
python UserGroups.py audit dev

# Audit prod in JSON format
python UserGroups.py audit prod --format json

# Audit QA with a custom output directory
python UserGroups.py audit qa --output-dir c:/reports/groups
```

**Output files:**

| File | Contents |
|---|---|
| `<output-dir>/user_groups_audit_empty.csv` (or `.json`) | Groups with no direct members |
| `<output-dir>/user_groups_audit_privileged.csv` (or `.json`) | Groups with directly-assigned privileges |

**CSV columns — empty groups:**

| Column | Description |
|---|---|
| `id` | Group GUID |
| `name` | Group display name |
| `description` | Group description |

**CSV columns — privileged groups:**

| Column | Description |
|---|---|
| `id` | Group GUID |
| `name` | Group display name |
| `description` | Group description |
| `privilege_count` | Number of directly-assigned privileges |
| `privilege_names` | Semicolon-separated list of privilege names |

---

#### `export` — export all groups with members and privileges

Exports every group with its direct members and directly-assigned privileges
in a single file.  Useful as a full snapshot of the group directory.

| Argument | Required | Description |
|---|---|---|
| `env` | Yes | Environment to export: `dev`, `qa`, or `prod` |
| `--format csv\|json` | No | Output format (default: `json`) |
| `--output-dir PATH` | No | Output directory (default: `MSTR_OUTPUT_DIR` or `c:/tmp`) |

```bash
# Export all groups from dev to JSON (default)
python UserGroups.py export dev

# Export all groups from prod to CSV
python UserGroups.py export prod --format csv
```

**Output file:** `<output-dir>/user_groups_export.json` (or `.csv`)

**JSON structure:**

```json
[
  {
    "id": "...",
    "name": "Finance Users",
    "description": "",
    "member_count": 5,
    "members": [
      {"id": "...", "name": "Alice", "type": "user"},
      {"id": "...", "name": "Sub-Group A", "type": "group"}
    ],
    "privilege_count": 2,
    "privileges": [
      {"id": "...", "name": "Use Library", "type": "..."},
      {"id": "...", "name": "Web User", "type": "..."}
    ]
  }
]
```

**CSV columns:** `id`, `name`, `description`, `member_count`, `members` (JSON string),
`privilege_count`, `privileges` (JSON string).

---

#### `privileges` — list directly-assigned privileges per group

Lists only the privileges explicitly assigned to each group object.
Inherited privileges (from roles or parent groups) are excluded.

| Argument | Required | Description |
|---|---|---|
| `env` | Yes | Environment to query: `dev`, `qa`, or `prod` |
| `--format csv\|json` | No | Output format (default: `json`) |
| `--output-dir PATH` | No | Output directory (default: `MSTR_OUTPUT_DIR` or `c:/tmp`) |

```bash
# List direct privileges from dev to JSON (default)
python UserGroups.py privileges dev

# List direct privileges from prod to CSV
python UserGroups.py privileges prod --format csv
```

**Output file:** `<output-dir>/user_groups_privileges.json` (or `.csv`)

**JSON structure:** one object per group that has at least one direct privilege,
with a `privileges` array.

**CSV columns** (one row per privilege, flattened):

| Column | Description |
|---|---|
| `group_id` | Group GUID |
| `group_name` | Group display name |
| `group_description` | Group description |
| `priv_id` | Privilege GUID |
| `priv_name` | Privilege name |
| `priv_type` | Privilege type |

---

#### `members` — list group members

Lists the members of every group.  By default returns **direct members only**
(both users and nested subgroups).  Pass `--resolve` to recursively expand
all subgroups and return the **effective set of users** (deduplicated).

| Argument | Required | Description |
|---|---|---|
| `env` | Yes | Environment to query: `dev`, `qa`, or `prod` |
| `--format csv\|json` | No | Output format (default: `json`) |
| `--resolve` | No | Recursively expand subgroups; return effective (deduplicated) users only |
| `--output-dir PATH` | No | Output directory (default: `MSTR_OUTPUT_DIR` or `c:/tmp`) |

```bash
# Direct members of every group on dev (JSON)
python UserGroups.py members dev

# Direct members in CSV format
python UserGroups.py members dev --format csv

# Recursive effective-user resolution on prod
python UserGroups.py members prod --resolve

# Effective users on QA, CSV output
python UserGroups.py members qa --resolve --format csv
```

**Output files:**

| Mode | File |
|---|---|
| Direct (default) | `<output-dir>/user_groups_members_direct.json` (or `.csv`) |
| Resolved (`--resolve`) | `<output-dir>/user_groups_members_resolved.json` (or `.csv`) |

**JSON structure (direct):** one object per group with a `members` array
containing objects with `id`, `name`, and `type` (`"user"` or `"group"`).

**JSON structure (resolved):** one object per group with a `members` array
containing only user objects (`type` is always `"user"`).

**CSV columns — direct:**

| Column | Description |
|---|---|
| `group_id` | Group GUID |
| `group_name` | Group display name |
| `member_id` | Member GUID |
| `member_name` | Member display name |
| `member_type` | `user` or `group` |

**CSV columns — resolved (`--resolve`):**

| Column | Description |
|---|---|
| `group_id` | Group GUID |
| `group_name` | Group display name |
| `user_id` | User GUID |
| `user_name` | User display name |

---

## Legacy Scripts

These scripts do not accept command-line arguments.  Configuration (server URL,
credentials, project IDs, output paths) is set by editing the variables at the
**top of each file** or via `.env` / environment variables.

Run them directly:

```bash
python <ScriptName>.py
```

| Script | Purpose |
|---|---|
| `ListProjects.py` | List all loaded projects as a DataFrame |
| `add_trustedauth_internal_users.py` | Add trusted authentication to users (SAML migration) |
| `UpdateProjectSettings.py` | Configure project-level settings |
| `SearchDependents.py` | Find dependents of a given object (e.g. transformations) |
| `MIgrationPackageInfo.py` | Query migration package details via REST API |
| `GetReportDefs_OutputCSV_SearchObject.py` | Extract report definitions → CSV |
| `get_MetricDef_SearchResultsObject_folderPathFunction_RestAPI_CSVOutput.py` | Extract metric definitions → CSV |
| `Addto_ContentGroup_RestAPI_ReportData_JSON.py` | Add dossiers to content groups via REST API |
| `ShortcutCreateFromExcel.py` | Create shortcuts from an Excel GUID list |
| `UpdateMetric_ReportDataSource_ChangeSet.py` | Update metric data source via REST API changeset |
| `mstrDocumentObjectDefinition.py` | Extract Dossier definitions → CSV + Platform Analytics dataset |

---

## Output Files

All output files are written to the directory resolved in this order:

1. `--output-dir` argument (if provided on the command line)
2. `MSTR_OUTPUT_DIR` environment variable
3. `MSTR_{ENV}_OUTPUT_DIR` environment variable
4. Built-in default: `c:/tmp`

The directory is created automatically if it does not exist.

---

## Logging

Log output goes to both the console and a rotating daily log file.

| Variable | Default | Description |
|---|---|---|
| `MSTR_LOG_DIR` | `logs` | Directory for log files (relative to the project root) |
| `MSTR_LOG_LEVEL` | `INFO` | Minimum log level: `DEBUG`, `INFO`, `WARNING`, or `ERROR` |

Log files are named by date (`logs/YYYY-MM-DD.log`) and retained for 30 days.

To enable verbose output during development or troubleshooting:

```ini
# .env
MSTR_LOG_LEVEL=DEBUG
```
