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
   - [ExportUsers.py](#exportuserspy)
   - [ExpireSchedules.py](#expireschedulespyy)
   - [CompareServerSettings.py](#compareserversettingspy)
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

The CLI scripts (`ExportUsers.py`, `ExpireSchedules.py`,
`CompareServerSettings.py`) override `MSTR_ENV` at runtime by constructing
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

### ExportUsers.py

Export all MicroStrategy users from an environment to a CSV file.

**Output columns:** `base_url`, `guid`, `name`, `id` (login username),
`trusted_auth` (SAML trust ID), `group_membership` (JSON array).

#### Usage

```
python ExportUsers.py <env> [--output-dir PATH]
```

| Argument | Required | Description |
|---|---|---|
| `env` | Yes | Environment to export: `dev`, `qa`, or `prod` |
| `--output-dir PATH` | No | Directory for the output CSV (default: `MSTR_OUTPUT_DIR` or `c:/tmp`) |

#### Examples

```bash
# Export users from dev (output → c:/tmp/users_export.csv)
python ExportUsers.py dev

# Export users from prod to a specific directory
python ExportUsers.py prod --output-dir c:/reports
```

#### Output file

`<output-dir>/users_export.csv`

---

### ExpireSchedules.py

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
python ExpireSchedules.py <env> [--apply] [--output-dir PATH]
```

| Argument | Required | Description |
|---|---|---|
| `env` | Yes | Environment to process: `dev`, `qa`, or `prod` |
| `--apply` | No | Apply changes to the server (default: dry run — preview only) |
| `--output-dir PATH` | No | Directory for the preview CSV (default: `MSTR_OUTPUT_DIR` or `c:/tmp`) |

#### Examples

```bash
# Step 1 — preview what would change on prod (no server modifications)
python ExpireSchedules.py prod

# Review c:/tmp/expired_schedules.csv, then:

# Step 2 — apply the changes
python ExpireSchedules.py prod --apply

# Dry run against dev with a custom output directory
python ExpireSchedules.py dev --output-dir c:/reports/schedules
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
| `has_dependents` | `True` / `False` — whether other objects depend on this schedule |
| `actions` | Comma-separated list of changes that will be (or were) applied |

---

### CompareServerSettings.py

Compare, export, or apply MicroStrategy I-Server settings across environments.

#### Subcommands

```
python CompareServerSettings.py compare <source> <target>  [--format csv|json] [--all]
python CompareServerSettings.py export  <env>              [--format csv|json] [--description]
python CompareServerSettings.py apply   <source> <target>  [--output-dir PATH]
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
python CompareServerSettings.py compare dev prod

# Compare dev vs prod, JSON output, include all rows
python CompareServerSettings.py compare dev prod --format json --all

# Compare qa vs prod with a custom output directory
python CompareServerSettings.py compare qa prod --output-dir c:/reports
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
python CompareServerSettings.py export dev

# Export QA settings to JSON
python CompareServerSettings.py export qa --format json

# Export prod settings with descriptions
python CompareServerSettings.py export prod --description
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
python CompareServerSettings.py apply dev qa

# Copy dev settings to prod with a custom snapshot directory
python CompareServerSettings.py apply dev prod --output-dir c:/reports/snapshots
```

**Files written:**

| File | Description |
|---|---|
| `<output-dir>/server_settings_<target>_BEFORE.csv` | Pre-apply snapshot of the target (audit trail) |

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
