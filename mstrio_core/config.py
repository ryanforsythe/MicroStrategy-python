"""
Configuration management for MicroStrategy connections.

All settings are read from environment variables, with optional .env file support.
Environment variables take precedence over .env file values.

Multi-environment support
─────────────────────────
Set MSTR_ENV=dev|qa|prod (default: dev) to select the target environment.
For each setting the prefixed variable is checked first, then the bare variable:

    MSTR_{ENV}_{SETTING}  →  MSTR_{SETTING}  →  built-in default

Example: when MSTR_ENV=qa, MSTR_QA_BASE_URL is used before MSTR_BASE_URL.

Required environment variables (per environment):
    MSTR_BASE_URL   — e.g. https://server.cloud.microstrategy.com/MicroStrategyLibrary
    MSTR_USERNAME
    MSTR_PASSWORD
    Or prefixed equivalents:
    MSTR_DEV_BASE_URL  / MSTR_DEV_USERNAME  / MSTR_DEV_PASSWORD
    MSTR_QA_BASE_URL   / MSTR_QA_USERNAME   / MSTR_QA_PASSWORD
    MSTR_PROD_BASE_URL / MSTR_PROD_USERNAME / MSTR_PROD_PASSWORD

Optional environment variables:
    MSTR_LOGIN_MODE         — 1 (Standard), 16 (SAML), 64 (LDAP). Default: 1
    MSTR_PROJECT_ID         — Default project GUID (can also be set per-session)
    MSTR_PROJECT_NAME       — Default project name (used if MSTR_PROJECT_ID not set)
    MSTR_PA_PROJECT_ID      — Platform Analytics project GUID
    MSTR_PA_DATASET_ID      — Platform Analytics dataset/cube GUID for metadata writes
    MSTR_OUTPUT_DIR         — Default directory for CSV/Excel output. Default: c:/tmp
    MSTR_LOG_DIR            — Directory for log files. Default: logs
    MSTR_LOG_LEVEL          — DEBUG | INFO | WARNING | ERROR. Default: INFO
    MSTR_SSL_VERIFY         — true | false. Default: true.
                              Set to false to disable SSL certificate verification for
                              environments with self-signed or internal CA certificates.
                              Supports env prefix: MSTR_{ENV}_SSL_VERIFY.
                              Accepts: true/false, 1/0, yes/no, on/off (case-insensitive).
    MSTR_KEYRING_SERVICE    — OS keyring service name for password lookup. Default: mstrio
                              Supports env prefix: MSTR_{ENV}_KEYRING_SERVICE.
                              If MSTR_*_PASSWORD is blank/absent, MstrConfig tries
                              keyring.get_password(service, username) automatically.
                              Requires: pip install keyring  (silently skipped if absent)

Auto-logging
────────────
MstrConfig automatically calls setup_logging() when instantiated, so scripts
need only one line to get both connection config and logging fully configured:

    config = MstrConfig()   # env vars + logging both ready
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from pathlib import Path

from loguru import logger

# Load .env if present — silently skip if python-dotenv is not installed
try:
    from dotenv import load_dotenv
    load_dotenv(override=False)  # env vars already set take precedence
except ImportError:
    pass


class LoginMode(IntEnum):
    STANDARD = 1
    SAML = 16
    LDAP = 64


class MstrEnvironment(str, Enum):
    """
    Deployment environment selector.

    Set MSTR_ENV=dev|qa|prod in the environment (or .env file) to activate
    the corresponding set of prefixed connection variables.

    Example:
        MSTR_ENV=qa          → uses MSTR_QA_BASE_URL, MSTR_QA_USERNAME, etc.
        MSTR_ENV=prod        → uses MSTR_PROD_BASE_URL, MSTR_PROD_USERNAME, etc.

    Falls back to the unprefixed MSTR_* variables if a prefixed one is absent.
    """
    DEV = "dev"
    QA = "qa"
    PROD = "prod"


@dataclass
class MstrConfig:
    """
    MicroStrategy connection configuration.

    Reads from environment variables on instantiation, applying the active
    environment prefix (MSTR_ENV → MSTR_DEV_* / MSTR_QA_* / MSTR_PROD_*).
    Logging is configured automatically — no separate setup_logging() call needed.

    Fields left as None are resolved from env vars in __post_init__.
    Pass explicit keyword arguments to bypass env var lookup for that field.

    Example:
        config = MstrConfig()                              # fully env-var driven
        config = MstrConfig(environment=MstrEnvironment.QA)  # force QA env
        config = MstrConfig(project_id="ABC123")           # override one field
    """

    # Determines which MSTR_{ENV}_* prefix is used for all other fields.
    environment: MstrEnvironment = field(
        default_factory=lambda: MstrEnvironment(
            os.environ.get("MSTR_ENV", "dev").strip().lower()
        )
    )

    # Fields left as None are resolved from env vars (with env prefix) in __post_init__
    base_url: str | None = None
    username: str | None = None
    password: str | None = None
    login_mode: LoginMode | None = None
    project_id: str | None = None
    project_name: str | None = None
    pa_project_id: str | None = None
    pa_dataset_id: str | None = None
    output_dir: Path | None = None
    log_dir: Path | None = None
    log_level: str | None = None
    ssl_verify: bool | None = None

    def __post_init__(self) -> None:
        # Coerce environment string to enum if caller passed a raw string
        if isinstance(self.environment, str):
            self.environment = MstrEnvironment(self.environment.strip().lower())

        pfx = self.environment.value.upper()  # "DEV", "QA", or "PROD"

        if self.base_url is None:
            self.base_url = _require_env_prefix("BASE_URL", pfx)
        if self.username is None:
            self.username = _require_env_prefix("USERNAME", pfx)
        if self.password is None:
            self.password = _env_prefix_get("PASSWORD", pfx)
        if not self.password and self.username:
            self.password = _keyring_get_password(self.username, pfx)
        if not self.password:
            pfx_key = f"MSTR_{pfx}_PASSWORD" if pfx else "MSTR_PASSWORD"
            base_key = "MSTR_PASSWORD"
            hint = (
                f"'{pfx_key}' or '{base_key}'"
                if pfx and pfx_key != base_key
                else f"'{base_key}'"
            )
            raise EnvironmentError(
                f"Required password not found. "
                f"Set env var {hint} in your shell or .env file, "
                f"or store it in the OS keyring: "
                f"python -m keyring set mstrio {self.username}"
            )
        if self.login_mode is None:
            raw = _env_prefix_get("LOGIN_MODE", pfx, "1")
            self.login_mode = LoginMode(int(raw))
        if self.project_id is None:
            self.project_id = _env_prefix_get("PROJECT_ID", pfx)
        if self.project_name is None:
            self.project_name = _env_prefix_get("PROJECT_NAME", pfx)
        if self.pa_project_id is None:
            self.pa_project_id = _env_prefix_get("PA_PROJECT_ID", pfx)
        if self.pa_dataset_id is None:
            self.pa_dataset_id = _env_prefix_get("PA_DATASET_ID", pfx)
        if self.output_dir is None:
            self.output_dir = Path(_env_prefix_get("OUTPUT_DIR", pfx, "c:/tmp"))
        if self.log_dir is None:
            self.log_dir = Path(_env_prefix_get("LOG_DIR", pfx, "logs"))
        if self.log_level is None:
            self.log_level = (
                _env_prefix_get("LOG_LEVEL", pfx, "INFO") or "INFO"
            ).upper()
        if self.ssl_verify is None:
            raw = _env_prefix_get("SSL_VERIFY", pfx, "true") or "true"
            self.ssl_verify = raw.strip().lower() not in {"false", "0", "no", "off"}

        # Normalize base_url — strip trailing slash
        self.base_url = self.base_url.rstrip("/")

        # Ensure output and log directories exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Configure logging automatically — scripts need only MstrConfig()
        from mstrio_core.logging_setup import setup_logging
        setup_logging(log_dir=self.log_dir, level=self.log_level)

        logger.debug(
            "MstrConfig loaded: env={env} base_url={url} login_mode={mode} "
            "ssl_verify={ssl} project_id={pid} pa_project_id={pa_pid}",
            env=self.environment.value,
            url=self.base_url,
            mode=self.login_mode.name,
            ssl=self.ssl_verify,
            pid=self.project_id or "(none)",
            pa_pid=self.pa_project_id or "(none)",
        )

    @property
    def api_url(self) -> str:
        """Base URL for the REST API, e.g. https://host/MicroStrategyLibrary/api"""
        return self.base_url + "/api"


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────


def _env_prefix_get(
    name: str, prefix: str = "", default: str | None = None
) -> str | None:
    """
    Look up MSTR_{PREFIX}_{NAME} first, then MSTR_{NAME}, then return default.

    Args:
        name:    Variable name without the MSTR_ prefix, e.g. "BASE_URL".
        prefix:  Uppercase environment prefix, e.g. "DEV". Empty for no prefix.
        default: Value to return if neither variable is set.

    Example:
        _env_prefix_get("BASE_URL", "DEV")
        # checks MSTR_DEV_BASE_URL → MSTR_BASE_URL → None
    """
    if prefix:
        value = os.environ.get(f"MSTR_{prefix}_{name}")
        if value:
            return value
    return os.environ.get(f"MSTR_{name}", default)


def _require_env_prefix(name: str, prefix: str = "") -> str:
    """
    Like _env_prefix_get but raises EnvironmentError if neither variable is set.

    Example:
        _require_env_prefix("BASE_URL", "QA")
        # checks MSTR_QA_BASE_URL → MSTR_BASE_URL → EnvironmentError
    """
    value = _env_prefix_get(name, prefix)
    if not value:
        prefixed_key = f"MSTR_{prefix}_{name}" if prefix else f"MSTR_{name}"
        base_key = f"MSTR_{name}"
        hint = (
            f"'{prefixed_key}' or '{base_key}'"
            if prefix and prefixed_key != base_key
            else f"'{base_key}'"
        )
        raise EnvironmentError(
            f"Required env var {hint} is not set. "
            f"Set it in your shell or in a .env file."
        )
    return value


def _keyring_get_password(username: str, prefix: str = "") -> str | None:
    """
    Try to retrieve the MicroStrategy password from the OS keyring.

    Resolution order (with prefix "QA" as an example):
        1. keyring.get_password(MSTR_QA_KEYRING_SERVICE or "mstrio-qa", username)
        2. keyring.get_password(MSTR_KEYRING_SERVICE or "mstrio", username)

    Silently returns None if:
        - keyring package is not installed (ImportError)
        - No matching entry exists in the keyring
        - username is empty / None

    Args:
        username: The MicroStrategy username to look up.
        prefix:   Uppercase environment prefix, e.g. "DEV". Empty for no prefix.

    Returns:
        Password string, or None if not found.
    """
    if not username:
        return None

    try:
        import keyring  # type: ignore[import]
    except ImportError:
        return None

    # Determine service names from env vars or derive sensible defaults
    base_service = _env_prefix_get("KEYRING_SERVICE", "", "mstrio") or "mstrio"

    if prefix:
        env_service = (
            _env_prefix_get("KEYRING_SERVICE", prefix)
            or f"{base_service}-{prefix.lower()}"
        )
        pw = keyring.get_password(env_service, username)
        if pw:
            logger.debug(
                "Password retrieved from keyring service '{svc}' for user '{user}'.",
                svc=env_service,
                user=username,
            )
            return pw

    pw = keyring.get_password(base_service, username)
    if pw:
        logger.debug(
            "Password retrieved from keyring service '{svc}' for user '{user}'.",
            svc=base_service,
            user=username,
        )
    return pw


# Backward-compatible alias for any existing code using _require_env directly
_require_env = _require_env_prefix
