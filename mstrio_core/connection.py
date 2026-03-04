"""
MicroStrategy connection management.

MstrRestSession  — Unified session: one mstrio-py login for both SDK and raw REST.
get_mstrio_connection — Factory for standalone mstrio-py Connection objects.

Design
──────
MstrRestSession authenticates once via mstrio-py (Connection.__init__) and
then exposes two access patterns from that single session:

1.  Raw REST API calls via the convenience methods (.get, .post, .put, …).
    These use the mstrio-py Connection's underlying requests.Session so
    the auth token and cookies are shared — no second login.

2.  mstrio-py SDK objects via session.mstrio_conn.
    Returns the live mstrio.connection.Connection for use with higher-level
    classes like OlapCube, full_search, User, etc.

Usage — REST API:
    from mstrio_core import MstrConfig, MstrRestSession

    config = MstrConfig()    # also configures logging
    with MstrRestSession(config) as session:
        session.set_project(name="My Project")

        r = session.get("/migrations")                            # server-scoped
        r = session.get("/reports/" + guid, scope="project")     # project-scoped

        with session.changeset() as cs_id:
            session.put("/model/metrics/" + guid, json=body, changeset_id=cs_id)

Usage — mstrio-py SDK (via same session):
    with MstrRestSession(config) as session:
        session.set_project(project_id=config.project_id)
        conn = session.mstrio_conn          # live Connection, already logged in
        cube = OlapCube(conn, id=dataset_id)
        cube.publish()

Usage — mstrio-py only (no raw REST needed):
    from mstrio_core import get_mstrio_connection

    conn = get_mstrio_connection()                          # standard auth
    conn = get_mstrio_connection(workstation_data=wd)       # Workstation auth

SSL verification:
    SSL cert verification is enabled by default. For environments with self-signed
    or internally-signed certificates, set MSTR_SSL_VERIFY=false in .env (or the
    env-prefixed variant, e.g. MSTR_DEV_SSL_VERIFY=false).
    Can also be overridden programmatically:
        config = MstrConfig(ssl_verify=False)
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Generator, Optional

import requests
from loguru import logger

from mstrio_core.config import MstrConfig


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _suppress_ssl_warnings() -> None:
    """
    Suppress urllib3's InsecureRequestWarning when SSL verification is disabled.

    The warning is replaced by a single loguru WARNING logged at connection time,
    keeping console output clean without hiding the fact that verification is off.
    """
    try:
        import urllib3  # type: ignore[import]
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except Exception:
        pass  # urllib3 not available directly — mstrio-py bundles requests which bundles it,
              # but if the import path differs we just skip; the warning is cosmetic only


class MstrRestSession:
    """
    Unified MicroStrategy session — one login, both REST API and mstrio-py SDK.

    Internally authenticates via mstrio-py's Connection, then borrows its
    requests.Session for raw REST calls. This ensures a single auth token
    and cookie jar is shared across all access patterns.

    Use as a context manager to ensure clean login/logout:

        with MstrRestSession(config) as session:
            session.set_project(project_id=config.project_id)
            r = session.get("/v2/documents/" + dossier_id)
            conn = session.mstrio_conn   # for SDK operations
    """

    def __init__(self, config: MstrConfig) -> None:
        self._config = config
        self._conn: Optional[Any] = None       # mstrio-py Connection, set on login
        self._project_id: Optional[str] = config.project_id

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def api_url(self) -> str:
        return self._config.api_url

    @property
    def mstrio_conn(self) -> Any:
        """
        The live mstrio-py Connection object.

        Use this to access SDK classes that require a Connection:
            OlapCube(session.mstrio_conn, id=dataset_id)
            full_search(session.mstrio_conn, object_types=[...])

        The Connection is already authenticated and shares the same session
        as the raw REST API calls — no second login is performed.
        """
        self._require_auth()
        return self._conn

    @property
    def _session(self) -> requests.Session:
        """
        The underlying requests.Session from the mstrio-py Connection.

        Shared with the SDK — one set of cookies and headers for everything.
        """
        self._require_auth()
        # Prefer the public .session property; fall back to the private attribute
        # for older mstrio-py versions that don't expose it publicly.
        s = getattr(self._conn, "session", None)
        if s is None:
            s = getattr(self._conn, "_session", None)
        if s is None:
            raise RuntimeError(
                "Could not access the mstrio-py Connection's underlying "
                "requests.Session. Check your mstrio-py version."
            )
        return s

    @property
    def _token(self) -> str:
        """Auth token from the mstrio-py Connection."""
        self._require_auth()
        return self._conn.token

    @property
    def server_headers(self) -> dict[str, str]:
        """Auth headers without project context."""
        return {
            "X-MSTR-AuthToken": self._token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    @property
    def project_headers(self) -> dict[str, str]:
        """Auth headers including the active project ID."""
        if not self._project_id:
            raise RuntimeError(
                "No project is set. Call set_project() before using project_headers."
            )
        return {**self.server_headers, "X-MSTR-ProjectID": self._project_id}

    def changeset_headers(self, changeset_id: str) -> dict[str, str]:
        """Auth + project + changeset headers for model PUT requests."""
        return {**self.project_headers, "X-MSTR-MS-Changeset": changeset_id}

    @property
    def project_id(self) -> Optional[str]:
        return self._project_id

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def login(self) -> None:
        """
        Authenticate via mstrio-py and store the Connection.
        Called automatically by __enter__ when using as a context manager.
        """
        try:
            from mstrio.connection import Connection
        except ImportError as exc:
            raise ImportError(
                "mstrio-py is not installed. Run: pip install mstrio-py"
            ) from exc

        cfg = self._config

        if not cfg.ssl_verify:
            logger.warning(
                "SSL verification is DISABLED (ssl_verify=False). "
                "Use only on networks you trust — never in production against public endpoints."
            )
            _suppress_ssl_warnings()

        self._conn = Connection(
            base_url=cfg.base_url,
            username=cfg.username,
            password=cfg.password,
            login_mode=int(cfg.login_mode),
            ssl_verify=cfg.ssl_verify,
        )

        # Set the default project on the mstrio-py Connection if one is configured
        if self._project_id:
            try:
                self._conn.select_project(project_id=self._project_id)
            except Exception as exc:
                logger.warning(
                    "Could not pre-select project {pid} on mstrio Connection: {exc}",
                    pid=self._project_id,
                    exc=exc,
                )

        logger.info(
            "Logged in to {url} as {user} (env={env}, mode={mode})",
            url=cfg.base_url,
            user=cfg.username,
            env=cfg.environment.value,
            mode=cfg.login_mode.name,
        )

    def logout(self) -> None:
        """
        Close the mstrio-py Connection (performs REST API logout).
        Called automatically by __exit__ when using as a context manager.
        """
        if self._conn is None:
            return
        try:
            self._conn.close()
            logger.info("Logged out successfully.")
        except Exception as exc:
            logger.warning("Logout failed: {exc}", exc=exc)
        finally:
            self._conn = None

    # ------------------------------------------------------------------
    # Project context
    # ------------------------------------------------------------------

    def set_project(
        self,
        project_id: Optional[str] = None,
        name: Optional[str] = None,
    ) -> str:
        """
        Set the active project for REST API calls and the mstrio-py Connection.

        Pass project_id (GUID) directly, or pass name to resolve it via the API.
        Both the raw REST headers and session.mstrio_conn are updated together.
        Returns the resolved project GUID.

        Example:
            session.set_project(project_id="ABC123")
            session.set_project(name="Platform Analytics")
        """
        if project_id:
            self._project_id = project_id
            self._sync_project_to_conn(project_id)
            logger.debug("Project set by ID: {id}", id=project_id)
            return project_id

        if name:
            r = self._session.get(
                self.api_url + "/projects/" + name,
                headers=self.server_headers,
            )
            if not r.ok:
                logger.error(
                    "Could not resolve project '{name}': HTTP {status} {body}",
                    name=name,
                    status=r.status_code,
                    body=r.text,
                )
                r.raise_for_status()
            data = r.json()
            self._project_id = data["id"]
            self._sync_project_to_conn(self._project_id)
            logger.info(
                "Project set: {name} ({id})",
                name=data.get("name", name),
                id=self._project_id,
            )
            return self._project_id

        raise ValueError("Provide either project_id or name.")

    def _sync_project_to_conn(self, project_id: str) -> None:
        """Keep the mstrio-py Connection's project in sync with our project_id."""
        if self._conn is None:
            return
        try:
            self._conn.select_project(project_id=project_id)
        except Exception as exc:
            logger.debug(
                "Could not sync project {pid} to mstrio Connection: {exc}",
                pid=project_id,
                exc=exc,
            )

    # ------------------------------------------------------------------
    # HTTP convenience methods
    # ------------------------------------------------------------------

    def get(
        self,
        path: str,
        scope: str = "auto",
        changeset_id: Optional[str] = None,
        **kwargs: Any,
    ) -> requests.Response:
        return self._request("GET", path, scope, changeset_id, **kwargs)

    def post(
        self,
        path: str,
        scope: str = "auto",
        changeset_id: Optional[str] = None,
        **kwargs: Any,
    ) -> requests.Response:
        return self._request("POST", path, scope, changeset_id, **kwargs)

    def put(
        self,
        path: str,
        scope: str = "auto",
        changeset_id: Optional[str] = None,
        **kwargs: Any,
    ) -> requests.Response:
        return self._request("PUT", path, scope, changeset_id, **kwargs)

    def patch(
        self,
        path: str,
        scope: str = "auto",
        changeset_id: Optional[str] = None,
        **kwargs: Any,
    ) -> requests.Response:
        return self._request("PATCH", path, scope, changeset_id, **kwargs)

    def delete(
        self,
        path: str,
        scope: str = "auto",
        changeset_id: Optional[str] = None,
        **kwargs: Any,
    ) -> requests.Response:
        return self._request("DELETE", path, scope, changeset_id, **kwargs)

    def _request(
        self,
        method: str,
        path: str,
        scope: str,
        changeset_id: Optional[str],
        **kwargs: Any,
    ) -> requests.Response:
        """
        Execute an HTTP request against the MicroStrategy API.

        scope:
            "auto"    — use project_headers if a project is set, else server_headers
            "server"  — always use server_headers (no X-MSTR-ProjectID)
            "project" — always use project_headers (raises if no project set)

        changeset_id:
            When provided, adds X-MSTR-MS-Changeset to the headers.
        """
        if changeset_id:
            headers = self.changeset_headers(changeset_id)
        elif scope == "server":
            headers = self.server_headers
        elif scope == "project":
            headers = self.project_headers
        else:  # "auto"
            headers = self.project_headers if self._project_id else self.server_headers

        # Allow caller to override/extend headers
        if "headers" in kwargs:
            headers = {**headers, **kwargs.pop("headers")}

        url = self.api_url + path
        r = self._session.request(method, url, headers=headers, **kwargs)

        logger.debug(
            "{method} {path} → HTTP {status}",
            method=method,
            path=path,
            status=r.status_code,
        )
        if not r.ok:
            logger.warning(
                "{method} {path} failed: HTTP {status} {reason} — {body}",
                method=method,
                path=path,
                status=r.status_code,
                reason=r.reason,
                body=r.text[:500],
            )

        return r

    # ------------------------------------------------------------------
    # Changeset context manager
    # ------------------------------------------------------------------

    @contextmanager
    def changeset(
        self, schema_edit: bool = False
    ) -> Generator[str, None, None]:
        """
        Context manager for MicroStrategy model changesets.

        Opens a changeset, yields the changeset ID, then commits on clean exit
        or rolls back if an exception is raised.

        Args:
            schema_edit: Set True for schema-level changes (DDL/structure).
                         False (default) for metric/filter/report updates.

        Example:
            with session.changeset() as cs_id:
                session.put("/model/metrics/" + guid, json=body, changeset_id=cs_id)
            # auto-committed here
        """
        r = self._session.post(
            self.api_url + f"/model/changesets?schemaEdit={str(schema_edit).lower()}",
            headers=self.project_headers,
        )
        if not r.ok:
            logger.error(
                "Failed to open changeset: HTTP {status} {body}",
                status=r.status_code,
                body=r.text,
            )
            r.raise_for_status()

        changeset_id: str = r.json()["id"]
        logger.debug("Opened changeset {id} (schemaEdit={se})", id=changeset_id, se=schema_edit)

        try:
            yield changeset_id

            # Commit
            cr = self._session.post(
                self.api_url + f"/model/changesets/{changeset_id}/commit",
                headers=self.project_headers,
            )
            if cr.ok:
                logger.info("Committed changeset {id}", id=changeset_id)
            else:
                logger.error(
                    "Changeset commit failed: HTTP {status} {body}",
                    status=cr.status_code,
                    body=cr.text,
                )
                cr.raise_for_status()

        except Exception:
            # Rollback (DELETE changeset)
            try:
                self._session.delete(
                    self.api_url + f"/model/changesets/{changeset_id}",
                    headers=self.project_headers,
                )
                logger.warning("Rolled back changeset {id}", id=changeset_id)
            except Exception as rb_exc:
                logger.error(
                    "Rollback of changeset {id} also failed: {exc}",
                    id=changeset_id,
                    exc=rb_exc,
                )
            raise

    # ------------------------------------------------------------------
    # Context manager (full session lifecycle)
    # ------------------------------------------------------------------

    def __enter__(self) -> "MstrRestSession":
        self.login()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.logout()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _require_auth(self) -> None:
        if self._conn is None:
            raise RuntimeError(
                "Not authenticated. Call login() or use MstrRestSession as a context manager."
            )


# ---------------------------------------------------------------------------
# mstrio-py Connection factory (standalone — no raw REST needed)
# ---------------------------------------------------------------------------

def get_mstrio_connection(
    config: Optional[MstrConfig] = None,
    workstation_data: Any = None,
    project_id: Optional[str] = None,
) -> Any:
    """
    Return a standalone mstrio-py Connection object.

    Use this when you only need the mstrio-py SDK and have no need for raw
    REST API calls. For scripts that use both, prefer MstrRestSession and
    access the Connection via session.mstrio_conn (one login, shared session).

    Two authentication modes:

    1. Standard (username/password from MstrConfig or env vars):
        conn = get_mstrio_connection()           # reads env vars automatically
        conn = get_mstrio_connection(config)     # explicit config

    2. Workstation (running inside MicroStrategy Workstation):
        conn = get_mstrio_connection(workstation_data=workstationData)

    Args:
        config:           MstrConfig instance. Created from env vars if omitted.
        workstation_data: Workstation session data object (inside Workstation).
        project_id:       Override the project GUID from config.

    Returns:
        mstrio.connection.Connection
    """
    try:
        from mstrio.connection import Connection, get_connection
    except ImportError as exc:
        raise ImportError(
            "mstrio-py is not installed. Run: pip install mstrio-py"
        ) from exc

    if workstation_data is not None:
        logger.info("Connecting via Workstation data.")
        return get_connection(workstation_data)

    if config is None:
        config = MstrConfig()

    pid = project_id or config.project_id

    if not config.ssl_verify:
        logger.warning(
            "SSL verification is DISABLED (ssl_verify=False). "
            "Use only on networks you trust — never in production against public endpoints."
        )
        _suppress_ssl_warnings()

    logger.info(
        "Connecting via mstrio-py: {url} as {user} (env={env})",
        url=config.base_url,
        user=config.username,
        env=config.environment.value,
    )

    conn = Connection(
        base_url=config.base_url,
        username=config.username,
        password=config.password,
        login_mode=int(config.login_mode),
        ssl_verify=config.ssl_verify,
    )

    # Select the project as a separate step to avoid triggering mstrio-py's
    # internal select_project() call from __init__, which has a known bug
    # (UnboundLocalError on tmp_projects) when called during construction.
    if pid:
        try:
            conn.select_project(project_id=pid)
        except Exception as exc:
            logger.warning(
                "Could not select project {pid}: {exc}", pid=pid, exc=exc
            )

    return conn
