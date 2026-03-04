"""
mstrio_core — Shared utilities for MicroStrategy management scripts.

Provides:
    MstrEnvironment     — dev / qa / prod environment selector
    MstrConfig          — env-var-driven configuration (multi-environment aware)
    MstrRestSession     — REST API session (auth, project scope, changesets)
    get_mstrio_connection — mstrio-py Connection factory
    setup_logging       — loguru configuration
    write_csv / write_excel / read_excel / object_location — output helpers
    PredefinedFolder    — IntEnum of MicroStrategy predefined system folder type IDs
    OBJECT_TYPE_MAP     — lowercase name → object type int
    OBJECT_TYPE_ID_MAP  — object type int → display name
    OBJECT_TYPE_CATEGORY — object type int → category string
    folder_contents     — list items in a folder via GET /api/folders/{id}
    folder_path_to_guid — resolve a backslash-delimited folder path to a GUID
    get_predefined_folder — resolve a predefined system folder to a GUID
    get_object_type_info — look up type/subtype/exttype for any object by GUID
"""

from mstrio_core.config import MstrConfig, MstrEnvironment, LoginMode
from mstrio_core.connection import MstrRestSession, get_mstrio_connection
from mstrio_core.logging_setup import setup_logging
from mstrio_core.output import write_csv, write_excel, read_excel, object_location
from mstrio_core.search import (
    PredefinedFolder,
    OBJECT_TYPE_MAP,
    OBJECT_TYPE_ID_MAP,
    OBJECT_TYPE_CATEGORY,
    folder_contents,
    folder_path_to_guid,
    get_predefined_folder,
    get_object_type_info,
)

__all__ = [
    # config
    "MstrEnvironment",
    "MstrConfig",
    "LoginMode",
    # connection
    "MstrRestSession",
    "get_mstrio_connection",
    # logging
    "setup_logging",
    # output
    "write_csv",
    "write_excel",
    "read_excel",
    "object_location",
    # search / folder utilities
    "PredefinedFolder",
    "OBJECT_TYPE_MAP",
    "OBJECT_TYPE_ID_MAP",
    "OBJECT_TYPE_CATEGORY",
    "folder_contents",
    "folder_path_to_guid",
    "get_predefined_folder",
    "get_object_type_info",
]
