'''
AttributeFormHtmlManage.py — Find and migrate Attribute Forms whose display
format is HTML / HTML Tag, from the legacy `?evt=3140&documentID=...` URL
pattern to the modern `?prompts=...` JSON URL pattern.

Two subcommands, designed to round-trip through the same CSV file:

    export
        Scan every loaded project (or `--project GUID ...`) for Attributes
        whose Forms contain an HTML / HTML Tag expression, parse the existing
        hyperlink, look up the matching prompt on the target document, and
        write a CSV with a suggested `NewFormExpression` column. Optional
        `--modified-since YYYY-MM-DD` filter on the attribute search.

    apply
        Read that same CSV back; for every row whose `NewFormExpression`
        is populated, replace the form expression text on the I-Server via
        a model changeset. Dry-run by default; `--apply` commits.

────────────────────────────────────────────────────────────────────────────
Expression conversion
────────────────────────────────────────────────────────────────────────────

Legacy expression (parsed). Note: in MicroStrategy formula syntax, a literal
double quote inside a string is escaped by doubling it ("").

    Concat("<a  title=""",ProductName,""" href=","""","?evt=3140&reportViewMode=1
    &promptAnswerMode=1&documentID=F7069A9C40775A6EB927FA9173795272
    &elementsPromptAnswers=A60F2B7E4029DF6CFDF4CE8D1915B535;
    A60F2B7E4029DF6CFDF4CE8D1915B535:",ToString(ProductID),
    "&evtwait=true&share=1",""""," target=","""","_blank","""",">",ProductName,"</a>")

Extracted:
    AttributeFormTargetDocumentID            F7069A9C40775A6EB927FA9173795272
    AttributeFormElementTargetAttributeID    A60F2B7E4029DF6CFDF4CE8D1915B535
    AttributeFormTargetValueID               ProductID    (passed via ToString)
    AttributeFormTargetValueName             ProductName  (title + label)

Prompt lookup:
    GET /api/documents/{TargetDocID}/prompts?closed=false
    Find the prompt whose `source.id` matches AttributeFormElementTargetAttributeID
    → AttributeFormTargetDocumentPromptID, AttributeFormTargetDocumentPromptKey

Suggested NewFormExpression (relative URL — browser keeps host/app/project):

    Concat("<a title=" + ESCAPED_QUOTE + ",ProductName, ESCAPED_QUOTE + " href=" +
        ESCAPED_QUOTE + "../{TargetDocID}?prompts=" + URL_ENCODED_JSON_PREFIX,
        ToString(ProductID), URL_ENCODED_JSON_SUFFIX + ESCAPED_QUOTE +
        " target=" + ESCAPED_QUOTE + "_blank" + ESCAPED_QUOTE + ">",
        ProductName,"</a>")

If any required piece (target doc, element-target attribute, value form,
display form, prompt key) cannot be resolved, NewFormExpression is left
blank — `apply` skips rows with empty NewFormExpression.

Usage
─────
    python AttributeFormHtmlManage.py export <env> [--output PATH]
                                                   [--project GUID ...]
                                                   [--modified-since YYYY-MM-DD]
                                                   [--concurrency N]

    python AttributeFormHtmlManage.py apply  <env> --input PATH [--apply]
'''

from __future__ import annotations

import argparse
import csv as csv_module
import json
import re
import sys
import threading
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from loguru import logger

from mstrio_core import MstrConfig, MstrRestSession
from mstrio_core.config import MstrEnvironment


# --Constants --───────────────────────────────────────────────────────────────

OBJECT_TYPE_ATTRIBUTE = 12

# Form expression display formats that indicate HTML rendering
HTML_DISPLAY_FORMATS = {"HTML_TAG", "HTML", "URL"}

# Heuristic: an HTML form's expression typically contains an <a/<img tag or href=
HTML_EXPRESSION_MARKERS = re.compile(r"<a\s|<img\s|<iframe|href=", re.IGNORECASE)

# CSV column names (PascalCase per spec)
COLUMNS = [
    "ProjectID",
    "ProjectName",
    "AttributeID",
    "AttributeName",
    "AttributeLocation",
    "AttributeFormName",
    "AttributeFormID",
    "AttributeFormExpression",
    "AttributeFormTargetDocumentID",
    "AttributeFormElementTargetAttributeID",
    "AttributeFormTargetValueID",
    "AttributeFormTargetValueName",
    "AttributeFormTargetDocumentPromptID",
    "AttributeFormTargetDocumentPromptKey",
    "NewFormExpression",
]

DEFAULT_CONCURRENCY = 10
DEFAULT_OUTPUT_NAME = "attribute_form_html.csv"


# --Expression parsing --──────────────────────────────────────────────────────

_DOC_ID_RE = re.compile(r"documentID=([0-9A-Fa-f]{32})")
_ELEMENT_ATTR_RE = re.compile(r"elementsPromptAnswers=([0-9A-Fa-f]{32})")
# After "elementsPromptAnswers=AID(;AID)?:", string-close + comma + optional ToString(IDENT)
_VALUE_REF_RE = re.compile(
    r'elementsPromptAnswers=[0-9A-Fa-f]{32}(?:;[0-9A-Fa-f]{32})?:'
    r'"\s*,\s*'
    r'(?:ToString\s*\(\s*)?([A-Za-z_][A-Za-z0-9_ ]*?)\s*[\),]'
)
# Display name candidates — try title= first, then the link text right before </a>
_DISPLAY_TITLE_RE = re.compile(r'title="""\s*,\s*([A-Za-z_][A-Za-z0-9_ ]*?)\s*,')
_LINK_TEXT_RE = re.compile(r',\s*([A-Za-z_][A-Za-z0-9_ ]*?)\s*,\s*"</a>"')


def parse_html_link_expression(expr: str) -> dict:
    """Pull out the components of a legacy `?evt=3140` hyperlink Concat()."""
    s = expr or ""
    out = {
        "target_document_id": "",
        "element_target_attribute_id": "",
        "target_value_id": "",
        "target_value_name": "",
    }
    m = _DOC_ID_RE.search(s)
    if m:
        out["target_document_id"] = m.group(1).upper()
    m = _ELEMENT_ATTR_RE.search(s)
    if m:
        out["element_target_attribute_id"] = m.group(1).upper()
    m = _VALUE_REF_RE.search(s)
    if m:
        out["target_value_id"] = m.group(1).strip()
    # Prefer the link text (always present in <a>...</a>), fall back to title=
    m = _LINK_TEXT_RE.search(s)
    if m:
        out["target_value_name"] = m.group(1).strip()
    else:
        m = _DISPLAY_TITLE_RE.search(s)
        if m:
            out["target_value_name"] = m.group(1).strip()
    return out


# --New expression builder --──────────────────────────────────────────────────

# URL-encoded JSON skeleton for `?prompts=[[{"key":"…","values":["…:…"],"useDefault":false}]]`
_JSON_OPEN = "%5B%5B%7B%22key%22%3A%22"
_JSON_MID = "%22%2C%22values%22%3A%5B%22"
_JSON_SEP = "%3A"
_JSON_END = "%22%5D%2C%22useDefault%22%3Afalse%7D%5D%5D"


def build_new_form_expression(
    target_doc_id: str,
    display_form: str,
    value_form: str,
    element_target_attr_id: str,
    prompt_key: str,
) -> str:
    '''
    Build the replacement Concat() expression using a relative URL so the
    browser keeps the current host / app / project context. The resulting
    formula (with MSTR's "" double-quote escapes shown literally) is:

        Concat("<a title=" QUOTE  NAME  QUOTE " href=" QUOTE
               "../{DOC}?prompts={JSON_URL_ENCODED}" QUOTE
               " target=" QUOTE "_blank" QUOTE ">"  NAME  "</a>")

    Returns "" if any required input is missing.
    '''
    if not all([target_doc_id, display_form, value_form, element_target_attr_id, prompt_key]):
        return ""

    key_enc = urllib.parse.quote(prompt_key, safe="")
    attr_enc = element_target_attr_id  # 32-hex GUID, nothing to encode

    url_prefix = (
        f"../{target_doc_id}?prompts="
        f"{_JSON_OPEN}{key_enc}{_JSON_MID}{attr_enc}{_JSON_SEP}"
    )
    url_suffix = _JSON_END

    return (
        'Concat('
        '"<a title=""",'
        f'{display_form},'
        f'""" href=""{url_prefix}",'
        f'ToString({value_form}),'
        f'"{url_suffix}"" target=""_blank"">",'
        f'{display_form},'
        '"</a>"'
        ')'
    )


# --Document prompt lookup --──────────────────────────────────────────────────


def find_doc_prompt_by_source_attr(
    session: MstrRestSession,
    project_id: str,
    doc_id: str,
    source_attr_id: str,
) -> tuple[str, str]:
    """
    Return (promptId, promptKey) for the prompt on the given document whose
    `source.id` matches `source_attr_id`. Tries v2 first, falls back to v1.
    """
    if not doc_id or not source_attr_id:
        return "", ""

    candidates = [
        (f"/v2/documents/{doc_id}/prompts", None),
        (f"/documents/{doc_id}/prompts", {"closed": "false"}),
    ]
    headers = {**session.server_headers, "X-MSTR-ProjectID": project_id}
    src_upper = source_attr_id.upper()

    for path, params in candidates:
        url = session.api_url + path
        try:
            r = session._session.get(url, headers=headers, params=params, timeout=30)
        except Exception:
            continue
        if r.status_code == 404:
            continue
        if not r.ok:
            continue
        try:
            data = r.json()
        except Exception:
            continue
        prompts = data if isinstance(data, list) else (data.get("prompts") or [])
        for p in prompts:
            source = p.get("source") or {}
            if (source.get("id") or "").upper() == src_upper:
                return p.get("id", ""), p.get("key", "")
        break  # got a valid response; don't try the other path

    return "", ""


# --Attribute search / detail fetch --─────────────────────────────────────────


def _expression_text(expr_obj: dict) -> str:
    """
    Return the MicroStrategy formula text from a form-expression entry.

    The /model/attributes/{id} response shapes the expression as:
        expressions[i].expression = { "text": "Concat(...)" }
    Older / future versions may flatten it to a string or nest under "text".
    """
    e = expr_obj.get("expression")
    if isinstance(e, dict):
        return e.get("text") or ""
    if isinstance(e, str):
        return e
    return expr_obj.get("text") or ""


def _set_expression_text(expr_obj: dict, new_text: str) -> None:
    """Set the formula text on an expression entry, preserving the wrapper shape."""
    e = expr_obj.get("expression")
    if isinstance(e, dict):
        e["text"] = new_text
        # Drop cached parse so the server re-tokenises from text.
        e.pop("tree", None)
        e.pop("tokens", None)
    else:
        expr_obj["expression"] = {"text": new_text}


def _is_html_form(form_obj: dict) -> bool:
    """
    True when the FORM's displayFormat marks it as HTML, or any of its
    expressions contains HTML markers in the formula text.

    NOTE: `displayFormat` is a property of the form (e.g. "html_tag"),
    NOT of an individual expression — earlier code looked in the wrong place.
    """
    fmt = (form_obj.get("displayFormat") or "").upper()
    if fmt in HTML_DISPLAY_FORMATS:
        return True
    for expr in form_obj.get("expressions") or []:
        if HTML_EXPRESSION_MARKERS.search(_expression_text(expr)):
            return True
    return False


def _list_loaded_projects(session: MstrRestSession) -> list[dict]:
    r = session.get("/projects", scope="server")
    r.raise_for_status()
    return r.json() or []


def _list_attributes_via_full_search(
    session: MstrRestSession,
    project_id: str,
    modified_since_iso: Optional[str] = None,
) -> list[dict]:
    """Attempt 1 — mstrio.object_management.search_operations.full_search.

    Uses `search_operations.full_search` (not the top-level `full_search`)
    because this deeper import exposes `begin_modification_time` /
    `end_modification_time` parameters and properly handles schema-object
    domains — attributes (type=12) are project-scoped schema objects that
    require `domain=2` (PROJECT) to be enumerated.

    Falls back to the top-level `mstrio.object_management.full_search` when
    the search_operations sub-module is not present (older mstrio-py builds).
    """
    # Prefer the deeper import that carries native date-range support.
    full_search = None
    try:
        from mstrio.object_management.search_operations import full_search  # type: ignore[assignment]
        logger.debug("Using mstrio.object_management.search_operations.full_search")
    except ImportError:
        pass
    if full_search is None:
        try:
            from mstrio.object_management import full_search  # type: ignore[assignment]
            logger.debug("Using mstrio.object_management.full_search (fallback)")
        except ImportError as exc:
            raise ImportError(
                "mstrio-py is required for full_search. Install: pip install mstrio-py"
            ) from exc

    conn = session.mstrio_conn

    # Make sure the mstrio Connection has this project selected — full_search
    # depends on the connection's active project even when `project=` is passed.
    try:
        conn.select_project(project_id=project_id)
        logger.info("select_project({p}) → OK", p=project_id)
    except Exception as exc:
        logger.warning("select_project({p}) failed: {exc}", p=project_id, exc=exc)

    kwargs: dict = dict(
        connection=conn,
        project=project_id,
        object_types=12,    # ATTRIBUTE
        domain=2,           # PROJECT domain — schema objects (attributes) live here
        to_dictionary=True,
        include_hidden=True,
    )
    if modified_since_iso:
        kwargs["begin_modification_time"] = modified_since_iso
        logger.info(
            "search_operations.full_search(project={p}, object_types=12, "
            "begin_modification_time={ts}) ...",
            p=project_id, ts=modified_since_iso,
        )
    else:
        logger.info(
            "search_operations.full_search(project={p}, object_types=12, domain=2) ...",
            p=project_id,
        )

    results = full_search(**kwargs) or []
    logger.info("search_operations.full_search returned {n} attribute(s)", n=len(results))
    return results


def _list_attributes(
    session: MstrRestSession,
    project_id: str,
    modified_since_iso: Optional[str],
) -> list[dict]:
    """
    List every attribute (type=12) in the project using
    mstrio.object_management.search_operations.full_search with domain=2
    (PROJECT), get_ancestors=True, and optional begin_modification_time
    applied server-side when --modified-since is set.
    """
    results = _list_attributes_via_full_search(session, project_id, modified_since_iso) or []
    if not results:
        logger.error(
            "search_operations.full_search returned 0 attributes for project {p}. "
            "Verify the user has read permission on schema objects in this project.",
            p=project_id,
        )
    return results


def _fetch_attribute_def(
    session: MstrRestSession, project_id: str, attr_id: str
) -> Optional[dict]:
    """GET the full attribute definition including all forms and expressions."""
    candidates = [f"/v2/model/attributes/{attr_id}", f"/model/attributes/{attr_id}"]
    headers = {**session.server_headers, "X-MSTR-ProjectID": project_id}
    for path in candidates:
        url = session.api_url + path
        try:
            r = session._session.get(url, headers=headers, timeout=30)
        except Exception:
            continue
        if r.status_code == 404:
            continue
        if not r.ok:
            logger.debug("Attribute fetch {oid} HTTP {s}", oid=attr_id, s=r.status_code)
            continue
        try:
            return r.json()
        except Exception:
            continue
    return None


# --Export workflow --─────────────────────────────────────────────────────────


def _get_attribute_location(
    session: MstrRestSession, project_id: str, attr_id: str
) -> str:
    """
    Return the folder path for an attribute by instantiating
    mstrio.modeling.schema.Attribute and reading its .location property.

    Attribute.location returns the full object path including the attribute
    name as the last segment, e.g.:
        /ProjectName/Schema Objects/Attributes/ProductName
    The last segment is stripped so the result is the containing folder:
        /ProjectName/Schema Objects/Attributes

    Falls back to "" on any error so callers can treat an empty string as
    "location unavailable" without crashing the export.
    """
    try:
        from mstrio.modeling.schema import Attribute as SchemaAttribute  # type: ignore[import]
        attr = SchemaAttribute(session.mstrio_conn, id=attr_id)
        raw = attr.location or ""
        # Strip the trailing attribute-name segment.
        location = raw.rstrip("/").rsplit("/", 1)[0] if "/" in raw else raw
        logger.debug(
            "Attribute {aid}: location resolved → {loc!r}",
            aid=attr_id, loc=location,
        )
        return location
    except Exception as exc:
        logger.debug(
            "Attribute {aid}: could not resolve location via Attribute.location: {exc}",
            aid=attr_id, exc=exc,
        )
        return ""


def _process_attribute(
    session: MstrRestSession,
    project_id: str,
    project_name: str,
    attr_summary: dict,
) -> list[dict]:
    """Return a list of CSV-row dicts for every HTML form-expression on the attribute."""
    # full_search returns native API dicts whose key casing varies across
    # mstrio-py / I-Server versions — accept both forms.
    attr_id = (attr_summary.get("id") or attr_summary.get("object_id") or "").strip()
    attr_name = attr_summary.get("name") or ""

    logger.debug(
        "Processing attribute id={aid} name={aname!r}",
        aid=attr_id, aname=attr_name,
    )

    defn = _fetch_attribute_def(session, project_id, attr_id)
    if not defn:
        logger.debug("Attribute {aid}: definition fetch returned nothing — skipping.", aid=attr_id)
        return []

    forms = defn.get("forms") or []
    logger.debug("Attribute {aid}: {n} form(s) found in definition.", aid=attr_id, n=len(forms))

    # Location is resolved lazily on the first HTML form match.
    # Attribute.location makes an SDK call; deferring it avoids the cost for
    # the majority of attributes that have no HTML forms.
    _resolved_location: Optional[str] = None

    out: list[dict] = []
    for form in forms:
        if not _is_html_form(form):
            continue

        # Resolve location on first HTML-form hit.
        if _resolved_location is None:
            _resolved_location = _get_attribute_location(session, project_id, attr_id)

        attr_location = _resolved_location
        form_id = form.get("id") or ""
        form_name = form.get("name") or ""
        form_display_format = form.get("displayFormat") or ""

        logger.debug(
            "Attribute {aid}: HTML form matched — name={fname!r} id={fid} "
            "displayFormat={fmt!r}",
            aid=attr_id, fname=form_name, fid=form_id, fmt=form_display_format,
        )

        # HTML forms typically have one expression; take its text.
        exprs = form.get("expressions") or []
        if not exprs:
            logger.debug(
                "Attribute {aid} form {fid}: no expressions array — skipping.",
                aid=attr_id, fid=form_id,
            )
            continue
        expr_text = _expression_text(exprs[0])
        if not expr_text:
            logger.debug(
                "Attribute {aid} form {fid}: expression text is empty — skipping.",
                aid=attr_id, fid=form_id,
            )
            continue

        parsed = parse_html_link_expression(expr_text)

        target_doc_id = parsed["target_document_id"]
        elem_attr_id = parsed["element_target_attribute_id"]
        value_form = parsed["target_value_id"]
        display_form = parsed["target_value_name"]

        prompt_id, prompt_key = "", ""
        if target_doc_id and elem_attr_id:
            prompt_id, prompt_key = find_doc_prompt_by_source_attr(
                session, project_id, target_doc_id, elem_attr_id,
            )

        new_expr = ""
        try:
            new_expr = build_new_form_expression(
                target_doc_id=target_doc_id,
                display_form=display_form,
                value_form=value_form,
                element_target_attr_id=elem_attr_id,
                prompt_key=prompt_key,
            )
        except Exception as exc:
            logger.debug(
                "Could not build NewFormExpression for {aid}/{fid}: {exc}",
                aid=attr_id, fid=form_id, exc=exc,
            )
            new_expr = ""

        if new_expr:
            logger.debug(
                "Attribute {aid} form {fid}: NewFormExpression built successfully "
                "({n} chars).",
                aid=attr_id, fid=form_id, n=len(new_expr),
            )
        else:
            missing = [
                name for name, val in [
                    ("target_doc_id", target_doc_id),
                    ("display_form", display_form),
                    ("value_form", value_form),
                    ("element_target_attr_id", elem_attr_id),
                    ("prompt_key", prompt_key),
                ] if not val
            ]
            logger.debug(
                "Attribute {aid} form {fid}: NewFormExpression is blank — "
                "missing fields: {missing}",
                aid=attr_id, fid=form_id, missing=missing,
            )

        row = {
            "ProjectID": project_id,
            "ProjectName": project_name,
            "AttributeID": attr_id,
            "AttributeName": attr_name,
            "AttributeLocation": attr_location,
            "AttributeFormName": form_name,
            "AttributeFormID": form_id,
            "AttributeFormExpression": expr_text,
            "AttributeFormTargetDocumentID": target_doc_id,
            "AttributeFormElementTargetAttributeID": elem_attr_id,
            "AttributeFormTargetValueID": value_form,
            "AttributeFormTargetValueName": display_form,
            "AttributeFormTargetDocumentPromptID": prompt_id,
            "AttributeFormTargetDocumentPromptKey": prompt_key,
            "NewFormExpression": new_expr,
        }
        logger.debug(
            "Attribute {aid} form {fid}: appending row — "
            "targetDoc={doc} promptKey={key!r} newExpr={has_expr}",
            aid=attr_id, fid=form_id,
            doc=target_doc_id or "(none)",
            key=prompt_key or "",
            has_expr=bool(new_expr),
        )
        out.append(row)

    logger.debug(
        "Attribute {aid}: _process_attribute returning {n} row(s).",
        aid=attr_id, n=len(out),
    )
    return out


def _build_rows_for_project(
    session: MstrRestSession,
    project_id: str,
    project_name: str,
    modified_since_iso: Optional[str],
    concurrency: int,
) -> list[dict]:
    logger.info("Project {p} ({pid}): listing attributes…", p=project_name, pid=project_id)
    attrs = _list_attributes(session, project_id, modified_since_iso)
    logger.info("Project {p}: {n} attribute(s) returned by search.", p=project_name, n=len(attrs))
    if not attrs:
        return []

    rows: list[dict] = []
    rows_lock = threading.Lock()

    def _worker(attr_summary: dict) -> None:
        try:
            row_set = _process_attribute(session, project_id, project_name, attr_summary)
        except Exception as exc:
            logger.warning(
                "Processing attribute {aid} failed: {exc}",
                aid=attr_summary.get("id"), exc=exc,
            )
            return
        if row_set:
            with rows_lock:
                rows.extend(row_set)

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(_worker, a) for a in attrs]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as exc:
                logger.warning("Worker raised: {exc}", exc=exc)
    return rows


def _enable_verbose() -> None:
    """Add a DEBUG-level stderr handler on top of the default INFO one."""
    logger.add(sys.stderr, level="DEBUG", colorize=True)


def cmd_export(args: argparse.Namespace) -> int:
    if getattr(args, "verbose", False):
        _enable_verbose()

    config = MstrConfig(environment=MstrEnvironment(args.env))
    output_path = args.output or (config.output_dir / DEFAULT_OUTPUT_NAME)

    modified_since_iso = None
    if args.modified_since:
        try:
            dt = datetime.strptime(args.modified_since, "%Y-%m-%d")
            modified_since_iso = dt.strftime("%Y-%m-%dT00:00:00.000Z")
        except ValueError:
            logger.error("--modified-since must be YYYY-MM-DD; got: {v}", v=args.modified_since)
            return 2
        logger.info("Filtering by modifiedSince={iso}", iso=modified_since_iso)

    attribute_id_filter = None
    if getattr(args, "attribute_id", None):
        attribute_id_filter = {a.strip().upper() for a in args.attribute_id}
        logger.info("Limiting to {n} attribute GUID(s): {ids}",
                    n=len(attribute_id_filter), ids=sorted(attribute_id_filter))

    with MstrRestSession(config) as session:
        all_projects = _list_loaded_projects(session)
        if args.project:
            wanted = {p.strip().upper() for p in args.project}
            projects = [p for p in all_projects if (p.get("id") or "").upper() in wanted]
            missing = wanted - {(p.get("id") or "").upper() for p in projects}
            if missing:
                logger.warning("Project GUIDs not loaded / not found: {ms}", ms=sorted(missing))
            if not projects:
                logger.error("No requested projects were resolvable.")
                return 1
        else:
            projects = all_projects

        all_rows: list[dict] = []
        for p in projects:
            pid = p.get("id") or ""
            pname = p.get("name") or ""
            if not pid:
                continue
            try:
                if attribute_id_filter is not None:
                    # Skip the search — fetch each ID directly. Saves one
                    # round trip per project and lets the user iterate fast.
                    attrs = [{"id": aid, "name": "", "ancestors": []}
                             for aid in attribute_id_filter]
                    rows = []
                    rows_lock = threading.Lock()

                    def _w(a, _pid=pid, _pname=pname):
                        try:
                            sub = _process_attribute(session, _pid, _pname, a)
                        except Exception as exc:
                            logger.warning("Attribute {aid} failed: {exc}", aid=a["id"], exc=exc)
                            return
                        if sub:
                            with rows_lock:
                                rows.extend(sub)

                    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
                        list(pool.map(_w, attrs))
                else:
                    rows = _build_rows_for_project(
                        session, pid, pname, modified_since_iso, args.concurrency,
                    )
                logger.success("Project {p}: {n} HTML form(s) captured.", p=pname, n=len(rows))
                all_rows.extend(rows)
            except Exception as exc:
                logger.error("Project {p} failed: {exc}", p=pname, exc=exc)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", newline="", encoding="utf-8") as fh:
            w = csv_module.writer(fh, delimiter=";", quoting=csv_module.QUOTE_MINIMAL)
            w.writerow(COLUMNS)
            for row in all_rows:
                w.writerow([row.get(c, "") for c in COLUMNS])

        logger.success("Wrote {n} row(s) → {p}", n=len(all_rows), p=output_path)

    # Always print a summary regardless of log level — so the user sees
    # the outcome even if MSTR_LOG_LEVEL is set to WARNING or higher.
    print(f"\nExport complete: {len(all_rows)} row(s) → {output_path}")
    if not all_rows:
        print(
            "  No rows were captured. Possible causes:\n"
            "    - Attributes don't have HTML/HTML Tag forms (check via `debug` on a known ID).\n"
            "    - full_search() returned no attributes for this project — verify user has\n"
            "      read permission on schema objects (attributes are project-scoped).\n"
            "  Re-run with --verbose for full DEBUG logs, or use\n"
            "  `python AttributeFormHtmlManage.py debug <env> --project-id PID --attribute-id AID`\n"
            "  to inspect a single attribute end-to-end."
        )
    return 0


# --Debug workflow --──────────────────────────────────────────────────────────


def _dump_json(label: str, obj: Any) -> None:
    print(f"\n--{label} --")
    print(json.dumps(obj, indent=2, default=str))


def cmd_debug(args: argparse.Namespace) -> int:
    """
    Fetch one attribute and walk the entire export pipeline verbosely,
    printing the raw API response, form classification, expression parsing,
    prompt lookup, and the generated NewFormExpression. After the verbose
    walkthrough the script also runs `_process_attribute()` — the same
    function `export` uses — and prints the row(s) that would be written.
    Pass `--output PATH` to also write those rows to a CSV.
    """
    if args.verbose:
        _enable_verbose()

    config = MstrConfig(environment=MstrEnvironment(args.env))
    pid = args.project_id.strip()
    aid = args.attribute_id.strip()

    print(f"\n==============================================================")
    print(f"  DEBUG  project={pid}  attribute={aid}  env={args.env}")
    print(f"==============================================================")

    with MstrRestSession(config) as session:
        # 1. Fetch attribute definition
        print("\n[1/4] Fetching /model/attributes/{id}...")
        defn = _fetch_attribute_def(session, pid, aid)
        if not defn:
            print("  - FAILED — attribute not found or fetch error.")
            print("    Possible causes: wrong project_id, wrong attribute_id,")
            print("    user lacks read permission, attribute is in unloaded project.")
            return 1

        info = defn.get("information") or {}
        forms = defn.get("forms") or []
        print(f"  + Name:           {info.get('name')!r}")
        print(f"    SubType:        {info.get('subType')!r}")
        print(f"    Date Modified:  {info.get('dateModified')!r}")
        print(f"    Forms total:    {len(forms)}")

        if args.show_raw:
            _dump_json("RAW attribute JSON", defn)

        # 2. Classify each form
        print("\n[2/4] Classifying forms by displayFormat / expression markers:")
        html_forms = []
        for i, form in enumerate(forms):
            fid = form.get("id")
            fname = form.get("name")
            fmt = form.get("displayFormat")
            cat = form.get("category")
            is_html = _is_html_form(form)
            marker = "+ HTML" if is_html else "  --  "
            print(f"  [{i:2}] {marker}  {fname!r:35} id={fid}")
            print(f"           category={cat!r}  displayFormat={fmt!r}")
            if is_html:
                html_forms.append(form)

        if not html_forms:
            print("\n  - No HTML forms on this attribute. Nothing to convert.")
            return 0

        # 3. Per-HTML-form parse + prompt lookup + NewFormExpression
        for form in html_forms:
            fid = form.get("id")
            fname = form.get("name")
            print(f"\n[3/4] Processing HTML form: {fname!r} ({fid})")

            exprs = form.get("expressions") or []
            if not exprs:
                print("  - Form has no expressions array.")
                continue

            text = _expression_text(exprs[0])
            print("\n  expression.text:")
            print(f"    {text}")

            parsed = parse_html_link_expression(text)
            print("\n  parsed:")
            for k, v in parsed.items():
                marker = "+" if v else "-"
                print(f"    {marker} {k:35} = {v!r}")

            tgt = parsed["target_document_id"]
            elem = parsed["element_target_attribute_id"]
            prompt_id, prompt_key = "", ""
            if tgt and elem:
                print(f"\n  Prompt lookup: /documents/{tgt}/prompts  source.id == {elem}")
                if args.show_doc_prompts:
                    # Dump the entire prompts list for the target document
                    candidates = [
                        (f"/v2/documents/{tgt}/prompts", None),
                        (f"/documents/{tgt}/prompts", {"closed": "false"}),
                    ]
                    headers = {**session.server_headers, "X-MSTR-ProjectID": pid}
                    for path, params in candidates:
                        try:
                            r = session._session.get(
                                session.api_url + path, headers=headers,
                                params=params, timeout=30,
                            )
                        except Exception as exc:
                            print(f"    {path} → exception: {exc}")
                            continue
                        if r.status_code == 404:
                            print(f"    {path} → HTTP 404 (try next)")
                            continue
                        if not r.ok:
                            print(f"    {path} → HTTP {r.status_code}  {r.text[:200]}")
                            continue
                        try:
                            data = r.json()
                        except Exception:
                            continue
                        prompts = data if isinstance(data, list) else (data.get("prompts") or [])
                        _dump_json(f"Document prompts ({path})", prompts)
                        break
                prompt_id, prompt_key = find_doc_prompt_by_source_attr(
                    session, pid, tgt, elem,
                )
                m1 = "+" if prompt_id else "-"
                m2 = "+" if prompt_key else "-"
                print(f"    {m1} promptId   = {prompt_id!r}")
                print(f"    {m2} promptKey  = {prompt_key!r}")
            else:
                print("\n  Prompt lookup skipped (missing target_document_id or element_target_attribute_id).")

            # 4. Build the new expression
            print("\n[4/4] Building NewFormExpression:")
            new_expr = build_new_form_expression(
                target_doc_id=tgt,
                display_form=parsed["target_value_name"],
                value_form=parsed["target_value_id"],
                element_target_attr_id=elem,
                prompt_key=prompt_key,
            )
            if new_expr:
                print(f"  + NewFormExpression:\n    {new_expr}")
            else:
                missing = [
                    name for name, val in [
                        ("target_doc_id", tgt),
                        ("display_form", parsed["target_value_name"]),
                        ("value_form", parsed["target_value_id"]),
                        ("element_target_attr_id", elem),
                        ("prompt_key", prompt_key),
                    ] if not val
                ]
                print(f"  - Skipped — missing: {missing}")

        # 5. Run the same code path `export` uses and show the rows that
        #    would be written to CSV. This is the cross-check: if the
        #    walkthrough above showed valid NewFormExpressions but this
        #    section returns 0 rows, there's a bug in _process_attribute.
        print("\n[5/5] Running _process_attribute (same path `export` uses):")
        attr_summary = {
            "id": aid,
            "name": (info.get("name") or ""),
            "ancestors": [],
        }
        rows = _process_attribute(session, pid, "<debug>", attr_summary)
        print(f"  → returned {len(rows)} row(s)")

        for i, r in enumerate(rows, 1):
            print(f"\n  -- Row {i} --")
            for col in COLUMNS:
                val = r.get(col, "")
                if isinstance(val, str) and len(val) > 200:
                    val = val[:200] + f" ... [{len(val)} chars]"
                print(f"    {col:42} = {val!r}")

        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w", newline="", encoding="utf-8") as fh:
                w = csv_module.writer(fh, delimiter=";", quoting=csv_module.QUOTE_MINIMAL)
                w.writerow(COLUMNS)
                for r in rows:
                    w.writerow([r.get(c, "") for c in COLUMNS])
            print(f"\n  Wrote {len(rows)} row(s) → {output_path}")

    print("\n==============================================================\n")
    return 0


# --Offline parse/build test --────────────────────────────────────────────────


def cmd_parse(args: argparse.Namespace) -> int:
    """
    Test the regex parser + new-expression builder against an arbitrary
    expression text — no server calls. Useful when iterating on regexes
    or debugging an exotic expression captured from the CSV.
    """
    if args.text:
        text = args.text
    elif args.text_file:
        text = Path(args.text_file).read_text(encoding="utf-8")
    else:
        print("Provide --text 'Concat(...)' or --text-file PATH.")
        return 2

    print("\nINPUT expression text:")
    print(f"  {text}")

    parsed = parse_html_link_expression(text)
    print("\nPARSED fields:")
    for k, v in parsed.items():
        marker = "+" if v else "-"
        print(f"  {marker} {k:35} = {v!r}")

    new = build_new_form_expression(
        target_doc_id=parsed["target_document_id"],
        display_form=parsed["target_value_name"],
        value_form=parsed["target_value_id"],
        element_target_attr_id=parsed["element_target_attribute_id"],
        prompt_key=args.prompt_key or "<PROMPT_KEY_FROM_DOC_LOOKUP>",
    )
    print("\nBUILT NewFormExpression:")
    if new:
        print(f"  {new}")
    else:
        print("  (blank — required pieces missing; pass --prompt-key to simulate)")
    print()
    return 0


# --Apply workflow --──────────────────────────────────────────────────────────


def _read_csv_rows(path: Path) -> list[dict]:
    """Read CSV with case-insensitive column matching against COLUMNS."""
    if not path.exists():
        logger.error("Input CSV not found: {p}", p=path)
        sys.exit(2)

    col_lookup = {c.lower(): c for c in COLUMNS}

    with open(path, newline="", encoding="utf-8-sig") as fh:
        sample = fh.read(4096)
        fh.seek(0)
        try:
            dialect = csv_module.Sniffer().sniff(sample, delimiters=";,\t")
        except csv_module.Error:
            dialect = "excel"
        reader = csv_module.DictReader(fh, dialect=dialect)
        rows = []
        for raw in reader:
            row: dict[str, str] = {}
            for k, v in raw.items():
                canon = col_lookup.get((k or "").strip().lower())
                if canon:
                    row[canon] = (v or "").strip()
            rows.append(row)
    return rows


def _apply_attribute_form_change(
    session: MstrRestSession,
    project_id: str,
    attribute_id: str,
    form_id: str,
    new_expression: str,
    do_apply: bool,
) -> tuple[bool, str]:
    """
    Open a schema-edit changeset, fetch the attribute, replace the HTML
    form's expression text with `new_expression`, PUT back, commit.
    """
    if not do_apply:
        return True, "DRY-RUN — would update form expression"

    headers = {**session.server_headers, "X-MSTR-ProjectID": project_id}

    # 1. Open schema changeset
    cs_r = session._session.post(
        session.api_url + "/model/changesets?schemaEdit=true",
        headers=headers, timeout=30,
    )
    if not cs_r.ok:
        return False, f"open changeset failed: HTTP {cs_r.status_code} {cs_r.text[:200]}"
    changeset_id = cs_r.json().get("id")
    if not changeset_id:
        return False, "open changeset returned no id"

    cs_headers = {**headers, "X-MSTR-MS-Changeset": changeset_id}

    try:
        # 2. Fetch attribute inside the changeset
        ga = session._session.get(
            session.api_url + f"/model/attributes/{attribute_id}",
            headers=cs_headers, timeout=30,
        )
        if not ga.ok:
            return False, f"fetch attribute failed: HTTP {ga.status_code} {ga.text[:200]}"
        body = ga.json()

        # 3. Locate the form by id; verify it's an HTML form; replace its
        #    expression text. The expression is nested:
        #        forms[i].expressions[j].expression.text = "Concat(...)"
        target_form = None
        for f in body.get("forms") or []:
            if (f.get("id") or "").upper() == form_id.upper():
                target_form = f
                break
        if not target_form:
            return False, f"form {form_id} not found on attribute {attribute_id}"
        if not _is_html_form(target_form):
            return False, f"form {form_id} is not an HTML/HTML Tag form"

        exprs = target_form.get("expressions") or []
        if not exprs:
            return False, f"form {form_id} has no expressions"

        # Update the first expression's text (HTML forms have exactly one)
        _set_expression_text(exprs[0], new_expression)

        # 4. PUT updated attribute body
        pa = session._session.put(
            session.api_url + f"/model/attributes/{attribute_id}",
            headers={**cs_headers, "Content-Type": "application/json"},
            json=body, timeout=60,
        )
        if not pa.ok:
            return False, f"PUT attribute failed: HTTP {pa.status_code} {pa.text[:200]}"

        # 5. Commit
        ca = session._session.post(
            session.api_url + f"/model/changesets/{changeset_id}/commit",
            headers=headers, timeout=30,
        )
        if not ca.ok:
            return False, f"commit failed: HTTP {ca.status_code} {ca.text[:200]}"

        return True, "updated"
    except Exception as exc:
        # Try to roll back the changeset
        try:
            session._session.delete(
                session.api_url + f"/model/changesets/{changeset_id}",
                headers=headers, timeout=30,
            )
        except Exception:
            pass
        return False, f"error: {exc}"


def cmd_apply(args: argparse.Namespace) -> int:
    config = MstrConfig(environment=MstrEnvironment(args.env))

    rows = _read_csv_rows(args.input)
    logger.info("Loaded {n} row(s) from {p}", n=len(rows), p=args.input)

    work = [r for r in rows if (r.get("NewFormExpression") or "").strip()]
    logger.info(
        "{n} of {total} row(s) have NewFormExpression populated.",
        n=len(work), total=len(rows),
    )
    if not work:
        logger.info("Nothing to apply.")
        return 0

    success = 0
    errors = 0

    with MstrRestSession(config) as session:
        for r in work:
            pid = r.get("ProjectID") or ""
            aid = r.get("AttributeID") or ""
            fid = r.get("AttributeFormID") or ""
            new_expr = r.get("NewFormExpression") or ""

            if not (pid and aid and fid and new_expr):
                logger.warning("Row missing required fields: {r}", r={k: r.get(k) for k in ("ProjectID","AttributeID","AttributeFormID")})
                errors += 1
                continue

            ok, msg = _apply_attribute_form_change(
                session, pid, aid, fid, new_expr, args.apply,
            )
            if ok:
                success += 1
                logger.info("OK   {a}/{f} — {m}", a=aid, f=fid, m=msg)
            else:
                errors += 1
                logger.error("FAIL {a}/{f} — {m}", a=aid, f=fid, m=msg)

    logger.info(
        "Summary: success={s} error={e} mode={m}",
        s=success, e=errors, m="apply" if args.apply else "dry-run",
    )
    return 0 if errors == 0 else 1


# --CLI --─────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Export/Apply MicroStrategy AttributeForm HTML expressions. "
            "`export` scans projects for HTML/HTML Tag forms and writes a CSV "
            "with a suggested NewFormExpression; `apply` reads that CSV back "
            "and updates form expressions where NewFormExpression is populated."
        ),
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    pe = sub.add_parser("export", help="Scan for HTML form attributes; write CSV.")
    pe.add_argument("env", choices=[e.value for e in MstrEnvironment])
    pe.add_argument("--output", "-o", type=Path, default=None,
                    help=f"Output CSV (default: <output_dir>/{DEFAULT_OUTPUT_NAME}).")
    pe.add_argument("--project", nargs="+", default=None,
                    help="Limit to specific project GUIDs (default: all loaded projects).")
    pe.add_argument("--attribute-id", nargs="+", default=None,
                    help="Limit to specific attribute GUIDs — skips the project-wide "
                         "search and fetches each ID directly. Combine with --project "
                         "to scope.")
    pe.add_argument("--modified-since", default=None,
                    help="Only include attributes modified on/after YYYY-MM-DD.")
    pe.add_argument("--concurrency", "-c", type=int, default=DEFAULT_CONCURRENCY,
                    help=f"Parallel attribute fetches per project (default: {DEFAULT_CONCURRENCY}).")
    pe.add_argument("--verbose", "-v", action="store_true",
                    help="Enable DEBUG-level stderr logging.")
    pe.set_defaults(func=cmd_export)

    pa = sub.add_parser("apply", help="Apply NewFormExpression updates from CSV.")
    pa.add_argument("env", choices=[e.value for e in MstrEnvironment])
    pa.add_argument("--input", "-i", type=Path, required=True,
                    help="CSV produced by `export` with NewFormExpression populated.")
    pa.add_argument("--apply", action="store_true",
                    help="Commit changes (default: dry-run).")
    pa.add_argument("--verbose", "-v", action="store_true",
                    help="Enable DEBUG-level stderr logging.")
    pa.set_defaults(func=cmd_apply)

    pd = sub.add_parser(
        "debug",
        help="Fetch one attribute and walk the full pipeline verbosely.",
    )
    pd.add_argument("env", choices=[e.value for e in MstrEnvironment])
    pd.add_argument("--project-id", required=True, help="Project GUID containing the attribute.")
    pd.add_argument("--attribute-id", required=True, help="Attribute GUID to inspect.")
    pd.add_argument("--output", "-o", type=Path, default=None,
                    help="Optional CSV path — when set, the row(s) `export` would "
                         "produce for this attribute are written here too.")
    pd.add_argument("--show-raw", action="store_true",
                    help="Print the full raw /model/attributes/{id} response as JSON.")
    pd.add_argument("--show-doc-prompts", action="store_true",
                    help="Print the full prompts list for the target document.")
    pd.add_argument("--verbose", "-v", action="store_true",
                    help="Enable DEBUG-level stderr logging.")
    pd.set_defaults(func=cmd_debug)

    pp = sub.add_parser(
        "parse",
        help="Test regex parser + builder on an expression text (no server calls).",
    )
    src = pp.add_mutually_exclusive_group(required=True)
    src.add_argument("--text", help="Expression text to parse.")
    src.add_argument("--text-file", help="Path to a file containing the expression text.")
    pp.add_argument("--prompt-key", default=None,
                    help="Simulate a prompt key for NewFormExpression preview.")
    pp.set_defaults(func=cmd_parse)

    return p.parse_args()


def main() -> int:
    args = parse_args()
    return args.func(args)


# ─────────────────────────────────────────────────────────────────────────────
# IDE / PyCharm debugging helpers
#
# When you run this file from PyCharm without any CLI parameters (Run / Debug
# with no arguments configured), the block below injects a default sys.argv
# so the script runs against a hardcoded test attribute. Set breakpoints in
# any of these functions to inspect what's happening:
#
#     cmd_export                      — top-level export workflow
#     _list_attributes                — delegates to _list_attributes_via_full_search
#     _list_attributes_via_full_search — search_operations.full_search call
#     _get_attribute_location         — Attribute.location SDK call + path trim
#     _process_attribute              — per-attribute fetch + form parse
#     _fetch_attribute_def            — /model/attributes/{id} fetch
#     find_doc_prompt_by_source_attr  — document-prompts lookup
#     build_new_form_expression       — Concat() builder
#
# To switch test scenarios, comment out the active block in `sys.argv = [...]`
# below and uncomment a different one. The block is bypassed when CLI args
# ARE supplied (so normal command-line use is unaffected).
# ─────────────────────────────────────────────────────────────────────────────

def _ide_debug_argv() -> list[str]:
    """Default sys.argv used when running this file from PyCharm with no args."""
    return [
        sys.argv[0],

        # ── 1) `debug` walkthrough on a single attribute (RECOMMENDED) ───────
        #     Fastest signal — bypasses the search entirely, goes straight to
        #     /model/attributes/{id}, prints every step, optionally writes a CSV.
        "debug", "qa",
        "--project-id",   "DB51FDAA428EACA827892C9A301D6012",
        "--attribute-id", "A60F2B7E4029DF6CFDF4CE8D1915B535",
        "--output",       "c:/tmp/debug.csv",
        "--verbose",

        # ── 2) `export` filtered to one attribute (skips project search) ─────
        # "export", "qa",
        # "--project",      "DB51FDAA428EACA827892C9A301D6012",
        # "--attribute-id", "A60F2B7E4029DF6CFDF4CE8D1915B535",
        # "--verbose",

        # ── 3) full `export` on one project (full search path) ───────────────
        #     Use this to step through _list_attributes' 3-attempt fallback.
        # "export", "qa",
        # "--project",      "DB51FDAA428EACA827892C9A301D6012",
        # "--verbose",

        # ── 4) `apply`, dry run (reads CSV; does not commit) ─────────────────
        # "apply", "qa",
        # "--input", "c:/tmp/attribute_form_html.csv",
        # "--verbose",

        # ── 5) `apply`, COMMIT (rewrites form expressions — be careful) ──────
        # "apply", "qa",
        # "--input", "c:/tmp/attribute_form_html.csv",
        # "--apply",
        # "--verbose",

        # ── 6) offline `parse` test (no MSTR connection needed) ──────────────
        # "parse",
        # "--text", 'Concat("<a  title=""",ProductName,""" href=...)',
        # "--prompt-key", "15B83F05424BFB5A0B84848A9F367801@0@10",
    ]


if __name__ == "__main__":
    # If launched with no CLI args (typical PyCharm Run/Debug), fall into the
    # IDE debug block above. Otherwise pass through to normal CLI parsing.
    if len(sys.argv) == 1:
        sys.argv = _ide_debug_argv()
        print(f"[IDE-DEBUG] sys.argv = {sys.argv[1:]}")
    sys.exit(main())
