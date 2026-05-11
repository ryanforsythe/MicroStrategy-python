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

from mstrio_core import MstrConfig, MstrRestSession, object_location
from mstrio_core.config import MstrEnvironment


# ── Constants ─────────────────────────────────────────────────────────────────

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


# ── Small helpers ─────────────────────────────────────────────────────────────


def _project_request(session: MstrRestSession, method: str, path: str, project_id: str, **kwargs):
    extra = kwargs.pop("headers", {}) or {}
    headers = {"X-MSTR-ProjectID": project_id, **extra}
    if method == "GET":
        return session.get(path, scope="server", headers=headers, **kwargs)
    if method == "POST":
        return session.post(path, scope="server", headers=headers, **kwargs)
    if method == "PUT":
        return session.put(path, scope="server", headers=headers, **kwargs)
    if method == "DELETE":
        return session.delete(path, scope="server", headers=headers, **kwargs)
    raise ValueError(f"Unsupported method: {method}")


# ── Expression parsing ────────────────────────────────────────────────────────

_DOC_ID_RE = re.compile(r"documentID=([0-9A-Fa-f]{32})")
_ELEMENT_ATTR_RE = re.compile(r"elementsPromptAnswers=([0-9A-Fa-f]{32})")
# After "elementsPromptAnswers=AID(;AID)?:", string-close + comma + optional ToString(IDENT)
_VALUE_REF_RE = re.compile(
    r'elementsPromptAnswers=[0-9A-Fa-f]{32}(?:;[0-9A-Fa-f]{32})?:'
    r'"\s*,\s*'
    r'(?:ToString\s*\(\s*)?([A-Za-z_][A-Za-z0-9_ ]*?)\s*[\),]'
)
# Display name: title=""",NAME,
_DISPLAY_REF_RE = re.compile(r'title="""\s*,\s*([A-Za-z_][A-Za-z0-9_ ]*?)\s*,')


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
    m = _DISPLAY_REF_RE.search(s)
    if m:
        out["target_value_name"] = m.group(1).strip()
    return out


# ── New expression builder ────────────────────────────────────────────────────

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
    """
    Build the replacement Concat() expression using a relative URL so the
    browser keeps the current host / app / project context:

        Concat("<a title=""",NAME,""" href=""../{DOC}?prompts={JSON_URL_ENCODED}""
                target=""_blank"">",NAME,"</a>")

    Returns "" if any required input is missing.
    """
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


# ── Document prompt lookup ────────────────────────────────────────────────────


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


# ── Attribute search / detail fetch ───────────────────────────────────────────


def _is_html_expression(expr_obj: dict) -> bool:
    """True when this form-expression dict represents an HTML/HTML Tag form."""
    fmt = (expr_obj.get("displayFormat") or expr_obj.get("format") or "").upper()
    if fmt in HTML_DISPLAY_FORMATS:
        return True
    text = expr_obj.get("expression") or expr_obj.get("text") or ""
    return bool(HTML_EXPRESSION_MARKERS.search(text))


def _list_loaded_projects(session: MstrRestSession) -> list[dict]:
    r = session.get("/projects", scope="server")
    r.raise_for_status()
    return r.json() or []


def _list_attributes(
    session: MstrRestSession,
    project_id: str,
    modified_since_iso: Optional[str],
) -> list[dict]:
    """Page through `/searches/results?type=12` for the project."""
    attrs: list[dict] = []
    offset = 0
    limit = 200
    while True:
        params: dict[str, Any] = {
            "type": OBJECT_TYPE_ATTRIBUTE,
            "limit": limit,
            "offset": offset,
            "includeAncestors": "true",
            "fields": "id,name,dateModified,ancestors,subtype",
        }
        if modified_since_iso:
            params["modifiedSince"] = modified_since_iso
        r = _project_request(session, "GET", "/searches/results", project_id, params=params)
        if not r.ok:
            logger.error(
                "Attribute search failed for project {p}: HTTP {s} {b}",
                p=project_id, s=r.status_code, b=r.text[:200],
            )
            r.raise_for_status()
        data = r.json()
        page = data.get("result") or []
        attrs.extend(page)
        total = int(data.get("totalItems") or len(attrs))
        offset += limit
        if not page or len(attrs) >= total:
            break
    return attrs


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


# ── Export workflow ───────────────────────────────────────────────────────────


def _process_attribute(
    session: MstrRestSession,
    project_id: str,
    project_name: str,
    attr_summary: dict,
) -> list[dict]:
    """Return a list of CSV-row dicts for every HTML form-expression on the attribute."""
    attr_id = attr_summary.get("id") or ""
    attr_name = attr_summary.get("name") or ""
    attr_location = object_location(attr_summary.get("ancestors") or [])

    defn = _fetch_attribute_def(session, project_id, attr_id)
    if not defn:
        return []

    out: list[dict] = []
    for form in defn.get("forms") or []:
        form_id = form.get("id") or ""
        form_name = form.get("name") or ""
        for expr_obj in form.get("expressions") or []:
            if not _is_html_expression(expr_obj):
                continue

            expr_text = expr_obj.get("expression") or expr_obj.get("text") or ""
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

            out.append({
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
            })
            # Only the first HTML expression per form
            break
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


def cmd_export(args: argparse.Namespace) -> int:
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

    return 0


# ── Apply workflow ────────────────────────────────────────────────────────────


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

        # 3. Locate the form, replace expression text on the HTML expression
        target_form = None
        for f in body.get("forms") or []:
            if (f.get("id") or "").upper() == form_id.upper():
                target_form = f
                break
        if not target_form:
            return False, f"form {form_id} not found on attribute {attribute_id}"

        replaced = False
        for expr in target_form.get("expressions") or []:
            if _is_html_expression(expr):
                expr["expression"] = new_expression
                # Drop any cached parse; the server will re-tokenise from text.
                expr.pop("tree", None)
                expr.pop("tokens", None)
                replaced = True
                break
        if not replaced:
            return False, f"no HTML expression on form {form_id}"

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


# ── CLI ───────────────────────────────────────────────────────────────────────


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
    pe.add_argument("--modified-since", default=None,
                    help="Only include attributes modified on/after YYYY-MM-DD.")
    pe.add_argument("--concurrency", "-c", type=int, default=DEFAULT_CONCURRENCY,
                    help=f"Parallel attribute fetches per project (default: {DEFAULT_CONCURRENCY}).")
    pe.set_defaults(func=cmd_export)

    pa = sub.add_parser("apply", help="Apply NewFormExpression updates from CSV.")
    pa.add_argument("env", choices=[e.value for e in MstrEnvironment])
    pa.add_argument("--input", "-i", type=Path, required=True,
                    help="CSV produced by `export` with NewFormExpression populated.")
    pa.add_argument("--apply", action="store_true",
                    help="Commit changes (default: dry-run).")
    pa.set_defaults(func=cmd_apply)

    return p.parse_args()


def main() -> int:
    args = parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
