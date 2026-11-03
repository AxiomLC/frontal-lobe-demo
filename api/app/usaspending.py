# ===============  usaspending.py — the USASpending federal-award leg  ===============
# The demo's ONE evidence leg: reads the actual federal contract/grant/subaward
# records behind a company from USASpending.gov and folds them into a small
# knowledge graph.
#   * FULLY DETERMINISTIC — no LLM anywhere in fetch/rank/build. The award's own
#     description IS the summary and the PRODUCT node.
#   * KEYLESS — the USASpending.gov API needs no key; this leg runs with nothing
#     but a Neo4j connection.
#   * IDEMPOTENT by recipe stamp — re-running at the same AWARD_RECIPE_VERSION
#     writes nothing; bump the version to rebuild.
# Node set (locked against live probes - see TECH_README section 10):
#   TICKER . FEDERAL_AWARD (role prime|subcontract, kind contract|grant)
#   . PRODUCT (description, COVERS) . COMPANY (prime on a subcontract, AWARDED_BY)
#   . AWARD_INVENTORY (one portfolio node per ticker) . rare SUBSIDIARY.
# Run as a module from the repo root (relative imports):
#   python -m api.app.usaspending --tickers IONQ RGTI          # ingest
#   python -m api.app.usaspending --tickers IONQ --dry-run     # fetch+plan only
#   python -m api.app.usaspending --tickers IONQ --export sample_data/IONQ_awards.json
# The server calls set_emitter() so pipeline events stream to the UI log.
import json
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

from .award_config import (
    AGENCY_ABBREV,
    AWARD_API_TIMEOUT_S,
    AWARD_DETAIL_PARALLEL,
    AWARD_FETCH_WINDOW_DAYS,
    AWARD_FETCH_WORKERS,
    AWARD_INVENTORY_MAX_LINES,
    AWARD_MAX_RECORDS_PER_UEI,
    AWARD_MIN_AMOUNT,
    AWARD_NAME_KEYS,
    AWARD_PAGE_SIZE,
    AWARD_RECIPE_VERSION,
    AWARD_TYPE_CONTRACTS,
    AWARD_TYPE_GRANTS,
    MAX_AWARDS_PER_TICKER,
    SUBAWARD_MAX_RECORDS_PER_UEI,
    SUBAWARD_MIN_AMOUNT,
)
from .writer import canonical_name, has_stamp, snapshot, stale_cleanup, write_plan

API = "https://api.usaspending.gov/api/v2"

# One award run at a time.
_award_lock = threading.Lock()

# Pipeline-event emitter. The server swaps this for app.events.emit so the log
# streams; the CLI keeps print.
_emit = lambda level, message, ticker="", job_id="", leg="usaspending": (
    print(f"[{level}] {message}")
)


def set_emitter(fn):
    """Point pipeline events at a UI log (server) vs stdout (CLI)."""
    global _emit
    _emit = fn if callable(fn) else _emit


# Search fields — the award-level rows carry these; type/description/NAICS need
# the per-award detail call.
_FIELDS = [
    "Award ID", "Recipient Name", "Recipient UEI", "Award Amount",
    "Start Date", "End Date", "Awarding Agency", "generated_internal_id",
]

# Subaward search fields — HYPHENATED names (not "Subaward..." — that 404s);
# the recipient search matches the SUB-AWARDEE.
_SUB_FIELDS = [
    "Sub-Award ID", "Sub-Award Type", "Sub-Award Date", "Sub-Award Amount",
    "Sub-Awardee Name", "Sub-Recipient UEI", "Prime Award ID",
    "Prime Recipient Name", "Prime Award Recipient UEI", "Prime Awarding Agency",
    "prime_award_generated_internal_id", "Description",
]


# ─────────────────────────────────────────────────────────────
# 1. API helpers (keyless)
# ─────────────────────────────────────────────────────────────
def _post(path, body, timeout=AWARD_API_TIMEOUT_S):
    """POST a JSON endpoint. Retries transient 429/502/503/504 with backoff."""
    req = urllib.request.Request(
        f"{API}/{path}", data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code in (429, 502, 503, 504) and attempt < 2:
                time.sleep(1.5 * (attempt + 1))
                continue
            _emit("warn", f"USASpending {path} HTTP {e.code}")
            return None
        except Exception as e:  # noqa: BLE001 - best-effort
            if attempt < 2:
                time.sleep(1.0)
                continue
            _emit("warn", f"USASpending {path} err — {type(e).__name__}")
            return None
    return None


def _get(path, timeout=AWARD_API_TIMEOUT_S):
    req = urllib.request.Request(f"{API}/{path}")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:  # noqa: BLE001 - best-effort
        _emit("warn", f"USASpending GET {path} err — {type(e).__name__}")
        return None


# ─────────────────────────────────────────────────────────────
# 2. Entity source — names + UEIs (deterministic runtime resolution)
# ─────────────────────────────────────────────────────────────
_uei_cache: dict[str, dict[str, str]] = {}


def _resolve_ueis(ticker):
    """UEI -> recipient name for this ticker. Deterministic runtime resolution:
    recipient-profile search per name variant (keyless). Bounded + filtered to
    names that actually contain the variant (the profile search also matches
    UEI/DUNS substrings — "ASML" pulled Tribal Tech LLC)."""
    if ticker in _uei_cache:
        return _uei_cache[ticker]
    seen = {}
    for variant in (AWARD_NAME_KEYS.get(ticker, {}).get("names") or [])[:4]:
        data = _post("recipient/", {"keyword": variant}, timeout=30)
        vlow = variant.lower()
        for x in (data or {}).get("results") or []:
            uei = x.get("uei")
            rname = str(x.get("name") or "")
            if not uei:
                continue
            if vlow not in rname.lower():
                continue
            seen.setdefault(str(uei), rname)
    _uei_cache[ticker] = seen
    return seen

# ─────────────────────────────────────────────────────────────
# 3. Fetch + normalize (deterministic)
# ─────────────────────────────────────────────────────────────
def _normalize_award(row, group):
    return {
        "piid": row.get("Award ID") or "",
        "ref_id": row.get("Award ID") or "",
        "recipient": row.get("Recipient Name") or "",
        "recipient_uei": row.get("Recipient UEI") or "",
        "amount": float(row.get("Award Amount") or 0),
        "start": row.get("Start Date") or "",
        "end": row.get("End Date") or "",
        "agency": row.get("Awarding Agency") or "",
        "gid": row.get("generated_internal_id") or "",
        "group": group,  # "contracts" | "grants"
        "role": "prime",
    }


def _normalize_subaward(row, group):
    desc = row.get("Description")
    if desc is None or str(desc).strip().lower() == "none":
        desc = ""
    else:
        desc = re.sub(r"\s+", " ", str(desc)).strip()
    return {
        "sub_id": row.get("Sub-Award ID") or "",
        "ref_id": row.get("Sub-Award ID") or "",
        "sub_type": row.get("Sub-Award Type") or "",
        "sub_date": row.get("Sub-Award Date") or "",
        "recipient": row.get("Sub-Awardee Name") or "",
        "recipient_uei": row.get("Sub-Recipient UEI") or "",
        "prime_piid": row.get("Prime Award ID") or "",
        "prime_recipient": row.get("Prime Recipient Name") or "",
        "prime_recipient_uei": row.get("Prime Award Recipient UEI") or "",
        "prime_gid": row.get("prime_award_generated_internal_id") or "",
        "description": desc,
        "agency": row.get("Prime Awarding Agency") or "",
        "amount": float(row.get("Sub-Award Amount") or 0),
        "group": group,
        "role": "subcontract",
        "gid": "",
    }


def _fetch_awards(uei):
    """In-window award rows for one UEI - contracts + grants as SEPARATE calls
    (mixing the type-code groups 422s), date_signed semantics. Paginated,
    deduped downstream by award id."""
    start = (date.today() - timedelta(days=AWARD_FETCH_WINDOW_DAYS)).isoformat()
    end = date.today().isoformat()
    rows = []
    for group, codes in (("contracts", AWARD_TYPE_CONTRACTS), ("grants", AWARD_TYPE_GRANTS)):
        page = 1
        while True:
            data = _post("search/spending_by_award/", {
                "filters": {
                    "award_type_codes": codes,
                    "recipient_search_text": [uei],
                    "time_period": [{"start_date": start, "end_date": end, "date_type": "date_signed"}],
                },
                "fields": _FIELDS, "limit": AWARD_PAGE_SIZE, "page": page,
                "sort": "Award Amount", "order": "desc",
            })
            page_rows = (data or {}).get("results") or []
            if not page_rows:
                break
            for r in page_rows:
                rec = _normalize_award(r, group)
                rec["src_uei"] = uei
                rows.append(rec)
            pm = (data or {}).get("page_metadata") or {}
            if not pm.get("hasNext") or len(rows) >= AWARD_MAX_RECORDS_PER_UEI:
                break
            page += 1
    return rows


def _in_window(datestr, start, end):
    d = (datestr or "").strip()
    return bool(d and start <= d <= end)

def _fetch_subawards(uei):
    """In-window SUBaward action rows matching one UEI (the sub-awardee).
    Top-level "subawards": True; NO sort and NO date_type (both break the sub
    query - "date_signed" returns 0 rows) - the leg post-filters by Sub-Award
    Date in Python."""
    start = (date.today() - timedelta(days=AWARD_FETCH_WINDOW_DAYS)).isoformat()
    end = date.today().isoformat()
    rows = []
    for group, codes in (("contracts", AWARD_TYPE_CONTRACTS), ("grants", AWARD_TYPE_GRANTS)):
        page = 1
        fetched = 0
        while True:
            body = {
                "subawards": True,
                "filters": {
                    "award_type_codes": codes,
                    "recipient_search_text": [uei],
                    "time_period": [{"start_date": start, "end_date": end}],
                },
                "fields": _SUB_FIELDS, "limit": AWARD_PAGE_SIZE, "page": page,
            }
            data = _post("search/spending_by_award/", body)
            if data is None and "Description" in body["fields"]:
                body["fields"] = [f for f in body["fields"] if f != "Description"]
                data = _post("search/spending_by_award/", body)
            page_rows = (data or {}).get("results") or []
            if not page_rows:
                break
            fetched += len(page_rows)
            for r in page_rows:
                rec = _normalize_subaward(r, group)
                rec["src_uei"] = uei
                if _in_window(rec.get("sub_date") or "", start, end):
                    rows.append(rec)
            pm = (data or {}).get("page_metadata") or {}
            if not pm.get("hasNext") or fetched >= SUBAWARD_MAX_RECORDS_PER_UEI * 4:
                break
            page += 1
    return rows


def _aggregate_subawards(rows):
    """One subaward = many action rows (modifications incl. negatives): group by
    (Sub-Award ID, prime award), net = sum of the action amounts."""
    groups = {}
    for r in rows:
        key = (r["sub_id"], r["prime_piid"])
        g = groups.get(key)
        if g is None:
            g = dict(r)
            g["amount"] = 0.0
            g["actions"] = 0
            groups[key] = g
        g["amount"] += float(r.get("amount") or 0)
        g["actions"] += 1
    out = list(groups.values())
    out.sort(key=lambda r: (r["amount"], r.get("sub_date") or ""), reverse=True)
    return out


def _award_detail(gid):
    """GET /awards/{generated_internal_id}/ - fills type code, description,
    total obligation, agency chain (toptier/subtier/office)."""
    if not gid:
        return {}
    d = _get(f"awards/{urllib.parse.quote(gid)}/")
    if not d:
        return {}
    ag = d.get("awarding_agency") or {}
    return {
        "type_code": d.get("type") or "",
        "description": re.sub(r"\s+", " ", (d.get("description") or "")).strip(),
        "total": d.get("total_obligation"),
        "toptier": (ag.get("toptier_agency") or {}).get("name") or "",
        "subtier": (ag.get("subtier_agency") or {}).get("name") or "",
        "office": ag.get("office_agency_name") or "",
    }


def _agency_short(name):
    if not name:
        return ""
    if name in AGENCY_ABBREV:
        return AGENCY_ABBREV[name]
    words = [w for w in re.split(r"\s+", name) if w and w.lower() not in ("of", "the", "and")]
    return " ".join(words[:2]).upper()[:12] if words else name[:12].upper()


def _fmt_usd(amount):
    if amount >= 1_000_000_000:
        return f"${amount / 1_000_000_000:.1f}B"
    if amount >= 1_000_000:
        return f"${amount / 1_000_000:.1f}M"
    return f"${amount:,.0f}"


def _clean_sub_name(sub):
    nm = (sub or "").strip()
    m = re.search(r"^(.*?\b(?:N\.V\.|Ltd\.|LLC|Inc\.|Co\.|GmbH|B\.V\.))\s*(?:\d+\s*:?|P\.O\.)\s*$", nm, re.I)
    if m:
        nm = m.group(1).strip()
    return nm or (sub or "").strip()


_JUNK_DESC_MARKERS = (
    "new award control number", "see above", "various", "n/a",
    "none", "tbd", "to be determined",
)


def _is_junk_product_desc(desc):
    d = re.sub(r"\s+", " ", desc or "").strip()
    if len(d) < 5:
        return True
    if re.match(r"^\d{7,}", d):
        return True
    low = d.lower()
    return any(m in low for m in _JUNK_DESC_MARKERS)


_ACRONYM_KEEP = frozenset({
    "qgg", "sbir", "afrl", "nasa", "dod", "doe", "nsf", "darpa", "nsn",
    "r&d", "uei", "piid", "dhs", "nih", "usaf", "usn", "usa", "lti",
    "gps", "lidar", "laser", "rf", "hv", "uv", "ir", "ph", "b2b", "b2c",
})
_ROMAN_RE = re.compile(r"^[IVXLCDM]+$")


def _product_name(desc):
    name = re.sub(r"\s+", " ", desc or "").strip(" .,;")
    name = name[:80].rstrip(" .,;")
    if name and name.isupper():
        words = []
        for w in name.split():
            if (
                w.lower() in _ACRONYM_KEEP
                or re.search(r"\d", w)
                or (_ROMAN_RE.match(w) and len(w) >= 2)
            ):
                words.append(w)
            else:
                words.append(w.title())
        name = " ".join(words)
    return name

# ─────────────────────────────────────────────────────────────
# 4. Fold — awardee onto the TICKER/SUB spine (never a bare COMPANY for awardees)
# ─────────────────────────────────────────────────────────────
def _primary_label(labels):
    for t in ("TICKER", "SUBSIDIARY", "COMPANY"):
        if t in labels:
            return t
    return labels[0] if labels else ""


def _known_parents(snapshot):
    """id + primary label for every existing TICKER/SUBSIDIARY/COMPANY - the
    awardee-fold match targets."""
    out = {}
    for eid, ent in (snapshot.get("entities") or {}).items():
        labs = ent.get("labels") or []
        if not any(l in ("TICKER", "SUBSIDIARY", "COMPANY") for l in labs):
            continue
        cn = canonical_name(ent.get("name") or "")
        if cn:
            out.setdefault(cn, (eid, _primary_label(labs)))
    return out


def _mint_sub(ticker, awardee, cn, parent_id, plan, tid):
    awardee = _clean_sub_name(awardee)
    if not awardee:
        return "", ""
    sid = f"subsidiary:{cn}"
    if sid in plan["entities"]:
        return sid, "SUBSIDIARY"
    plan["entities"][sid] = {
        "id": sid,
        "name": awardee,
        "label": "SUBSIDIARY",
        "props": {
            "ticker": ticker,
            "source": "USASpending",
            "award_recipe_version": AWARD_RECIPE_VERSION,
            "summary": f"{awardee} - Subsidiary of {ticker} (USASpending leg)",
        },
    }
    plan["relations"].append(
        {"source_id": sid, "target_id": tid, "label": "SUBSIDIARY_OF", "props": {}}
    )
    return sid, "SUBSIDIARY"


def _fold_awardee(ticker, awardee, cn, known_parents, plan, tid, family_names, own_identity):
    """Awardee fold ladder:
    1. exact canonical match on an existing TICKER/SUBSIDIARY -> attach there
    2. SAM-alias map -> ticker, or mint a NEW-SUB under the ticker
    3. family-prefix match -> NEW-SUB under that parent
    4. own-name-forms fallback -> the parent attaches to the TICKER
    5. no match -> ("", "INVENTORY") - the award still lands in the ticker's
       AWARD_INVENTORY chunk (never a bare COMPANY).
    Returns (entity_id, label)."""
    hit = known_parents.get(cn)
    if hit and hit[1] != "COMPANY":
        return hit
    # aliases are SAM/legal spellings ("rigetti & co"). canonical_name over-strips
    # short keys ("rigetti & co" -> "rigetti"), so match by TOKEN PREFIX against
    # the recipient's canonical instead: "rigetti co" is a prefix of the awardee
    # "RIGETTI & CO, LLC" (-> "rigetti co llc"), so it folds to the TICKER.
    cn_tokens = cn.split() if cn else []
    target = None
    alias_rank = -1
    for alias_key, alias_val in (AWARD_NAME_KEYS.get(ticker, {}).get("aliases") or {}).items():
        at = [w for w in re.sub(r"[^a-z0-9 ]", " ", (alias_key or "").lower()).split() if w]
        if (
            at and len(cn_tokens) >= len(at)
            and cn_tokens[: len(at)] == at
            and len(at) > alias_rank
        ):
            target = alias_val
            alias_rank = len(at)
    if target == "ticker":
        return tid, "TICKER"
    if target == "sub":
        return _mint_sub(ticker, awardee, cn, tid, plan, tid)
    best = None
    for pcn, (pid, plabel) in known_parents.items():
        if plabel == "COMPANY":
            continue
        if cn != pcn and cn.startswith(pcn + " ") and (best is None or len(pcn) > len(best[0])):
            best = (pcn, (pid, plabel))
    if best:
        return _mint_sub(ticker, awardee, cn, best[1][0], plan, tid)
    if family_names and any(cn == f or cn.startswith(f + " ") for f in family_names):
        if any(cn == i or i.startswith(cn + " ") for i in own_identity):
            return tid, "TICKER"
        return _mint_sub(ticker, awardee, cn, tid, plan, tid)
    return "", "INVENTORY"


def _fold_prime(ticker, prime_name, known_parents, plan):
    """The PRIME-contractor fold (subawards only): exact canonical match vs ANY
    existing TICKER/SUBSIDIARY/COMPANY -> attach there (the cross-cluster hook -
    BAE / Honeywell / GD also appear in SEC contracts); else MINT a COMPANY node.
    Returns (entity_id, label)."""
    cn = canonical_name(prime_name)
    if not cn:
        return "", ""
    hit = known_parents.get(cn)
    if hit:
        return hit
    cid = f"company:{cn}"
    if cid in plan["entities"]:
        return cid, "COMPANY"
    plan["entities"][cid] = {
        "id": cid,
        "name": prime_name,
        "label": "COMPANY",
        "props": {
            "ticker": ticker,
            "source": "USASpending",
            "award_recipe_version": AWARD_RECIPE_VERSION,
            "summary": (
                f"{prime_name} - prime contractor paying {ticker} via a "
                "USASpending subaward"
            ),
        },
    }
    return cid, "COMPANY"

# ─────────────────────────────────────────────────────────────
# 5. Node content — summary, chunk0 (the Viewer's "whole record")
# ─────────────────────────────────────────────────────────────
def _award_summary(r, det, kind):
    toptier = det.get("toptier") or r["agency"]
    subtier = det.get("subtier") or ""
    agency_s = toptier if (not subtier or subtier == toptier) else f"{toptier} ({subtier})"
    head = (
        f'A {kind} award to {r["recipient"]} worth {_fmt_usd(r["amount"])} '
        f"from {agency_s}"
        + f" - {r['start']} to {r['end'] or 'open'}."
    )
    desc = det.get("description")
    return f'{head} {desc[:240] + "..." if desc and len(desc) > 240 else desc or ""}'.strip()


def _award_label(desc):
    """FEDERAL_AWARD node name: prefer the PROJECT/TITLE segment of the
    description, else strip the envelope junk + cap at 120. Original casing
    preserved."""
    d = re.sub(r"\s+", " ", desc or "").strip(" .,;")
    if not d:
        return d
    m = re.search(
        r"(?:project|program)\s+title\s*:\s*([^:]{4,160}?)\s*(?:description\s*:|$)",
        d, re.IGNORECASE,
    )
    lab = m.group(1).strip(" \"'.,;-") if m else ""
    if len(lab.split()) >= 2:
        return lab[:120].rstrip(" .,;")
    low = d.lower()
    for marker in _JUNK_DESC_MARKERS + ("description", "project title"):
        idx = low.find(marker)
        if idx >= 0:
            d = d[:idx].rstrip(" :,;-")
            low = d.lower()
            break
    return d[:120].rstrip(" .,;") or desc[:120]


def _award_chunk(r, det, kind):
    """chunk0 - the full award record (the Viewer shows this whole text)."""
    toptier = det.get("toptier") or r["agency"]
    subtier = det.get("subtier") or ""
    agency_s = toptier if (not subtier or subtier == toptier) else f"{toptier} / {subtier}"
    lines = [
        f"Federal {kind} award - {det.get('description') or r['piid']}",
        f"Award ID (PIID): {r['piid']}",
        f"Recipient: {r['recipient']} - UEI {r['recipient_uei']}",
        f"Amount: {_fmt_usd(r['amount'])} ({r['amount']:,.0f} USD total obligation)",
        f"Awarding agency: {agency_s}"
        + (f" ({det.get('office')})" if det.get("office") else ""),
        f"Period of performance: {r['start']} -> {r['end'] or 'open'}",
        f"Type code: {det.get('type_code') or 'n/a'}",
    ]
    desc = det.get("description")
    if desc:
        lines.append(f"Description: {desc}")
    return "\n".join(lines)


def _sub_summary(r, det):
    det = det or {}
    agency = det.get("toptier") or r["agency"] or ""
    return (
        f"A subcontract award to {r['recipient']} worth {_fmt_usd(r['amount'])} "
        f"from {r['prime_recipient'] or 'a prime contractor'} "
        f"({agency or 'n/a'}) on prime {r['prime_piid'] or 'n/a'}."
    )


def _sub_chunk(r, kind, det):
    det = det or {}
    agency = det.get("toptier") or r["agency"] or ""
    subtier = det.get("subtier") or ""
    agency_s = agency if (not subtier or subtier == agency) else f"{agency} / {subtier}"
    lines = [
        (
            "Federal subcontract award - "
            f"{r.get('description') or det.get('description') or r['sub_id']}"
        ),
        f"Sub-Award ID: {r['sub_id']} (type {r['sub_type'] or 'n/a'})",
        f"Sub-Awardee: {r['recipient']} - UEI {r['recipient_uei']}",
        (
            f"Amount: {_fmt_usd(r['amount'])} ({r['amount']:,.0f} USD net of "
            f"{r.get('actions', 1)} action(s))"
        ),
        (
            f"Prime contract: {r['prime_recipient'] or 'n/a'} "
            f"(UEI {r['prime_recipient_uei'] or 'n/a'}) - {r['prime_piid'] or 'n/a'}"
        ),
        f"Awarding agency: {agency_s or 'n/a'}",
        f"Sub-Award Date: {r['sub_date'] or 'n/a'}",
    ]
    desc = r.get("description") or det.get("description") or ""
    if desc:
        lines.append(f"Description: {desc}")
    return "\n".join(lines)


def _inventory_lines(records, cap):
    lines = []
    for i, r in enumerate(records[:cap], start=1):
        tag = " (sub)" if r.get("role") == "subcontract" else ""
        ref = r.get("ref_id") or r.get("piid") or r.get("sub_id") or ""
        lines.append(
            f"{i}. {_fmt_usd(r['amount'])}{tag} - "
            f"{_agency_short(r['agency']) or r['agency']} - "
            f"{r['recipient']} - {ref}"
        )
    if len(records) > cap:
        lines.append(f"... +{len(records) - cap} more")
    return lines

# ─────────────────────────────────────────────────────────────
# 6. Per-ticker plan
# ─────────────────────────────────────────────────────────────
def _ticker_plan(ticker, snapshot, known_parents, job_id=""):
    """Fetch -> floor -> dedup -> rank -> fold -> build FEDERAL_AWARD /
    AWARD_INVENTORY plan for ONE ticker. Pure compute + keyless API; the write
    happens later, all-or-nothing."""
    plan = {"entities": {}, "relations": [], "chunks": [], "stats": {}}
    _emit("info", f">{ticker}: USASpending - resolving UEIs...", ticker, job_id)
    ueis = _resolve_ueis(ticker)
    if not ueis:
        _emit("warn", f"{ticker}: no UEIs found on USAspending - skipped", ticker, job_id)
        return None

    _emit("info", f">{ticker}: fetching awards in the {AWARD_FETCH_WINDOW_DAYS}d window...", ticker, job_id)
    raw, sub_raw = [], []
    for uei in ueis:
        raw += _fetch_awards(uei)
    for uei in ueis:
        sub_raw += _fetch_subawards(uei)
    plan["stats"]["records"] = len(raw) + len(sub_raw)
    plan["stats"]["prime_raw"] = len(raw)
    plan["stats"]["sub_raw"] = len(sub_raw)

    # prime: dedup by award id + the prime floor
    seen = {}
    for r in raw:
        key = r["gid"] or r["piid"]
        if not key:
            continue
        if r["amount"] < AWARD_MIN_AMOUNT:
            continue
        seen.setdefault(key, r)
    records = sorted(seen.values(), key=lambda r: (r["amount"], r["start"] or ""), reverse=True)

    # sub: net-sum per Sub-Award ID + the sub floor
    sub_records = [r for r in _aggregate_subawards(sub_raw) if r["amount"] >= SUBAWARD_MIN_AMOUNT]

    if not records and not sub_records:
        _emit(
            "warn",
            f"{ticker}: no awards >= {_fmt_usd(AWARD_MIN_AMOUNT)} prime / "
            f"{_fmt_usd(SUBAWARD_MIN_AMOUNT)} sub in the window "
            f"(from {len(raw) + len(sub_raw)} fetched)",
            ticker, job_id,
        )
        return None

    tid = f"ticker_parent:{ticker}"
    ticker_ent = snapshot["entities"].get(tid) or {}
    company = (
        ticker_ent.get("props", {}).get("company_name")
        or ticker_ent.get("name")
        or ticker
    )
    cfg = AWARD_NAME_KEYS.get(ticker, {})
    aliases = cfg.get("aliases") or {}
    own_identity = {ticker.lower(), canonical_name(company)} | {
        k for k, v in aliases.items() if v == "ticker"
    }
    family_names = {canonical_name(n) for n in (cfg.get("names") or [])} | set(aliases)

    all_records = sorted(
        records + sub_records,
        key=lambda r: (r["amount"], r.get("sub_date") or r.get("start") or ""),
        reverse=True,
    )
    owner_cache = {}
    for r in all_records:
        cn = canonical_name(r["recipient"])
        if cn and cn in owner_cache:
            eid, label = owner_cache[cn]
        else:
            eid, label = _fold_awardee(
                ticker, r["recipient"], cn, known_parents, plan, tid,
                family_names, own_identity,
            )
            if not eid:
                # UEI-family fallback: the award's recipient NAME can be a sub
                # ("GLOBE MOTORS") under one of OUR family UEIs. Retry with the
                # SEARCH-UEI's registered name (the fetch key IS family scope).
                src = r.get("src_uei") or ""
                uei_name = ueis.get(src) or ""
                ucn = canonical_name(uei_name) if uei_name else ""
                if ucn and ucn != cn:
                    eid, label = _fold_awardee(
                        ticker, uei_name, ucn, known_parents, plan, tid,
                        family_names, own_identity,
                    )
            if cn:
                owner_cache[cn] = (eid, label)
        r["owner_entity"], r["owner_label"] = eid, label

    folded = [r for r in all_records if r["owner_entity"]]
    featured = folded[:MAX_AWARDS_PER_TICKER]
    nonfeatured = [r for r in all_records if r not in featured]
    plan["stats"]["featured_total"] = len(featured)

    if tid not in snapshot["entities"] and tid not in plan["entities"] and featured:
        plan["entities"][tid] = {
            "id": tid,
            "name": company,
            "label": "TICKER",
            "props": {
                "ticker": ticker,
                "company_name": company,
                "award_recipe_version": AWARD_RECIPE_VERSION,
            },
        }

    # detail for featured PRIME awards fills description/type/agency chain;
    # a subaward has no own detail endpoint, so fetch its PRIME's detail.
    prime_featured = [r for r in featured if r.get("role") != "subcontract"]
    sub_featured = [r for r in featured if r.get("role") == "subcontract"]
    if AWARD_DETAIL_PARALLEL and (prime_featured or sub_featured):
        with ThreadPoolExecutor(max_workers=AWARD_FETCH_WORKERS) as pool:
            for rec, det in zip(
                prime_featured, pool.map(lambda r: _award_detail(r["gid"]), prime_featured)
            ):
                rec["detail"] = det
            for rec, det in zip(
                sub_featured, pool.map(lambda r: _award_detail(r.get("prime_gid") or ""), sub_featured)
            ):
                rec["detail"] = det
    else:
        for rec in prime_featured:
            rec["detail"] = _award_detail(rec["gid"])
        for rec in sub_featured:
            rec["detail"] = _award_detail(rec.get("prime_gid") or "")

    for r in featured:
        kind = "contract" if r["group"] == "contracts" else "grant"
        is_sub = r["role"] == "subcontract"
        det = r.get("detail") or {}
        if is_sub:
            aid = f"federal_award:sub:{r['sub_id']}"
            desc = (
                r.get("description")
                or det.get("description")
                or f"{r['prime_recipient'] or r['agency']} subcontract - {r['sub_id']}"
            )
            summary = _sub_summary(r, det)
            chunk_text = _sub_chunk(r, kind, det)
            chunk_id = f"federal_award_chunk:sub:{r['sub_id']}"
        else:
            aid = f"federal_award:{r['gid'] or r['piid']}"
            desc = det.get("description") or f"{r['agency']} {kind} - {r['piid']}"
            summary = _award_summary(r, det, kind)
            chunk_text = _award_chunk(r, det, kind)
            chunk_id = f"federal_award_chunk:{r['gid'] or r['piid']}"
        if aid in plan["entities"]:
            continue
        props = {
            "ticker": ticker,
            "company_name": company,
            "source": "USASpending",
            "award_recipe_version": AWARD_RECIPE_VERSION,
            "kind": kind,
            "role": r["role"],
            "amount": r["amount"],
            "currency": "USD",
            "agency_toptier": det.get("toptier") or r["agency"],
            "agency_subtier": det.get("subtier") or "",
            "agency_office": det.get("office") or "",
            "agency_abbrev": _agency_short(det.get("toptier") or r["agency"]),
            "recipient_uei": r["recipient_uei"],
            "recipient_name": r["recipient"],
            "summary": summary,
        }
        if is_sub:
            props.update({
                "sub_award_id": r["sub_id"],
                "sub_award_type": r.get("sub_type") or "",
                "sub_award_date": r.get("sub_date") or "",
                "prime_piid": r.get("prime_piid") or "",
                "prime_recipient": r.get("prime_recipient") or "",
            })
        else:
            props.update({
                "piid": r["piid"],
                "award_type_code": det.get("type_code") or "",
                "period_start": r["start"],
                "period_end": r["end"],
            })
        plan["entities"][aid] = {"id": aid, "name": _award_label(desc), "label": "FEDERAL_AWARD", "props": props}
        plan["relations"].append({"source_id": r["owner_entity"], "target_id": aid, "label": "HAS_AWARD", "props": {}})
        plan["chunks"].append({
            "id": chunk_id,
            "text": chunk_text,
            "props": {"ticker": ticker, "source": "USASpending", "award_recipe_version": AWARD_RECIPE_VERSION},
        })
        plan["relations"].append({"source_id": chunk_id, "target_id": aid, "label": "MENTIONS", "props": {}})
        plan["stats"]["featured"] = plan["stats"].get("featured", 0) + 1

        # PRODUCT per featured award - junk-gated description
        prod_desc = r.get("description") or det.get("description") or ""
        if not _is_junk_product_desc(prod_desc):
            pname = _product_name(prod_desc)
            pcn = canonical_name(pname)
            if pcn:
                pid = f"product:{pcn}"
                if pid not in plan["entities"]:
                    plan["entities"][pid] = {
                        "id": pid,
                        "name": pname,
                        "label": "PRODUCT",
                        "props": {
                            "ticker": ticker,
                            "source": "USASpending",
                            "award_recipe_version": AWARD_RECIPE_VERSION,
                            "summary": f"{pname} - scope of a federal {kind} {r['role']} award ({r['agency']})",
                        },
                    }
                plan["relations"].append({"source_id": aid, "target_id": pid, "label": "COVERS", "props": {}})

        # COMPANY (prime) per featured SUBCONTRACT award - cross-cluster hook
        if is_sub and (r.get("prime_recipient") or "").strip():
            peid, _plabel = _fold_prime(ticker, r["prime_recipient"], known_parents, plan)
            if peid:
                plan["relations"].append({"source_id": aid, "target_id": peid, "label": "AWARDED_BY", "props": {}})

    # AWARD_INVENTORY - one portfolio node per ticker (everything not featured,
    # including fold-nowhere recipients)
    if nonfeatured:
        inv_id = f"award_inventory:{ticker}"
        plan["entities"][inv_id] = {
            "id": inv_id,
            "name": f"{company} - Federal Award Inventory",
            "label": "AWARD_INVENTORY",
            "props": {
                "ticker": ticker,
                "company_name": company,
                "source": "USASpending",
                "award_recipe_version": AWARD_RECIPE_VERSION,
                "award_count": len(nonfeatured),
                "sub_count": sum(1 for r in nonfeatured if r.get("role") == "subcontract"),
                "summary": (
                    f"{len(nonfeatured)} federal awards in the inventory for {company} - "
                    "every in-window award not featured as a node, ranked by amount."
                ),
            },
        }
        if tid not in snapshot["entities"] and tid not in plan["entities"]:
            plan["entities"][tid] = {
                "id": tid,
                "name": company,
                "label": "TICKER",
                "props": {
                    "ticker": ticker,
                    "company_name": company,
                    "award_recipe_version": AWARD_RECIPE_VERSION,
                },
            }
        plan["relations"].append({"source_id": tid, "target_id": inv_id, "label": "HAS_AWARD_INVENTORY", "props": {}})
        chunk_id = f"award_inventory_chunk:{ticker}"
        plan["chunks"].append({
            "id": chunk_id,
            "text": "\n".join(_inventory_lines(nonfeatured, AWARD_INVENTORY_MAX_LINES)),
            "props": {"ticker": ticker, "source": "USASpending", "award_recipe_version": AWARD_RECIPE_VERSION},
        })
        plan["relations"].append({"source_id": chunk_id, "target_id": inv_id, "label": "MENTIONS", "props": {}})
        plan["stats"]["inventory"] = len(nonfeatured)

    _emit(
        "info",
        f">{ticker}: USASpending - {len(records)} prime >= {_fmt_usd(AWARD_MIN_AMOUNT)} "
        f"(from {len(raw)}) - {len(sub_records)} sub >= {_fmt_usd(SUBAWARD_MIN_AMOUNT)} "
        f"(from {len(sub_raw)}) - {plan['stats'].get('featured_total', plan['stats'].get('featured', 0))} featured",
        ticker, job_id,
    )
    return plan

# ─────────────────────────────────────────────────────────────
# 7. run_awards - the leg entry
# ─────────────────────────────────────────────────────────────
def run_awards(tickers=None, dry_run=False, job_id=""):
    """Snapshot -> per-ticker fetch+fold (parallel) -> ONE bulk write ->
    stale-node cleanup (idempotent by recipe stamp). Returns a summary dict."""
    tickers = [t.upper() for t in (tickers or list(AWARD_NAME_KEYS))]
    if not _award_lock.acquire(blocking=False):
        _emit("warn", "USASpending ingest already running - this request skipped (one at a time)", job_id=job_id)
        return {"status": "busy"}
    try:
        from . import db
        driver = db.get_driver()
        with driver.session() as session:
            snap = snapshot(session)
        known_parents = _known_parents(snap)

        done, todo = [], []
        for t in tickers:
            if not dry_run:
                with driver.session() as session:
                    if has_stamp(session, t, AWARD_RECIPE_VERSION):
                        done.append(t)
                        continue
            todo.append(t)
        if done:
            _emit("warn", f"USASpending leg: {', '.join(done)} already ingested at {AWARD_RECIPE_VERSION} - skipped", job_id=job_id)

        plans = {}
        if todo:
            with ThreadPoolExecutor(max_workers=min(len(todo), 3)) as pool:
                futures = {pool.submit(_ticker_plan, t, snap, known_parents, job_id): t for t in todo}
                for fut, t in futures.items():
                    try:
                        p = fut.result()
                        if p:
                            plans[t] = p
                    except Exception as e:  # noqa: BLE001 - one ticker must not kill the run
                        _emit("error", f"{t}: USASpending leg failed - {e}", t, job_id)
                        return {"status": "error", "error": str(e), "ticker": t}

        if not plans:
            return {"status": "ok", "skipped": done, "planned": 0, "dry_run": dry_run, "recipe": AWARD_RECIPE_VERSION}

        total_records = sum(p["stats"].get("records", 0) for p in plans.values())
        featured_total = sum(p["stats"].get("featured_total", p["stats"].get("featured", 0)) for p in plans.values())
        inv_total = sum(1 for p in plans.values() if p["stats"].get("inventory", 0) > 0)

        if not dry_run:
            with driver.session() as session:
                counts = write_plan(session, _merge_plans(plans), AWARD_RECIPE_VERSION)
                stale_cleanup(session, list(plans), AWARD_RECIPE_VERSION)
        else:
            counts = {"dry_run": True}

        _emit(
            "ok",
            f"*USASpending Ingest Complete* write success; {total_records} records; "
            f"{featured_total} AWARD featured; {inv_total} inventory",
            job_id=job_id,
        )
        return {
            "status": "ok",
            "recipe": AWARD_RECIPE_VERSION,
            "skipped": done,
            "planned": list(plans),
            "dry_run": dry_run,
            "counts": counts,
            "stats": {"records": total_records, "featured": featured_total, "inventory_tickers": inv_total},
        }
    finally:
        _award_lock.release()


def _merge_plans(plans):
    """Collapse many per-ticker plans into one (chunks, entities, relations)
    so the whole ingest is ONE transaction."""
    merged = {"entities": {}, "relations": [], "chunks": [], "stats": {}}
    for p in plans.values():
        for k, v in (p.get("entities") or {}).items():
            merged["entities"].setdefault(k, v)
        merged["relations"] += p.get("relations") or []
        merged["chunks"] += p.get("chunks") or []
    return merged

# ─────────────────────────────────────────────────────────────
# 8. CLI
# ─────────────────────────────────────────────────────────────
def _export_sample(tickers, out_path):
    """Build the fetch+fold plans WITHOUT a database and dump the node/edge set
    to a JSON file - used to generate sample_data/ (real output, no DB needed)."""
    import datetime as _dt

    snap = {"entities": {}}
    known = _known_parents(snap)
    plans = {}
    for t in tickers:
        p = _ticker_plan(t, snap, known, job_id="export")
        if p:
            plans[t] = p
    payload = {
        "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
        "recipe": AWARD_RECIPE_VERSION,
        "tickers": {
            t: {
                "entities": list(plans[t]["entities"].values()),
                "relations": plans[t]["relations"],
                "chunks": plans[t]["chunks"],
                "stats": plans[t]["stats"],
            }
            for t in plans
        },
    }
    Path(out_path).write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(f"Wrote {out_path} - tickers: {list(plans)}")
    return payload


def _main(argv=None):
    argv = list(argv if argv is not None else sys.argv[1:])
    tickers = []
    dry_run = False
    export = None
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--dry-run":
            dry_run = True
        elif a == "--tickers":
            j = i + 1
            while j < len(argv) and not argv[j].startswith("--"):
                tickers.append(argv[j].upper())
                j += 1
            i = j - 1
        elif a == "--export":
            export = argv[i + 1] if i + 1 < len(argv) else None
            tickers = [t.upper() for t in tickers or list(AWARD_NAME_KEYS)]
            return _export_sample(tickers, export)
        i += 1
    if not tickers:
        tickers = list(AWARD_NAME_KEYS)
    result = run_awards(tickers, dry_run=dry_run, job_id="cli")
    print(json.dumps(result, indent=2, default=str))
    return result


if __name__ == "__main__":
    _main()
