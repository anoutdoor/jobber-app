"""Quote decision history + self-retraining risk model.

Keeps a growing dataset of decided quotes (converted or archived) in a hidden
Google Sheet tab ("_quotehistory", gzip+base64 chunked JSON, same pattern as
financial/cash_state.py) and refits the quote-risk logistic model from it on a
weekly cadence.

Three moving parts:
  * seed_from_csv()     one-time load of the 1,818-quote quote-analytics
                        training set so the history starts with real depth.
  * run_nightly()       appends quotes that transitioned to converted/archived
                        since the last high-water mark (transitionedAt tracked
                        client-side; the server-side date filters are broken,
                        see quote_risk.QUOTES_QUERY note).
  * run_weekly_refit()  refits win ~ intercept + log1p(total) + repeat_client
                        + discounted via pure-Python IRLS (no numpy/sklearn in
                        the deployed requirements), recomputes the
                        pct-of-home-value bands, and publishes to a hidden
                        "_riskmodel" tab only if the new fit is sane and not
                        worse than the old one on recent quotes (AUC guard).

load_current_model() is the read side: published Sheet model if present, else
the static risk_model.json that quote_risk.py ships with.

READ-ONLY against Jobber: this module never executes mutations.
"""
import base64
import csv
import gzip
import json
import logging
import math
import os
import time
from datetime import datetime, timedelta, timezone

from gspread.exceptions import WorksheetNotFound

from jobber_sync import graphql_request, get_sheets_client
from financial.token_persistence import read_tokens
from quote_risk import lookup_market_value, MODEL_PATH, REPEAT_CLIENT_MIN_DAYS

logger = logging.getLogger(__name__)

HISTORY_TAB = "_quotehistory"
MODEL_TAB = "_riskmodel"
_CHUNK = 40000  # chars per cell, under the 50k Sheets cell limit

SEED_CSV_PATH = "/Users/anoutdoorservices/Desktop/AN/quote-analytics/data/quotes_enriched.csv"

FIRST_RUN_LOOKBACK_DAYS = 14
PAGE_SIZE = 25  # nested client/property makes pages costly; keep under throttle
MAX_PAGES = 240  # runaway guard per status query (~6,000 quotes)
THROTTLE_RETRIES = 4
THROTTLE_WAIT_S = 5

# pct-of-home-value band edges (upper bounds); labels match risk_model.json
PCT_BAND_EDGES = [0.001, 0.002, 0.005, 0.01, 1]

# Drift guards for publishing a refit
AUC_TOLERANCE = 0.02       # new model may not lose more than this vs old
COEF_CHANGE_LIMIT = 3.0    # |new - old| must stay under 3x the old magnitude
RECENT_WINDOW = 400        # AUC compared over the last N decided quotes

# Status is inlined (not a GraphQL variable) to sidestep guessing the enum
# type name; quote_risk.py uses the same inline-literal style.
DECIDED_QUOTES_QUERY_TMPL = """
query DecidedQuotes($cursor: String) {
  quotes(first: %(page)d, after: $cursor, filter: { status: %(status)s }) {
    nodes {
      id
      quoteNumber
      quoteStatus
      createdAt
      transitionedAt
      client { id name createdAt }
      property { address { street postalCode } }
      amounts { total discountAmount }
    }
    pageInfo { hasNextPage endCursor }
    totalCount
  }
}
"""


# ---------------------------------------------------------------------------
# History blob in a hidden Sheet tab (gzip+base64 chunked, cash_state pattern)
# ---------------------------------------------------------------------------

def _history_ws(create=True):
    sid = os.getenv("GOOGLE_SHEET_ID")
    if not sid:
        raise RuntimeError("GOOGLE_SHEET_ID not set; can't persist quote history.")
    sh = get_sheets_client().open_by_key(sid)
    try:
        return sh.worksheet(HISTORY_TAB)
    except WorksheetNotFound:
        if not create:
            return None
        ws = sh.add_worksheet(title=HISTORY_TAB, rows=100, cols=2)
        try:
            ws.hide()
        except Exception:
            pass
        return ws


def _encode(blob):
    raw = json.dumps(blob, separators=(",", ":")).encode("utf-8")
    return base64.b64encode(gzip.compress(raw)).decode("ascii")


def _decode(text):
    return json.loads(gzip.decompress(base64.b64decode(text)).decode("utf-8"))


def _load_history():
    """Return the stored history blob dict, or {} if nothing saved yet."""
    try:
        ws = _history_ws(create=False)
        if ws is None:
            return {}
        col = ws.col_values(1)
    except WorksheetNotFound:
        return {}
    text = "".join(c for c in col if c)
    if not text:
        return {}
    try:
        return _decode(text)
    except Exception:
        logger.exception("QuoteHistory: blob decode failed; treating as empty.")
        return {}


def _save_history(blob):
    text = _encode(blob)
    chunks = [text[i:i + _CHUNK] for i in range(0, len(text), _CHUNK)] or [""]
    ws = _history_ws()
    existing = len(ws.col_values(1))
    ws.update(f"A1:A{len(chunks)}", [[c] for c in chunks])
    if existing > len(chunks):
        ws.batch_clear([f"A{len(chunks) + 1}:A{existing}"])
    return len(chunks)


# ---------------------------------------------------------------------------
# One-time seed from the quote-analytics training CSV
# ---------------------------------------------------------------------------

def _float_or_none(v):
    try:
        f = float(v)
        return f if not math.isnan(f) else None
    except (TypeError, ValueError):
        return None


def _rows_from_csv(path):
    """Parse the quote-analytics enriched CSV into history row dicts."""
    rows = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            total = _float_or_none(r.get("total"))
            if r.get("quote_id") in (None, "") or total is None:
                continue
            year = int(float(r["created_year"]))
            month = int(float(r["created_month"]))
            rows.append({
                "quote_id": r["quote_id"],
                # Best decided-date proxy in the seed data: creation month.
                "decided_date": f"{year:04d}-{month:02d}-01",
                "won": int(float(r["won"])),
                "total": total,
                "repeat_client": int(float(r.get("repeat_client") or 0)),
                "discounted": int(float(r.get("discounted") or 0)),
                "market_value": _float_or_none(r.get("market_value")),
            })
    return rows


def seed_from_csv(path=None, dry_run=True):
    """One-time: load the historical training CSV into the _quotehistory tab.

    Refuses to overwrite an already-seeded blob (would clobber rows appended
    by run_nightly since seeding). dry_run parses + encodes and reports sizes
    without touching the Sheet.
    """
    path = path or SEED_CSV_PATH
    rows = _rows_from_csv(path)
    dedup = {r["quote_id"]: r for r in rows}
    rows = list(dedup.values())

    blob = {"seeded": True, "last_transitioned_at": None, "rows": rows}
    encoded = _encode(blob)
    n_chunks = (len(encoded) + _CHUNK - 1) // _CHUNK

    result = {
        "status": "ok",
        "rows": len(rows),
        "encoded_chars": len(encoded),
        "chunks": n_chunks,
        "dry_run": dry_run,
    }
    if dry_run:
        return result

    existing = _load_history()
    if existing.get("seeded"):
        return {"status": "error", "rows": len(existing.get("rows") or []),
                "message": "history already seeded; refusing to overwrite"}
    # Keep anything nightly appended pre-seed (unlikely but cheap to honor).
    for r in existing.get("rows") or []:
        dedup[r["quote_id"]] = r
    blob["rows"] = list(dedup.values())
    blob["last_transitioned_at"] = existing.get("last_transitioned_at")
    _save_history(blob)
    logger.info(f"QuoteHistory: seeded {len(blob['rows'])} rows from {path}.")
    result["rows"] = len(blob["rows"])
    return result


# ---------------------------------------------------------------------------
# Nightly append of newly decided quotes
# ---------------------------------------------------------------------------

def _fetch_decided(access, status, since_iso):
    """All quotes in `status` with transitionedAt after since_iso.

    Server-side date filters are broken (see quote_risk), so this pages the
    full status set and cuts client-side. Bounded by MAX_PAGES.
    """
    query = DECIDED_QUOTES_QUERY_TMPL % {"page": PAGE_SIZE, "status": status}
    out, cursor = [], None
    for _ in range(MAX_PAGES):
        block = None
        for attempt in range(THROTTLE_RETRIES + 1):
            d = graphql_request(query, {"cursor": cursor},
                                access_token=access, _retry=False)
            block = ((d or {}).get("data") or {}).get("quotes")
            if block:
                break
            errors = (d or {}).get("errors") or []
            throttled = any((e.get("extensions") or {}).get("code") == "THROTTLED"
                            for e in errors)
            if throttled and attempt < THROTTLE_RETRIES:
                # Query cost bucket refills at ~500/s; a short sit-out clears it.
                wait = THROTTLE_WAIT_S * (attempt + 1)
                logger.info(f"QuoteHistory: throttled on quotes({status}); "
                            f"waiting {wait}s (attempt {attempt + 1}).")
                time.sleep(wait)
                continue
            raise RuntimeError(f"quotes({status}) query failed: {json.dumps(d)[:400]}")
        out.extend(block.get("nodes") or [])
        pi = block.get("pageInfo") or {}
        if not pi.get("hasNextPage"):
            break
        cursor = pi.get("endCursor")
    return [q for q in out if (q.get("transitionedAt") or "") > since_iso]


def _quote_to_row(q):
    """Map a Jobber quote node to a history row (market value looked up here,
    error-isolated: a Cook County API hiccup just leaves market_value None)."""
    amounts = q.get("amounts") or {}
    total = float(amounts.get("total") or 0)
    discounted = 1 if float(amounts.get("discountAmount") or 0) > 0 else 0

    # Repeat-client proxy, same as quote_risk / the training data: the client
    # record predates the quote by 30+ days.
    repeat = 0
    try:
        c_created = datetime.fromisoformat(
            (q.get("client") or {})["createdAt"].replace("Z", "+00:00"))
        q_created = datetime.fromisoformat(q["createdAt"].replace("Z", "+00:00"))
        repeat = 1 if (q_created - c_created) > timedelta(days=REPEAT_CLIENT_MIN_DAYS) else 0
    except (KeyError, TypeError, ValueError):
        pass

    mv = None
    addr = (q.get("property") or {}).get("address") or {}
    try:
        mv = lookup_market_value(addr.get("street"), (addr.get("postalCode") or "")[:5])
    except Exception as e:
        logger.warning(f"QuoteHistory: market value lookup failed for quote "
                       f"{q.get('quoteNumber')} ({e}); storing None.")

    t = q.get("transitionedAt") or ""
    return {
        "quote_id": q["id"],
        "decided_date": t[:10] or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "won": 1 if q.get("quoteStatus") == "converted" else 0,
        "repeat_client": repeat,
        "total": total,
        "discounted": discounted,
        "market_value": mv,
    }


def run_nightly(dry_run=True):
    """Append quotes that became converted/archived since the last run.

    High-water mark is the max transitionedAt processed, stored in the
    _quotehistory blob as "last_transitioned_at". A quote that transitions
    again later (e.g. archived then reopened then converted) replaces its
    older row: dedupe is by quote_id, latest wins.
    """
    access = (read_tokens("jobber") or {}).get("access_token")
    if not access:
        return {"status": "error", "message": "no Jobber access token"}

    blob = _load_history()
    since = blob.get("last_transitioned_at")
    if not since:
        since = (datetime.now(timezone.utc)
                 - timedelta(days=FIRST_RUN_LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        nodes = (_fetch_decided(access, "converted", since)
                 + _fetch_decided(access, "archived", since))
    except RuntimeError as e:
        logger.error(f"QuoteHistory: {e}")
        return {"status": "error", "message": str(e)}

    # A quote can appear once per status sweep; keep the latest transition.
    nodes = list({q["id"]: q for q in sorted(
        nodes, key=lambda q: q.get("transitionedAt") or "")}.values())

    new_rows = []
    for q in nodes:
        try:
            new_rows.append(_quote_to_row(q))
        except Exception as e:
            logger.error(f"QuoteHistory: row build failed for quote "
                         f"{q.get('quoteNumber')} ({e}); skipping.")

    high_water = max((q.get("transitionedAt") or "" for q in nodes), default=since)

    result = {
        "status": "ok",
        "since": since,
        "found": len(new_rows),
        "won": sum(r["won"] for r in new_rows),
        "lost": sum(1 - r["won"] for r in new_rows),
        "new_high_water": high_water,
        "dry_run": dry_run,
        "rows": new_rows,
    }
    if dry_run:
        return result

    by_id = {r["quote_id"]: r for r in blob.get("rows") or []}
    appended = sum(1 for r in new_rows if r["quote_id"] not in by_id)
    by_id.update({r["quote_id"]: r for r in new_rows})
    blob["rows"] = list(by_id.values())
    blob["last_transitioned_at"] = high_water
    blob.setdefault("seeded", False)
    _save_history(blob)
    logger.info(f"QuoteHistory: nightly appended {appended} new / "
                f"{len(new_rows) - appended} updated rows since {since}; "
                f"history now {len(blob['rows'])} quotes.")
    result["appended"] = appended
    result["updated"] = len(new_rows) - appended
    result["history_size"] = len(blob["rows"])
    return result


# ---------------------------------------------------------------------------
# Logistic regression via IRLS (pure Python; numpy is not in requirements)
# ---------------------------------------------------------------------------

def _solve(A, b):
    """Solve A x = b by Gaussian elimination with partial pivoting (tiny k)."""
    k = len(b)
    M = [row[:] + [b[i]] for i, row in enumerate(A)]
    for col in range(k):
        piv = max(range(col, k), key=lambda r: abs(M[r][col]))
        if abs(M[piv][col]) < 1e-12:
            raise ValueError("singular system in IRLS solve")
        M[col], M[piv] = M[piv], M[col]
        for r in range(k):
            if r == col:
                continue
            f = M[r][col] / M[col][col]
            for c in range(col, k + 1):
                M[r][c] -= f * M[col][c]
    return [M[i][k] / M[i][i] for i in range(k)]


def _fit_logistic(X, y, max_iter=50, tol=1e-10):
    """Newton-Raphson / IRLS MLE for logistic regression.

    X: list of feature lists (first column should be the 1s intercept),
    y: list of 0/1. Returns (beta, n_iter, converged).
    """
    n, k = len(X), len(X[0])
    beta = [0.0] * k
    converged = False
    it = 0
    for it in range(1, max_iter + 1):
        grad = [0.0] * k
        H = [[0.0] * k for _ in range(k)]
        for xi, yi in zip(X, y):
            z = sum(b * v for b, v in zip(beta, xi))
            p = 1.0 / (1.0 + math.exp(-max(-35.0, min(35.0, z))))
            w = p * (1.0 - p)
            r = yi - p
            for a in range(k):
                grad[a] += xi[a] * r
                for c in range(a, k):
                    H[a][c] += w * xi[a] * xi[c]
        for a in range(k):
            for c in range(a):
                H[a][c] = H[c][a]
        step = _solve(H, grad)
        beta = [b + s for b, s in zip(beta, step)]
        if max(abs(s) for s in step) < tol:
            converged = True
            break
    return beta, it, converged


def _design(rows):
    """History rows -> (X, y) for won ~ 1 + log1p(total) + repeat + discounted."""
    X, y = [], []
    for r in rows:
        X.append([1.0,
                  math.log1p(float(r["total"])),
                  1.0 if r.get("repeat_client") else 0.0,
                  1.0 if r.get("discounted") else 0.0])
        y.append(int(r["won"]))
    return X, y


def _score(coefs, row):
    """Linear predictor under a coefficient dict (monotone in win prob)."""
    return (coefs["intercept"]
            + coefs["log_total"] * math.log1p(float(row["total"]))
            + coefs["repeat_client"] * (1 if row.get("repeat_client") else 0)
            + coefs["discounted"] * (1 if row.get("discounted") else 0))


def _auc(scores, labels):
    """AUC via the rank-sum (Mann-Whitney) formula with average ranks for ties."""
    n_pos = sum(labels)
    n_neg = len(labels) - n_pos
    if not n_pos or not n_neg:
        return None
    order = sorted(range(len(scores)), key=lambda i: scores[i])
    ranks = [0.0] * len(scores)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and scores[order[j + 1]] == scores[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0  # 1-based average rank across the tie block
        for t in range(i, j + 1):
            ranks[order[t]] = avg
        i = j + 1
    rank_sum_pos = sum(r for r, lab in zip(ranks, labels) if lab)
    return (rank_sum_pos - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)


def _pct_value_bands(rows):
    """Win rates by quote-total-as-%-of-home-value band, same schema as
    risk_model.json (max_pct upper bound labels, n per band)."""
    edges = PCT_BAND_EDGES
    counts = [[0, 0] for _ in edges]  # [n, wins]
    for r in rows:
        mv = r.get("market_value")
        total = float(r.get("total") or 0)
        if not mv or mv <= 0 or total <= 0:
            continue
        pct = total / mv
        idx = len(edges) - 1
        for i, e in enumerate(edges):
            if pct < e:
                idx = i
                break
        counts[idx][0] += 1
        counts[idx][1] += int(r["won"])
    return [{"max_pct": e,
             "win_rate": round(c[1] / c[0], 4) if c[0] else None,
             "n": c[0]}
            for e, c in zip(edges, counts)]


# ---------------------------------------------------------------------------
# Model read/publish
# ---------------------------------------------------------------------------

def _model_ws(create=True):
    sid = os.getenv("GOOGLE_SHEET_ID")
    if not sid:
        raise RuntimeError("GOOGLE_SHEET_ID not set; can't persist risk model.")
    sh = get_sheets_client().open_by_key(sid)
    try:
        return sh.worksheet(MODEL_TAB)
    except WorksheetNotFound:
        if not create:
            return None
        ws = sh.add_worksheet(title=MODEL_TAB, rows=5, cols=2)
        try:
            ws.hide()
        except Exception:
            pass
        return ws


def load_current_model():
    """The live model: published _riskmodel tab if present, else the static
    risk_model.json shipped with quote_risk. Same schema either way."""
    try:
        ws = _model_ws(create=False)
        if ws is not None:
            raw = ws.acell("A1").value
            if raw:
                model = json.loads(raw)
                if model.get("coefficients"):
                    return model
    except Exception as e:
        logger.warning(f"QuoteHistory: _riskmodel read failed ({e}); "
                       f"falling back to risk_model.json.")
    with open(MODEL_PATH) as f:
        return json.load(f)


def _publish_model(model):
    _model_ws().update("A1", [[json.dumps(model)]])


def run_weekly_refit(dry_run=True, _rows=None):
    """Refit the logistic model + pct-value bands from the full history.

    Publishes to the _riskmodel tab only when the refit passes the drift
    guards: AUC over the most recent RECENT_WINDOW decided quotes must not
    drop more than AUC_TOLERANCE vs the current model, and no coefficient may
    move more than COEF_CHANGE_LIMIT x its old magnitude (a degenerate-fit
    tripwire). dry_run fits and reports but never writes.

    _rows is a test hook: pass history rows directly to bypass the Sheet.
    """
    all_rows = _rows if _rows is not None else (_load_history().get("rows") or [])
    # Zero-dollar quotes are excluded from the fit, matching the original
    # statsmodels training run (verified: including them shifts the intercept
    # by ~0.57; excluding reproduces risk_model.json exactly).
    rows = [r for r in all_rows if float(r.get("total") or 0) > 0]
    if len(rows) < 100:
        return {"status": "error",
                "message": f"only {len(rows)} usable history rows; refusing to refit"}

    old_model = load_current_model()
    old_coefs = old_model["coefficients"]

    X, y = _design(rows)
    try:
        beta, n_iter, converged = _fit_logistic(X, y)
    except ValueError as e:
        return {"status": "error", "message": f"IRLS failed: {e}"}
    new_coefs = {"intercept": beta[0], "log_total": beta[1],
                 "repeat_client": beta[2], "discounted": beta[3]}

    # Drift guard: compare both models on the most recently decided quotes.
    recent = sorted(rows, key=lambda r: (r.get("decided_date") or "",
                                         r.get("quote_id") or ""))[-RECENT_WINDOW:]
    labels = [int(r["won"]) for r in recent]
    old_auc = _auc([_score(old_coefs, r) for r in recent], labels)
    new_auc = _auc([_score(new_coefs, r) for r in recent], labels)

    auc_ok = (old_auc is None or new_auc is None
              or new_auc >= old_auc - AUC_TOLERANCE)
    coef_ok = all(abs(new_coefs[k] - old_coefs[k]) < COEF_CHANGE_LIMIT * abs(old_coefs[k])
                  for k in new_coefs)
    publish = converged and auc_ok and coef_ok

    model = {
        "coefficients": new_coefs,
        "pct_value_bands": _pct_value_bands(rows),
        "fitted_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "n": len(rows),
    }

    published = False
    if publish and not dry_run:
        try:
            _publish_model(model)
            published = True
        except Exception as e:
            logger.error(f"QuoteHistory: model publish failed ({e}).")
            return {"status": "error", "message": f"publish failed: {e}"}

    logger.info(f"QuoteHistory: refit n={len(rows)} iters={n_iter} "
                f"old_auc={old_auc} new_auc={new_auc} "
                f"publish={publish} dry_run={dry_run}.")
    return {
        "status": "ok",
        "n": len(rows),
        "iterations": n_iter,
        "converged": converged,
        "old_coefficients": old_coefs,
        "new_coefficients": new_coefs,
        "old_auc": round(old_auc, 4) if old_auc is not None else None,
        "new_auc": round(new_auc, 4) if new_auc is not None else None,
        "auc_ok": auc_ok,
        "coef_ok": coef_ok,
        "would_publish": publish,
        "published": published,
        "pct_value_bands": model["pct_value_bands"],
        "dry_run": dry_run,
    }
