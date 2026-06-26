"""Python port of the cash tracker's position math (cash-position-tracker
src/lib/calculations.js). Kept in lockstep with the JS so the daily email
matches the app to the penny.

Input is the raw cash-tracker state (the 'current' object synced to the
_cashstate tab). Output is the headline numbers + breakdowns the email needs.
"""
import re
from datetime import date

TIMING_NOW = "now"
TIMING_MONTH = "month"

OBLIGATION_GROUPS = [
    ("payroll", "Payroll"),
    ("ownerPay", "Owner pay"),
    ("materials", "Materials / POs"),
    ("vendorAP", "Vendor AP"),
    ("taxes", "Taxes"),
]

_CREDIT_RE = re.compile(r"credit|card|visa|master|amex|discover|quicksilver|venture|savor|platinum", re.I)
_LURVEY_RE = re.compile(r"lurvey", re.I)
_ORD_RE = re.compile(r"^(\d{1,2})(st|nd|rd|th)?$", re.I)
_DATE_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})$")


def _num(v):
    if isinstance(v, bool):
        return 0.0
    if isinstance(v, (int, float)):
        return float(v) if v == v else 0.0  # NaN guard
    try:
        return float(str(v).strip())
    except (ValueError, TypeError, AttributeError):
        return 0.0


def _sum_rows(rows):
    if not isinstance(rows, list):
        return 0.0
    return sum(_num(r.get("amount")) for r in rows if isinstance(r, dict))


def _all_obligations(layer4):
    out = []
    if not isinstance(layer4, dict):
        return out
    for key, label in OBLIGATION_GROUPS:
        rows = layer4.get(key)
        if not isinstance(rows, list):
            continue
        for r in rows:
            if not isinstance(r, dict):
                continue
            o = dict(r)
            o["amount"] = _num(r.get("amount"))
            o["category"] = key
            o["categoryLabel"] = label
            out.append(o)
    return out


def _categories_from_obligations(obligations):
    totals = [{"key": k, "label": l, "total": 0.0} for k, l in OBLIGATION_GROUPS]
    index = {t["key"]: t for t in totals}
    for ob in obligations or []:
        if ob.get("category") in index:
            index[ob["category"]]["total"] += ob.get("amount", 0.0)
    return totals


def _days_until(due_date, today):
    if not due_date:
        return None
    try:
        due = date.fromisoformat(str(due_date)[:10])
    except (ValueError, TypeError):
        return None
    return (due - today).days


def _obligation_in_window(ob, window, today):
    days = _days_until(ob.get("dueDate"), today)
    if days is not None:
        eff = days
    elif ob.get("timing") == TIMING_NOW:
        eff = 0
    elif ob.get("timing") == TIMING_MONTH:
        eff = 20
    else:
        eff = 60
    if window == "now":
        return eff <= 1
    if window == "week":
        return eff <= 7
    if window == "month":
        return eff <= 31
    return True


def _payout_in_window(p, window, today):
    raw = (p.get("arrivalDate") if isinstance(p, dict) else "") or ""
    days = _days_until(raw[:10], today)
    if days is None:
        return False
    if window == "now":
        return days <= 1
    if window == "week":
        return days <= 7
    if window == "month":
        return days <= 31
    return True


def _guess_bucket(a):
    # Mirrors the JS precedence quirk: name wins if set, else ' ' + org.
    nm = a.get("name") or ""
    n = nm.lower() if nm else (" " + (a.get("org") or "")).lower()
    if _CREDIT_RE.search(n):
        return "credit"
    if _num(a.get("balance")) < 0:
        return "credit"
    return "liquid"


def _resolve_bucket(a, mapping):
    if mapping and mapping.get(a.get("id")):
        return mapping[a["id"]]
    if a.get("bucket"):
        return a["bucket"]
    return _guess_bucket(a)


def _compute_payroll_owed_weekly(weekly_amount, today):
    weekly = _num(weekly_amount)
    day_index = today.weekday() + 1  # Mon=1 .. Sun=7
    current = (weekly / 7) * day_index
    prior = weekly if day_index <= 2 else 0
    return prior + current


def _compute_ar(ar):
    a = ar or {}
    auto = a.get("auto") or {}
    if auto.get("enabled"):
        j = auto.get("jobber") or {}
        return {
            "source": "jobber",
            "total": _num(j.get("total")),
            "namedCount": 0,
            "breakdown": j.get("breakdown") if isinstance(j.get("breakdown"), list) else [],
        }
    general = _num(a.get("general"))
    named = a.get("namedJobs") if isinstance(a.get("namedJobs"), list) else []
    named_total = sum(_num(n.get("amount")) for n in named if isinstance(n, dict))
    return {
        "source": "manual",
        "total": general + named_total,
        "general": general,
        "namedTotal": named_total,
        "namedCount": len(named),
    }


def compute_cash_position(state, today=None):
    s = state or {}
    today = today or date.today()

    bs = s.get("bankSync") or {}
    synced = bs.get("accounts") if (bs.get("connected") and isinstance(bs.get("accounts"), list)) else []
    synced = synced or []
    mapping = bs.get("mapping") or {}
    synced_liquid = 0.0
    synced_credit = 0.0
    for a in synced:
        if not isinstance(a, dict):
            continue
        bucket = _resolve_bucket(a, mapping)
        if bucket == "liquid":
            synced_liquid += _num(a.get("balance"))
        elif bucket == "credit":
            synced_credit += abs(_num(a.get("balance")))

    total_liquid = _sum_rows(s.get("layer1_liquid")) + synced_liquid
    uncleared = _sum_rows(s.get("layer2_uncleared"))
    deposits = _sum_rows(s.get("layer3_deposits"))

    auto_payroll = None
    pa = s.get("payrollAuto") or {}
    if pa.get("enabled"):
        if pa.get("source") == "jobber":
            j = pa.get("jobber") or {}
            auto_payroll = {
                "source": "jobber",
                "total": _num(j.get("total")),
                "grossPay": _num(j.get("grossPay")),
                "taxBurden": _num(j.get("taxBurden")),
                "weekStart": j.get("weekStart") or "",
                "breakdown": j.get("breakdown") if isinstance(j.get("breakdown"), list) else [],
            }
        else:
            auto_payroll = {
                "source": "weekly",
                "total": _compute_payroll_owed_weekly(pa.get("weeklyAmount"), today),
            }

    obligations = _all_obligations(s.get("layer4"))
    if auto_payroll:
        obligations = [o for o in obligations if o.get("category") != "payroll"]
        obligations.append({
            "id": "auto_payroll", "label": "Payroll owed (auto)",
            "amount": auto_payroll["total"], "category": "payroll",
            "categoryLabel": "Payroll", "timing": TIMING_NOW,
        })

    la = s.get("lurveyAuto") or {}
    if la.get("enabled"):
        obligations = [
            o for o in obligations
            if not (o.get("category") == "vendorAP"
                    and _LURVEY_RE.search(o.get("label") or o.get("description") or ""))
        ]
        obligations.append({
            "id": "auto_lurvey", "label": "Lurvey (auto)",
            "amount": _num(la.get("balance")), "category": "vendorAP",
            "categoryLabel": "Vendor AP", "timing": la.get("timing") or TIMING_MONTH,
            "dueDate": la.get("dueDate") or "",
        })

    total_obligations = sum(o.get("amount", 0.0) for o in obligations)

    ar = _compute_ar(s.get("ar"))
    total_cc = _sum_rows(s.get("creditCards")) + synced_credit
    excluded_debt = _sum_rows(s.get("excludedDebt"))

    ip = s.get("incomingPayouts") or {}
    payout_list = ip.get("payouts") if (ip.get("enabled") and isinstance(ip.get("payouts"), list)) else []
    payout_list = payout_list or []
    incoming_payouts = _num(ip.get("total")) if ip.get("enabled") else 0.0

    free_cash = total_liquid - uncleared - deposits - total_obligations
    net_position = (total_liquid + ar["total"] + incoming_payouts
                    - uncleared - deposits - total_obligations - total_cc)
    total_owed = uncleared + deposits + total_obligations + total_cc

    def horizon(window):
        ob_out = sum(o.get("amount", 0.0) for o in obligations if _obligation_in_window(o, window, today))
        pay_in = sum(_num(p.get("netAmount")) for p in payout_list if _payout_in_window(p, window, today))
        return total_liquid - uncleared - deposits - ob_out + pay_in

    return {
        "totalLiquid": total_liquid,
        "unclearedChecks": uncleared,
        "depositsHeld": deposits,
        "totalObligations": total_obligations,
        "obligationCategories": _categories_from_obligations(obligations),
        "autoPayroll": auto_payroll,
        "ar": ar,
        "totalCC": total_cc,
        "excludedDebt": excluded_debt,
        "incomingPayouts": incoming_payouts,
        "payoutCount": len(payout_list),
        "lurvey": ({
            "balance": _num(la.get("balance")),
            "creditLimit": _num(la.get("creditLimit")),
            "creditAvailable": _num(la.get("creditAvailable")),
            "accountId": la.get("accountId") or "",
        } if la.get("enabled") else None),
        "freeCash": free_cash,
        "netPosition": net_position,
        "totalOwed": total_owed,
        "freeCashNow": horizon("now"),
        "freeCashWeek": horizon("week"),
        "freeCashMonth": horizon("month"),
    }


def _parse_billing_day(raw, cur_month, cur_year):
    if not raw or not isinstance(raw, str):
        return None
    sv = raw.strip()
    m = _ORD_RE.match(sv)
    if m:
        day = int(m.group(1))
        return (day, True) if 1 <= day <= 31 else None
    m = _DATE_RE.match(sv)
    if m:
        month = int(m.group(1)) - 1  # JS getMonth() is 0-based
        day = int(m.group(2))
        year = int(m.group(3))
        if year < 100:
            year += 2000
        return (day, month == cur_month and year == cur_year)
    return None


def fixed_overhead_due(overhead, today=None):
    o = overhead or {}
    expenses = o.get("expenses") if isinstance(o.get("expenses"), list) else []
    today = today or date.today()
    cur_day = today.day
    cur_month = today.month - 1  # 0-based to match parse
    cur_year = today.year

    total_due = 0.0
    items = []
    for e in expenses:
        if not isinstance(e, dict) or e.get("type") != "fixed":
            continue
        amt = _num(e.get("amount"))
        if amt == 0:
            continue
        parsed = _parse_billing_day(e.get("billingDate"), cur_month, cur_year)
        if parsed is None:
            continue
        day, in_this_month = parsed
        if not in_this_month or day < cur_day:
            continue
        total_due += amt
        items.append({"description": e.get("description"), "amount": amt, "day": day})

    items.sort(key=lambda x: x["day"])
    return {"totalDue": total_due, "items": items}
