"""Loads financial_config.yaml and exposes typed accessors."""
import os
from pathlib import Path

import yaml

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "financial_config.yaml"
_cache = None


def load_config(force=False):
    global _cache
    if _cache is not None and not force:
        return _cache
    with open(_CONFIG_PATH) as f:
        _cache = yaml.safe_load(f)
    return _cache


def email_settings():
    return load_config()["email"]


def vendors():
    return load_config()["vendors"]


def overhead_bills():
    """Return the raw list of recurring overhead bills from config."""
    return load_config().get("overhead", {}).get("bills", []) or []


def payroll_settings():
    return load_config()["payroll"]


def aging_buckets():
    return load_config()["ar"]["aging_buckets"]


def anomaly_settings():
    return load_config()["anomalies"]


def qbo_settings():
    return load_config().get("qbo", {})


def qbo_accounts():
    """Configured bank + CC accounts. Balances may be overridden by the
    'Balance Overrides' Google Sheet tab (id-keyed)."""
    return load_config().get("qbo", {}).get("accounts", []) or []


# --- New sections added 2026-06-09 -----------------------------------------

def cc_rewards():
    """Credit card rewards available — economic offset to CC debt.
    Returns dict {available, as_of, note} or {} if not configured."""
    return load_config().get("cc_rewards") or {}


def customer_deposits():
    """List of customer deposits held (liability). Each entry: {client, amount, as_of}."""
    return load_config().get("customer_deposits") or []


def uncleared_checks_amount():
    """Sum of checks written but not yet cleared (still in bank balance,
    but will leave soon)."""
    cfg = load_config().get("uncleared_checks") or {}
    return float(cfg.get("amount") or 0)


def owner_pay_amount():
    """Owner draw / pay owed (separate from crew payroll)."""
    cfg = load_config().get("owner_pay") or {}
    return float(cfg.get("amount") or 0)


def variable_overhead():
    """Static reference info for monthly variable overhead + breakeven target."""
    return load_config().get("variable_overhead") or {}


def bucket_settings():
    """Dates used for the email's two-bucket structure (this-week / by-July-1).
    Returns dict {due_by_july_1: 'YYYY-MM-DD'}."""
    return load_config().get("buckets") or {}
