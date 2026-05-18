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
