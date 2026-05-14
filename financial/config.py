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


def overhead_settings():
    cfg = load_config()["overhead"]
    sheet_id = os.getenv(cfg["sheet_id_env"])
    return {"sheet_id": sheet_id, "tab_name": cfg["tab_name"]}


def payroll_settings():
    return load_config()["payroll"]


def aging_buckets():
    return load_config()["ar"]["aging_buckets"]


def anomaly_settings():
    return load_config()["anomalies"]


def qbo_settings():
    return load_config().get("qbo", {"mode": "api"})
