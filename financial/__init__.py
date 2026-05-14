"""A&N daily cashflow digest.

Pulls bank/CC balances from QuickBooks Online, AR + expenses + time entries
from Jobber, recurring overhead from a Google Sheet, computes the current
financial position plus a 30-day forecast, and emails a digest via Gmail
once a day at 6am CT.

Entry point: financial.digest.run_digest()
"""
