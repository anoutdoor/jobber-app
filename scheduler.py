import logging
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)
_scheduler = None

CT = pytz.timezone("America/Chicago")


def start_scheduler(sync_fn, reconcile_fn, digest_fn=None, mow_export_fn=None,
                    pnl_reminder_fn=None, marketing_refresh_fn=None,
                    projections_refresh_fn=None, homeowners_fn=None,
                    bid_scan_fn=None, quote_risk_fn=None,
                    quote_history_fn=None, model_refit_fn=None):
    global _scheduler
    if _scheduler and _scheduler.running:
        return

    _scheduler = BackgroundScheduler(daemon=True, timezone=CT)

    _scheduler.add_job(
        sync_fn,
        trigger=IntervalTrigger(hours=1),
        id="jobber_sync",
        name="Jobber → Sheets hourly sync",
        replace_existing=True,
    )

    _scheduler.add_job(
        reconcile_fn,
        trigger=CronTrigger(hour=20, minute=0, timezone=CT),
        id="daily_reconcile",
        name="Daily overhead reconciliation at 8pm CT",
        replace_existing=True,
    )

    if digest_fn:
        _scheduler.add_job(
            digest_fn,
            trigger=CronTrigger(hour=6, minute=0, timezone=CT),
            id="daily_cashflow_digest",
            name="Daily cashflow digest email at 6am CT",
            replace_existing=True,
        )
        logger.info("Scheduler started — hourly sync, 8pm reconcile, 6am cashflow digest (CT).")
    else:
        logger.info("Scheduler started — hourly sync + 8pm reconciliation (CT).")

    if mow_export_fn:
        _scheduler.add_job(
            mow_export_fn,
            trigger=CronTrigger(hour=19, minute=0, timezone=CT),
            id="daily_mow_export",
            name="Daily mow-time export + dashboard refresh (7pm CT)",
            replace_existing=True,
        )
        logger.info("Scheduled daily mow-time export + dashboard refresh (7pm CT).")

    if pnl_reminder_fn:
        _scheduler.add_job(
            pnl_reminder_fn,
            trigger=CronTrigger(day=15, hour=8, minute=0, timezone=CT),
            id="monthly_pnl_reminder",
            name="Monthly P&L update reminder (15th, 8am CT)",
            replace_existing=True,
        )
        logger.info("Scheduled monthly P&L update reminder (15th, 8am CT).")

    if marketing_refresh_fn:
        _scheduler.add_job(
            marketing_refresh_fn,
            trigger=CronTrigger(hour=5, minute=30, timezone=CT),
            id="marketing_refresh",
            name="Marketing dashboard client cache refresh (5:30am CT)",
            replace_existing=True,
        )
        logger.info("Scheduled marketing dashboard refresh (5:30am CT).")

    if projections_refresh_fn:
        _scheduler.add_job(
            projections_refresh_fn,
            trigger=CronTrigger(hour=5, minute=45, timezone=CT),
            id="projections_refresh",
            name="Revenue projections cache refresh (5:45am CT)",
            replace_existing=True,
        )
        logger.info("Scheduled revenue projections refresh (5:45am CT).")

    if homeowners_fn:
        _scheduler.add_job(
            homeowners_fn,
            trigger=CronTrigger(day_of_week="mon", hour=7, minute=30, timezone=CT),
            id="weekly_new_homeowners",
            name="Weekly new-homeowner lead digest (Mon 7:30am CT)",
            replace_existing=True,
        )
        logger.info("Scheduled weekly new-homeowner digest (Mon 7:30am CT).")

    if bid_scan_fn:
        _scheduler.add_job(
            bid_scan_fn,
            trigger=CronTrigger(hour=6, minute=15, timezone=CT),
            id="daily_bid_scan",
            name="Daily municipal bid-opportunity scan (6:15am CT)",
            replace_existing=True,
        )
        logger.info("Scheduled daily bid-opportunity scan (6:15am CT).")

    if quote_risk_fn:
        _scheduler.add_job(
            quote_risk_fn,
            trigger=IntervalTrigger(hours=1),
            id="hourly_quote_risk",
            name="Hourly at-risk quote scoring",
            replace_existing=True,
        )
        logger.info("Scheduled hourly at-risk quote scoring.")

    if quote_history_fn:
        _scheduler.add_job(
            quote_history_fn,
            trigger=CronTrigger(hour=20, minute=45, timezone=CT),
            id="nightly_quote_history",
            name="Nightly decided-quote history append (8:45pm CT)",
            replace_existing=True,
        )
        logger.info("Scheduled nightly quote-history append (8:45pm CT).")

    if model_refit_fn:
        _scheduler.add_job(
            model_refit_fn,
            trigger=CronTrigger(day_of_week="sun", hour=21, minute=0, timezone=CT),
            id="weekly_model_refit",
            name="Weekly risk-model refit (Sun 9pm CT)",
            replace_existing=True,
        )
        logger.info("Scheduled weekly risk-model refit (Sun 9pm CT).")

    _scheduler.start()


def stop_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped.")
