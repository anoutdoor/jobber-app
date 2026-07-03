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
                    projections_refresh_fn=None, homeowners_fn=None):
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

    _scheduler.start()


def stop_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped.")
