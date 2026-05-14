import logging
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)
_scheduler = None

CT = pytz.timezone("America/Chicago")


def start_scheduler(sync_fn, reconcile_fn, digest_fn=None):
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

    _scheduler.start()


def stop_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped.")
