import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)
_scheduler = None


def start_scheduler(sync_fn, reconcile_fn):
    global _scheduler
    if _scheduler and _scheduler.running:
        return

    _scheduler = BackgroundScheduler(daemon=True)

    _scheduler.add_job(
        sync_fn,
        trigger=IntervalTrigger(hours=1),
        id="jobber_sync",
        name="Jobber → Sheets hourly sync",
        replace_existing=True,
    )

    _scheduler.add_job(
        reconcile_fn,
        trigger=CronTrigger(hour=20, minute=0),
        id="daily_reconcile",
        name="Daily overhead reconciliation at 8pm",
        replace_existing=True,
    )

    _scheduler.start()
    logger.info("Scheduler started — hourly sync + daily reconciliation at 8pm.")


def stop_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped.")
