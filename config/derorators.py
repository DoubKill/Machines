import traceback

from config.log_config import error_logger


def api_recorder(func):
    def inner(*args, **kwargs):
        try:
            resp = func(*args, **kwargs)
        except Exception:
            error_logger.error(traceback.format_exc())

    return inner
