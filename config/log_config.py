import os
import logging.config

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEBUG = True
LOGGING_DIR = os.path.join(BASE_DIR, 'logs')
if not os.path.exists(LOGGING_DIR):
    os.makedirs(LOGGING_DIR)


LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'
        },
        'simple': {
            'format': '%(levelname)s %(message)s'
        },
        'standard': {
            'format': '%(asctime)s [%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] '
                      '[%(module)s:%(funcName)s] [%(levelname)s]- %(message)s'
        }
    },
    'filters': {
    },
    'handlers': {
        'apiFile': {
            'level': 'DEBUG',
            'class': 'config.custom_log.CommonTimedRotatingFileHandler',
            'filename': os.path.join(LOGGING_DIR, 'api_log.log'),
            'when': 'midnight',
            'backupCount': 10,
            'formatter': 'standard',
            'interval': 1,
        },
        'errorFile': {
            'level': 'DEBUG',
            'class': 'config.custom_log.CommonTimedRotatingFileHandler',
            'filename': os.path.join(LOGGING_DIR, 'error.log'),
            'when': 'midnight',
            'backupCount': 10,
            'formatter': 'standard',
            'interval': 1,
        },
        'backupFile': {
            'level': 'DEBUG',
            'class': 'config.custom_log.CommonTimedRotatingFileHandler',
            'filename': os.path.join(LOGGING_DIR, 'backup.log'),
            'when': 'midnight',
            'backupCount': 10,
            'formatter': 'standard',
            'interval': 1,
        },
        'inventoryFile': {
            'level': 'DEBUG',
            'class': 'config.custom_log.CommonTimedRotatingFileHandler',
            'filename': os.path.join(LOGGING_DIR, 'inventory.log'),
            'when': 'midnight',
            'backupCount': 10,
            'formatter': 'standard',
            'interval': 1,
        },
        'heartFile': {
            'level': 'DEBUG',
            'class': 'config.custom_log.CommonTimedRotatingFileHandler',
            'filename': os.path.join(LOGGING_DIR, 'heart.log'),
            'when': 'midnight',
            'backupCount': 10,
            'formatter': 'standard',
            'interval': 1,
        },
        'prepareFile': {
            'level': 'DEBUG',
            'class': 'config.custom_log.CommonTimedRotatingFileHandler',
            'filename': os.path.join(LOGGING_DIR, 'prepare_stock.log'),
            'when': 'midnight',
            'backupCount': 10,
            'formatter': 'standard',
            'interval': 1,
        },
    },
    'loggers': {
        'api_log': {
            'handlers': ['apiFile'],
            'level': 'DEBUG' if DEBUG else 'INFO',
        },
        'error_log': {
            'handlers': ['errorFile'],
            'level': 'DEBUG' if DEBUG else 'INFO',
        },
        'backup_log': {
            'handlers': ['backupFile'],
            'level': 'DEBUG' if DEBUG else 'INFO',
        },
        'inventory_log': {
            'handlers': ['inventoryFile'],
            'level': 'DEBUG' if DEBUG else 'INFO',
        },
        'heart_log': {
            'handlers': ['heartFile'],
            'level': 'DEBUG' if DEBUG else 'INFO',
        },
        'prepare_log': {
            'handlers': ['prepareFile'],
            'level': 'DEBUG' if DEBUG else 'INFO',
        }
    }
}


logging.config.dictConfig(LOGGING)
api_logger = logging.getLogger('api_log')
error_logger = logging.getLogger('error_log')
backup_logger = logging.getLogger('backup_log')
inventory_logger = logging.getLogger('inventory_log')
heart_logger = logging.getLogger('heart_log')
prepare_logger = logging.getLogger('prepare_log')
