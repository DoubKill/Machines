import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from opcua import Client
from config.json_config import JsonConf
from config.log_config import backup_logger
from multiprocessing.dummy import Pool as ThreadPool
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_path = os.path.join(BASE_DIR, 'config')


def read_data(single_lk, opcua_ip):
    variates = {}
    client = Client(f"opc.tcp://{opcua_ip}:{single_lk['port']}", timeout=1)
    try:
        client.connect()
        for lk_k, lk_v in single_lk.items():
            var = client.get_node(f"ns=2;s={lk_k}")
            variates[lk_k] = var.get_value()
    except Exception as e:
        backup_logger.info(f'e: {e}')
    finally:
        try:
            client.disconnect()
        except Exception as e:
            backup_logger.info(f'关闭opcua client失败: {e}')
    return variates


def backup_data(save_file_path):
    j_obj = JsonConf()
    config = j_obj.load(os.path.join(config_path, 'config.json'))
    results = {}
    opcua_ip = os.environ.get('OPCUA_IP', '127.0.0.1')

    pool = ThreadPool()
    for device, content in config.items():
        for single_lk in content:
            res = pool.apply_async(read_data, args=(single_lk, opcua_ip))
            try:
                _res = res.get()
            except Exception as e:
                backup_logger.error(e)
                return
            else:
                if _res:
                    results[device] = results.get(device, []) + [_res]
    pool.close()
    pool.join()

    if results:
        save_path = os.path.join(config_path, save_file_path)
        j_obj.set(results, save_path)


def job_listener(Event):
    if not Event.exception:
        backup_logger.info(f'任务正常运行！')
    else:
        backup_logger.error(f'任务出错了！任务ID:{Event.job_id}, 任务异常信息:{Event.exception}, 堆栈跟踪: {Event.traceback}')


def run_backup():
    try:
        backup_scheduler = BlockingScheduler()
        backup_scheduler.add_job(backup_data, args=('config_backup.json',), trigger='interval', seconds=30)
        backup_scheduler.add_listener(job_listener, mask=EVENT_JOB_ERROR | EVENT_JOB_MISSED | EVENT_JOB_EXECUTED)
        backup_scheduler.start()
    except Exception as e:
        backup_logger.error(f"启动备份程序失败{e}")


if __name__ == '__main__':
    run_backup()
