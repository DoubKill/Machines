import os
import sys
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from databases.excute_db import SQLiteConnector
from config.json_config import JsonConf

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
databases_file_path = os.path.join(BASE_DIR, 'databases')


def get_files():
    config = JsonConf.load(os.path.join(BASE_DIR, 'config', 'config.json'))
    files = []
    for device, v in config.items():
        if not device.startswith('DZ'):
            continue
        lk_list = [os.path.join(databases_file_path, f"{device}.sqlite3")]
        files.extend(lk_list)
    return files


def clean_stock():
    files = get_files()
    for file_path in files:
        db = SQLiteConnector(file_path)
        db.update(f"""update stock_inventory set process_id=Null, equip_id=Null, material_out_time=Null, Qtime=Null, in_order_id=Null, deposit=0, is_full=Null, material_type=Null, update_time=Null, storge_time=0""")


if __name__ == '__main__':
    try:
        clean_stock()
    except Exception as e:
        print(f'清除堆栈库存数据失败！{e}')
    else:
        print('清除堆栈库存数据成功！')
