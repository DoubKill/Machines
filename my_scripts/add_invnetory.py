import os
import sys
from datetime import datetime, timedelta

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from databases.excute_db import SQLiteConnector
from config.json_config import JsonConf

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
databases_file_path = os.path.join(BASE_DIR, 'databases')
stock_file_path = os.path.join(BASE_DIR, 'template_file', '增加库存.xlsx')


def get_config():
    dz = []
    config = JsonConf.load(os.path.join(BASE_DIR, 'config', 'config.json'))
    for device, v in config.items():
        if not device.startswith('DZ'):
            continue
        dz.append(device)
    return dz


def add_inventory():
    dz = get_config()
    # 读取文件
    data = pd.read_excel(stock_file_path, sheet_name='Sheet1').to_dict(orient='records')
    for d in data:
        device, seq, process_id, material_type, equip_id, material_out_time, Qtime, in_order_id = d.values()
        if not all([device, seq, process_id, material_type, equip_id, material_out_time, Qtime, in_order_id]):
            print(f'failed: 数据不完整: {d}')
            continue
        if device not in dz:
            print(f'failed: 堆栈编号 {device}-{seq} 不存在')
            continue
        # 查询空库位
        remote_file_path = os.path.join(databases_file_path, f'{device}.sqlite3')
        if not os.path.exists(remote_file_path):
            print(f'failed: 堆栈编号 {device}-{seq} 的数据库文件不存在')
            continue
        db = SQLiteConnector(remote_file_path)
        remain_check = db.run(f"""select id from stock_inventory where deposit=0""")
        if len(remain_check) < 2:
            print(f'failed: 堆栈编号 {device}-{seq} 的空库位不足2个')
            continue
        # 更新库存
        now_time = datetime.now()
        update_time = str(now_time - timedelta(minutes=material_out_time))
        db.update(f"""update stock_inventory set process_id='{process_id}', material_type='{material_type}', equip_id='{equip_id}', material_out_time='{material_out_time}', Qtime='{Qtime}', in_order_id='{in_order_id}', deposit=10, is_full=1, update_time='{update_time}' where id='{remain_check[0]['id']}'""")
        print(f'success: 堆栈编号 {device}-{seq} 增加库存成功')


if __name__ == '__main__':
    user_check = input('确认是否已经更正过增加库存.xlsx文件？1[是] 2[否]')
    if user_check == '1':
        try:
            add_inventory()
        except Exception as e:
            print(f'增加库存时出现异常: {e}')
        else:
            print('脚本执行完成')
    else:
        print('请先更正增加库存.xlsx文件, 文件路径: Machines/template_file/增加库存.xlsx')

