import json
import opcua
from datetime import datetime, timedelta
from databases.excute_db import SQLiteConnector
from config.log_config import inventory_logger


def update_stock(server, ip_port, device, namespace, lk, stock_map):
    """
    更新Qtime和标签的值
    """
    try:
        inventory_logger.info(f'{ip_port} {device} {lk}: 检测是否需要更新堆栈库存信息...')
        obj = SQLiteConnector(f'databases/{device}.sqlite3')
        now_time = datetime.now()
        # 更新Qtime
        wait_data = obj.run(f"""select * from stock_inventory where deposit=10""")
        if wait_data:
            wait_updates = []
            for data in wait_data:
                id, storge_time, update_time = data['id'], data['storge_time'], data['update_time']
                if not update_time:
                    continue
                strp_update_time = datetime.strptime(update_time, '%Y-%m-%d %H:%M:%S.%f')
                new_storge_time = (now_time - strp_update_time).seconds // 60
                if new_storge_time == 0:
                    continue
                wait_updates.append((new_storge_time, id))
            if wait_updates:
                obj.multi_update(f"""update stock_inventory set storge_time=? where id=?""", wait_updates)
                inventory_logger.info(f'{ip_port} {device} {lk}: 更新堆栈Qtime成功')
        # 更新库存标签
        datas = obj.run("select * from stock_inventory")
        new_inventory = json.dumps(datas)
        server.get_node(f"ns={namespace};s=stock_inventory").set_value(new_inventory)
        inventory_logger.info(f'{ip_port} {device} {lk}: 更新堆栈标签1成功')
        # 更新另外一个料口的库存信息
        other_port = stock_map.get(lk)
        if other_port:
            other_ip_port = f"{ip_port.split(':')[0]}:{other_port}"
            t_client = opcua.Client(f'opc.tcp://{other_ip_port}')
            t_client.connect()
            t_client.get_node('ns=2;s=stock_inventory').set_value(new_inventory)
            t_client.disconnect()
            inventory_logger.info(f'{ip_port} {device} {lk}: 更新堆栈标签2成功')
    except Exception as e:
        inventory_logger.error(f'{ip_port} {device} {lk}: 更新堆栈Qtime和库存标签出现异常：{e}')
