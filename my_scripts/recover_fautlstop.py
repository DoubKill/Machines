"""
__Title__ = 'recover_fautlstop.py'
__Author__ = 'yangzhenchao'
__Date__ = '2023/11/3'
__Version__ = 'Python 3.9'
__Software__ = 'PyCharm'
"""
import os
import sys
import ast
from opcua import *

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config.json_config import JsonConf
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_file_path = os.path.join(BASE_DIR, 'config', 'config.json')


def get_stations():
    j_obj = JsonConf()
    config = j_obj.load(config_file_path)
    device_port = {}
    if config:
        for device, content in config.items():
            for single_lk in content:
                node_id = 'GZ_StackFaultStop' if device.startswith('DZ') else 'GZ_EquipmentFaultStop'
                device_port[single_lk['lk_code']] = [single_lk['port'], node_id]
    return device_port


def foo(device, port_node, ip='10.10.130.58', set_value=0):
    try:
        c = Client(f'opc.tcp://{ip}:{port_node[0]}')
        c.connect()
        c.get_node(f'ns=2;s={port_node[1]}').set_value(set_value)
        c.disconnect()
    except Exception as e:
        print(f'{device} {port_node}出错了: {e}')


def run():
    device_port = get_stations()
    if not device_port:
        print('未加载到配置')
        return
    for device, port_node in device_port.items():
        foo(device, port_node)


if __name__ == '__main__':
    run()
