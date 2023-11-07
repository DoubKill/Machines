"""
__Title__ = 'check_baskets.py'
__Author__ = 'yangzhenchao'
__Date__ = '2023/9/8'
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
            if device.startswith('DZ'):
                continue
            for single_lk in content:
                if set(single_lk['chute_list']) != {1, 2}:
                    continue
                device_port[single_lk['lk_code']] = single_lk['port']
    return device_port


def foo(device, port, ip='10.10.181.46', set_value=None):
    try:
        c = Client(f'opc.tcp://{ip}:{port}')
        c.connect()
        nodes = [c.get_node('ns=2;s=GZ_EquipmentLoadCasNum'), c.get_node('ns=2;s=hc'), c.get_node('ns=2;s=GZ_EquipmentUnloadCasNum')]
        if not set_value:
            sl, hc, xl = c.get_values(nodes)
            total = sl + hc + xl
            if total != 12:
                if sl > xl:
                    n_sl = sl + 1 if sl % 2 != 0 else sl
                    n_hc = 2
                    n_xl = 12 - n_sl - n_hc
                elif sl < xl:
                    n_xl = xl + 1 if xl % 2 != 0 else xl
                    n_hc = 2
                    n_sl = 12 - n_xl - n_hc
                else:
                    n_sl, n_hc, n_xl = 4, 2, 6
                print(f'{device} total: {total}, sl: {sl}, hc: {hc}, xl: {xl}, 设置新的数据: sl: {n_sl}, hc: {n_hc}, xl: {n_xl}')
                res = [device, port, ip, [n_sl, n_hc, n_xl]]
            else:
                res = []
        else:
            c.set_values(nodes, set_value)
            print(f'{device} 数量设置成功: {set_value}')
            res = []
        c.disconnect()
    except Exception as e:
        res = []
    return res


def run(change_values):
    if not change_values:
        device_port = get_stations()
        if not device_port:
            print('未加载到配置')
            return
        need_change = []
        for device, port in device_port.items():
            res = foo(device, port)
            if res:
                need_change.append(res)
        print(need_change)
    else:
        for i in change_values:
            device, port, ip, set_value = i
            res = foo(device, port, ip, set_value)


if __name__ == '__main__':
    args = sys.argv
    if len(args) < 2:
        run(change_values=None)
    else:
        real_values = ast.literal_eval(args[1])
        if isinstance(real_values, list):
            run(change_values=real_values)
        else:
            print('设置的数据不是数组')
