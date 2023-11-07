import sys
import time
import traceback
import argparse
from opcua import *


def run(ip, port, o_type, t_time):
    c = Client(f'opc.tcp://{ip}:{port}')
    c.connect()
    if o_type == 1:
        sl_var_list = [f'GZ_AGVPutActionArrived', f'GZ_AGVPutActioning', f'GZ_AGVPutActionFinish', f'GZ_AGVPutActionRquestExit']
        for var in sl_var_list:
            signal = c.get_node(f'ns=2;s={var}').get_value()
            if signal == 1:
                if var.endswith('Arrived'):
                    print(f'到达: {var}, 设置allowed-1')
                    c.get_node(f'ns=2;s=GZ_EquipmentLoadAllowed').set_value(1)
                elif var.endswith('Actioning'):
                    print(f'传篮开始: {var}, 设置loading-1, allowed-0')
                    c.get_node(f"ns=2;s=GZ_EquipmentLoading").set_value(1)
                    c.get_node(f"ns=2;s=GZ_EquipmentLoadAllowed").set_value(0)
                elif var.endswith('Finish'):
                    print(f'完成: {var}, 设置finish-1, loading-0')
                    c.get_node(f"ns=2;s=GZ_EquipmentLoadFinish").set_value(1)
                    c.get_node(f"ns=2;s=GZ_EquipmentLoading").set_value(0)
                elif var.endswith('Exit'):
                    print(f'离开: {var}, 设置exit_allowed-1, finish-0')
                    c.get_node(f"ns=2;s=GZ_EquipmentLoadExitAllowed").set_value(1)
                    c.get_node(f"ns=2;s=GZ_EquipmentLoadFinish").set_value(0)
                else:
                    pass
            else:
                if var.endswith('Exit'):
                    c.get_node(f"ns=2;s=GZ_EquipmentLoadExitAllowed").set_value(0)


    elif o_type == 2:
        xl_var_list = [f"GZ_AGVFetchActionArrived", f'GZ_AGVFetchActionBegin', f'GZ_AGVFetchActioning', f'GZ_AGVFetchActionFinish', f'GZ_AGVFetchActionRquestExit']
    else:
        print(f'未知类型: {o_type}')
    c.disconnect()


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('-i', '--ip', type=str, help='ip地址')
        parser.add_argument('-p', '--port', type=int, default=4840, help='端口')
        parser.add_argument('-o', '--o_type', type=int, help='进、出')
        parser.add_argument('-t', '--t_time', type=int, help='检测间隔')
        args, remaining_argv = parser.parse_known_args()
        ip, port, o_type, t_time = args.ip, args.port, args.o_type, args.t_time
        run(ip, port, o_type, t_time)
    except Exception as e:
        print(f"运行出现异常: {traceback.format_exc()}")
        sys.exit(1)
