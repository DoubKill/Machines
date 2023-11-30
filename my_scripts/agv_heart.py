import time
import argparse
import traceback
from opcua import Client


def write_agv_heart(host, port, namespace, label):
    """模拟agv心跳"""
    # 连接服务
    client = Client(f'opc.tcp://{host}:{port}')
    client.connect()
    try:
        while True:
            now_agv_heart = client.get_node(f"ns={namespace};s={label}").get_value()
            next_agv_heart = 1 if now_agv_heart == 255 else (now_agv_heart + 1)
            print(f'当前机台心跳: {now_agv_heart} 下一次机台心跳: {next_agv_heart}')
            client.get_node(f"ns={namespace};s={label}").set_value(next_agv_heart)
            time.sleep(1)
    except KeyboardInterrupt:
        print("程序被用户中断")
    finally:
        client.disconnect()


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('-i', '--host', type=str, help='plc服务ip地址')
        parser.add_argument('-p', '--port', type=str, help='plc服务端口')
        parser.add_argument('-n', '--namespace', type=int, default=2, help='点位标签所处命名空间')
        parser.add_argument('-l', '--label', type=str, help='点位标签名称')
        args, remaining_argv = parser.parse_known_args()
        host = args.host
        port = args.port
        namespace = args.namespace
        label = args.label
        if not all([host, port, namespace, label]):
            raise ValueError('启动参数异常:-i ip地址 -p 端口 -n 标签所处命名空间  -l 标签名称')
        write_agv_heart(host, port, namespace, label)
    except Exception as e:
        print(f'模拟agv心跳出现异常: {traceback.format_exc()}')
