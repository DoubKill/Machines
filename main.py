import os
import sys
from opcua import Server, ua
from multiprocessing import Pool
from task.snapshot import run_backup
from config.json_config import JsonConf
from config.log_config import api_logger
from databases.excute_db import PostgresConnector
from logic_subject.inventory import update_stock
from logic_subject.consume import device_consume_job
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from logic_subject.dock import common_device_dock, other_device_dock, heart, prepare_stock


def start_server(device, single_lk):
    chute_code, port, equip_type, chute_list = single_lk['lk_code'], single_lk['port'], single_lk['equip_type'], single_lk.get('chute_list')
    server = Server()
    opcua_ip = os.environ.get('OPCUA_IP', '127.0.0.1')
    server.set_endpoint(f"opc.tcp://{opcua_ip}:{port}")
    server.set_server_name(f"OPC UA {device}-{chute_code}:{port} Server")

    ip_port = f"{opcua_ip}:{port}"
    # 设置变量
    root_node = server.get_objects_node()
    namespace = server.register_namespace(chute_code)
    chute_node = root_node.add_object(namespace, chute_code)
    for lk_k, lk_v in single_lk.items():
        var = chute_node.add_variable(f"ns={namespace};s={lk_k}", lk_k, lk_v)
        # 设置my_var的display_name属性
        # var.set_attribute(ua.AttributeIds.DisplayName, ua.DataValue(ua.Variant('test', ua.VariantType.String)))
        var.set_writable(True)
    try:
        server.start()
    except Exception as e:
        api_logger.error(f"服务启动失败, 机台:{chute_code}, 端口:{port}, 错误信息:{e}, 启动地址: {server.endpoint}")
    else:
        api_logger.warning(f'{device}-{chute_code}:{port} Server started at {server.endpoint}')
        try:
            # 并发处理该设备中的所有点位的对接情况
            if equip_type == 1:  # 普通设备
                # 对接
                common_scheduler = BackgroundScheduler()
                common_scheduler.add_job(common_device_dock, trigger='interval', seconds=1, args=(server, ip_port, device, namespace, chute_code))
                common_scheduler.start()

                # 定时检测消耗
                consume_scheduler = BackgroundScheduler()
                consume_scheduler.add_job(device_consume_job, trigger='interval', seconds=2, args=(server, ip_port, namespace, chute_code, chute_list))
                consume_scheduler.start()

            else:
                # 堆栈口对应关系(更新同一个堆栈的所有口的库存信息)
                stock_map = {'zrkh01': 60002, 'zrkh02': 60001, 'zrkh03': 60004, 'zrkh04': 60003, 'pkh01': 60006, 'pkh02': 60005, 'thh01': 60008, 'thh02': 60007,
                             'seh01': 60010, 'seh02': 60009, 'bsgkh01': 60022, 'bsgkh02': 60021, 'lph01': 60024, 'lph02': 60023, 'lkh01': 60026, 'lkh02': 60025,
                             'psgkh01': 60042, 'psgkh02': 60041, 'aldh01': 60044, 'aldh02': 60043, 'bph03': 60062, 'bph04': 60061, 'dbh01': 60064, 'dbh02': 60063}
                stock_scheduler, other_scheduler = [BackgroundScheduler(), BackgroundScheduler()] if set(chute_list) == {1} else [None, BackgroundScheduler()]
                # 对接
                other_scheduler.add_job(other_device_dock, trigger='interval', seconds=1, args=(server, ip_port, device, namespace, chute_code))
                other_scheduler.start()
                # 堆栈只启动一个定时任务去更新更新Qtime和标签的值
                if stock_scheduler:
                    stock_scheduler.add_job(update_stock, trigger='interval', seconds=3, args=(server, ip_port, device, namespace, chute_code, stock_map))
                    stock_scheduler.start()

                # 备料定时任务
                prepare_scheduler = BackgroundScheduler()
                prepare_scheduler.add_job(prepare_stock, trigger='interval', seconds=3, args=(server, ip_port, device, namespace, chute_code))
                prepare_scheduler.start()

            # 增加机台心跳
            heart_scheduler = BlockingScheduler()
            heart_scheduler.add_job(heart, trigger='interval', seconds=3, args=(server, ip_port, device, namespace, chute_code, equip_type))
            heart_scheduler.start()

        except Exception as e:
            api_logger.error(f'device: {device}, ip_port: {ip_port}, chute_code: {chute_code}, chute_list: {chute_list}定时任务启动失败, 错误信息: {e}')


def main(config_path):
    # 读取配置文件
    j_obj = JsonConf()
    config = j_obj.load(config_path)
    if config:
        # 读取mcs节拍
        platform = PostgresConnector().run('select * from bdm_platform_info')
        pitch_times = {i['location_name']: i['pitch_time'] for i in platform}
        pool = Pool(180)
        for device, content in config.items():
            for single_lk in content:
                single_lk['pitch_time'] = pitch_times.get(single_lk['lk_code'], 600)
                pool.apply_async(start_server, args=(device, single_lk))
        # pool.apply_async(run_backup)

        pool.close()
        pool.join()
        pool.terminate()
    else:
        api_logger.warning(f'读取配置文件: {config_path}失败！config: {config}')


if __name__ == '__main__':
    argv = sys.argv
    if len(argv) == 2:
        keyword, config_path = argv[1], ''
        if keyword == 'common':
            config_path = 'config/config.json'
        elif keyword == 'backup':
            config_path = 'config/config_backup.json'
        elif keyword == 'exit_save':
            config_path = 'config/config_exit_save.json'
        else:
            print('只支持common、backup、exit_save参数')
        if config_path:
            try:
                main(config_path)
            except Exception as e:
                print(f'通过配置文件 {config_path} 启动服务时出现异常: {e}')
    else:
        print('缺少运行参数！')
