import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from multiprocessing import Value
from multiprocessing.pool import ThreadPool
from config.log_config import error_logger, heart_logger

tran_time = 10  # 传篮时间
floor_capacity = 80


def check_dock_heart(server, ip_port, device, namespace, lk, task_type, heart_label='GZ_AGVHeartBeat'):
    """检查对接心跳"""
    error_logger.info(f'{ip_port} {device} {lk}: 检查{task_type}对接心跳。。。')
    now_time = datetime.now().replace(microsecond=0)
    now_agv_heart = server.get_node(f"ns={namespace};s={heart_label}").get_value()
    last_agv_heart = server.get_node(f"ns={namespace};s=Last_{heart_label}").get_value()
    heart_time = server.get_node(f"ns={namespace};s={heart_label}_heart_time").get_value()
    heart_check = True
    error_logger.info(f'{ip_port} {device} {lk}: {task_type} 当前AGV心跳: {now_agv_heart}, 上一次AGV心跳: {last_agv_heart}, 心跳记录时间: {heart_time}')
    if (now_time - datetime.strptime(heart_time, '%Y-%m-%d %H:%M:%S')).total_seconds() > 4:
        if last_agv_heart == now_agv_heart:
            heart_check = False
        else:
            server.get_node(f"ns={namespace};s=Last_{heart_label}").set_value(now_agv_heart)
            server.get_node(f"ns={namespace};s={heart_label}_heart_time").set_value(str(now_time))
    return heart_check


def heart(server, ip_port, device, namespace, lk, equip_type):
    """维持机台心跳"""
    if equip_type == 1:
        equip_heart_labels = ['GZ_EquipmentHeartBeat']
    else:
        equip_heart_labels = ['GZ_StackPutHeartBeat', 'GZ_StackFetchHeartBeat']
    heart_logger.info(f'{ip_port} {device} {lk}: 维持机台心跳。。。')
    for equip_heart_label in equip_heart_labels:
        now_equip_heart = server.get_node(f"ns={namespace};s={equip_heart_label}").get_value()
        next_equip_heart = 1 if now_equip_heart == 255 else (now_equip_heart + 1)
        server.get_node(f"ns={namespace};s={equip_heart_label}").set_value(next_equip_heart)
        heart_logger.info(f'{ip_port} {device} {lk}: 设置机台心跳成功{now_equip_heart}->{next_equip_heart}。。。')


def common_exec_dock(server, ip_port, namespace, device, lk, sl_task, xl_task, new_capacity):
    try:
        now_time = datetime.now().replace(microsecond=0)
        now_time_str = now_time.strftime('%Y-%m-%d %H:%M:%S')
        chute_list = set(server.get_node(f"ns={namespace};s=chute_list").get_value())
        fault_init = server.get_node(f"ns={namespace};s=GZ_EquipmentFaultStop").get_value()
        if sl_task != 0:
            error_logger.info(f'{ip_port} {lk}: agv送料[put]-开始上料对接++++++++++++++++++')
            # 上料口数量
            sl_num = server.get_node(f"ns={namespace};s=GZ_EquipmentLoadCasNum").get_value()
            sl_finished = server.get_node(f"ns={namespace};s=sl_finished").get_value()
            # 轮询变量
            sl_var_list = [f'GZ_AGVPutActionArrived', f'GZ_AGVPutActioning', f'GZ_AGVPutActionFinish', f'GZ_AGVPutActionRquestExit']
            for var in sl_var_list:
                signal = server.get_node(f"ns={namespace};s={var}").get_value()
                if signal == 1:
                    if var.endswith('Arrived') and sl_num == 0:
                        error_logger.info(f'{ip_port} {lk}: agv送料[put]-检测到agv到达信号{var}-{signal} 当前标识: {sl_finished} 复位机台后续信号点位++++++++++++++++++')
                        server.get_node(f"ns={namespace};s=GZ_EquipmentLoadAllowed").set_value(1)
                        # 检测到机台 arrive清理掉其他残留信号
                        server.get_node(f"ns={namespace};s=GZ_EquipmentLoading").set_value(0)
                        server.get_node(f"ns={namespace};s=GZ_EquipmentLoadFinish").set_value(0)
                        server.get_node(f"ns={namespace};s=GZ_EquipmentLoadExitAllowed").set_value(0)
                        server.get_node(f"ns={namespace};s=GZ_EquipmentFaultStop").set_value(0)
                        server.get_node(f"ns={namespace};s=sl_finished").set_value(0)
                    elif var.endswith('Actioning'):
                        error_logger.info(f'{ip_port} {lk}: agv送料[put]-检测到agv传篮开始信号{var}-{signal}, 初始故障点位值{fault_init}++++++++++++++++++')
                        f_check_result = check_dock_heart(server, ip_port, device, namespace, lk, task_type='上料')
                        if f_check_result:
                            error_logger.info(f'{ip_port} {lk}: agv送料[put]-检测到agv传篮开始信号 初始心跳正常 sl_num: {sl_num}++++++++++++++++++')
                            times = int((10 - sl_num) / 10 * tran_time)
                            while times > 0:
                                check_result = check_dock_heart(server, ip_port, device, namespace, lk, task_type='上料')
                                if not check_result:
                                    error_logger.info(f'{ip_port} {lk}: agv送料[put]-检测到agv传篮开始信号{var}-{signal}，但是agv心跳异常++++++++++++++++++')
                                    server.get_node(f"ns={namespace};s=GZ_EquipmentFaultStop").set_value(1)
                                    already_trans = int((tran_time - times) // 2)
                                    server.get_node(f"ns={namespace};s=GZ_EquipmentLoadCasNum").set_value(already_trans)
                                    error_logger.info(f'{ip_port} {lk}: agv送料[put]-检测到agv传篮开始信号 心跳异常设置故障点位为1 设置花篮数{already_trans} ++++++++++++++++++')
                                    return
                                time.sleep(1)
                                times -= 1
                        else:
                            error_logger.info(f'{ip_port} {lk}: agv送料[put]-检测到agv传篮开始信号 初始心跳异常 sl_num: {sl_num}------------------')
                            return
                        server.get_node(f"ns={namespace};s=GZ_EquipmentLoading").set_value(1)
                        server.get_node(f"ns={namespace};s=GZ_EquipmentLoadAllowed").set_value(0)
                        error_logger.info(f'{ip_port} {lk}: agv送料[put]-检测到agv传篮开始信号 心跳检测正常 滚动置1 sl_num: {sl_num}------------------')
                        # 增加延时给wcs检测信号的时间
                        time.sleep(5)
                    elif var.endswith('Finish'):
                        error_logger.info(f'{ip_port} {lk}: agv送料[put]-检测到agv送料完成信号{var}-{signal}++++++++++++++++++')
                        # 故障恢复后又重发finish信号导致数量覆盖(1、对接挂掉 2、agv未读到允许离开时挂掉)[针对第二种情况]
                        get_exit_allow = server.get_node(f"ns={namespace};s=GZ_EquipmentLoadExitAllowed").get_value()
                        if get_exit_allow == 1:
                            server.get_node(f"ns={namespace};s=GZ_EquipmentFaultStop").set_value(0)
                            server.get_node(f"ns={namespace};s=GZ_EquipmentLoadFinish").set_value(1)
                            server.get_node(f"ns={namespace};s=GZ_EquipmentLoading").set_value(0)
                            error_logger.info(f'{ip_port} {lk}: agv送料[put]-检测到agv传篮完成信号{var}-{signal}, 读取到允许离开信号 {get_exit_allow}++++++++++++++++++')
                            continue
                        server.get_node(f"ns={namespace};s=GZ_EquipmentLoadFinish").set_value(1)
                        server.get_node(f"ns={namespace};s=GZ_EquipmentLoading").set_value(0)
                        error_logger.info(f'{ip_port} {lk}: agv送料[put]-当前上料数 {sl_num}, 完成标识:{sl_finished}++++++++++++++++++')
                        if sl_finished == 0:
                            # 缓存数是0需要更新上料时间
                            hc = server.get_node(f"ns={namespace};s=hc").get_value()
                            if chute_list == {1}:
                                if hc == 0:
                                    server.get_node(f"ns={namespace};s=sl_time").set_value(now_time_str)
                                    error_logger.info(f'{ip_port} {lk}上料完成后更新最新一次上料时间(当前对接完成缓存是{hc})')
                            else:
                                server.get_node(f"ns={namespace};s=sl_time").set_value(now_time_str)
                                error_logger.info(f'{ip_port} {lk}上料完成后更新最新一次上料时间(当前对接完成缓存是{hc})')
                            server.get_node(f"ns={namespace};s=GZ_EquipmentLoadCasNum").set_value(10)
                            server.get_node(f"ns={namespace};s=sl_finished").set_value(1)
                            with new_capacity.get_lock():
                                new_capacity.value += 10
                            error_logger.info(f'{ip_port} {lk}: agv送料[put]-送料完成++++++++++++++++++')
                        # 完成对接后清除故障点位
                        server.get_node(f"ns={namespace};s=GZ_EquipmentFaultStop").set_value(0)
                        error_logger.info(f'{ip_port} {lk}: agv取料[put]-检测到agv传篮完成信号 恢复故障点位为0 ++++++++++++++++++')
                    elif var.endswith('Exit'):
                        error_logger.info(f'{ip_port} {lk}: agv送料[put]-检测到agv请求离开信号 {var}-{signal}++++++++++++++++++')
                        server.get_node(f"ns={namespace};s=GZ_EquipmentLoadExitAllowed").set_value(1)
                        time.sleep(3)
                        server.get_node(f"ns={namespace};s=GZ_EquipmentLoadFinish").set_value(0)
                        server.get_node(f"ns={namespace};s=sl_finished").set_value(0)
                    else:
                        error_logger.info(f'{ip_port} {lk}: agv送料[put]-检测到agv其他信号{var}-{signal}-{sl_num}++++++++++++++++++')
                else:
                    if var.endswith('Exit'):
                        server.get_node(f"ns={namespace};s=GZ_EquipmentLoadExitAllowed").set_value(0)
        if xl_task != 0:
            error_logger.info(f'{ip_port} {lk}: agv取料[fetch]-开始下料对接------------------')
            # 下料口数量
            xl_num = server.get_node(f"ns={namespace};s=GZ_EquipmentUnloadCasNum").get_value()
            xl_finished = server.get_node(f"ns={namespace};s=xl_finished").get_value()
            tail_material = server.get_node(f"ns={namespace};s=GZ_EquipmentTailMaterial").get_value()
            # 轮询变量
            xl_var_list = [f"GZ_AGVFetchActionArrived", f'GZ_AGVFetchActionBegin', f'GZ_AGVFetchActionFinish', f'GZ_AGVFetchActionRquestExit']
            for var in xl_var_list:
                signal = server.get_node(f"ns={namespace};s={var}").get_value()
                if signal == 1:
                    if var.endswith('Arrived') and xl_num == 10:
                        error_logger.info(f'{ip_port} {lk}: agv取料[fetch]-检测到agv到达信号{var}-{signal} 当前标识: {xl_finished} 复位机台后续信号点位------------------')
                        server.get_node(f"ns={namespace};s=GZ_EquipmentUnloadAllowed").set_value(1)
                        # 检测到机台 arrive清理掉其他残留信号
                        server.get_node(f"ns={namespace};s=GZ_EquipmentUnloading").set_value(0)
                        server.get_node(f"ns={namespace};s=GZ_EquipmentUnloadFinish").set_value(0)
                        server.get_node(f"ns={namespace};s=GZ_EquipmentUnloadExitAllowed").set_value(0)
                        server.get_node(f"ns={namespace};s=GZ_EquipmentFaultStop").set_value(0)
                        server.get_node(f"ns={namespace};s=xl_finished").set_value(0)
                    elif var.endswith('Begin'):
                        error_logger.info(f'{ip_port} {lk}: agv取料[fetch]-检测到传篮开始信号{var}-{signal}, 初始故障点位值{fault_init}------------------')
                        f_check_result = check_dock_heart(server, ip_port, device, namespace, lk, task_type='下料')
                        if f_check_result:
                            error_logger.info(f'{ip_port} {lk}: agv取料[fetch]-检测到传篮开始信号 初始心跳正常 xl_num: {xl_num}------------------')
                            times = int(xl_num / 10 * tran_time)
                            while times > 0:
                                check_result = check_dock_heart(server, ip_port, device, namespace, lk, task_type='下料')
                                if not check_result:
                                    server.get_node(f"ns={namespace};s=GZ_EquipmentFaultStop").set_value(1)
                                    already_trans = int((tran_time - times) // 2)
                                    server.get_node(f"ns={namespace};s=GZ_EquipmentUnloadCasNum").set_value(10 - already_trans)
                                    error_logger.info(f'{ip_port} {lk}: agv取料[fetch]-检测到传篮开始信号 心跳异常设置故障点位为1 设置花篮数{10 - already_trans} ------------------')
                                    return
                                time.sleep(1)
                                times -= 1
                                # 临时检查finish
                                temp_finish = server.get_node(f"ns={namespace};s=GZ_AGVFetchActionFinish").get_value()
                                error_logger.info(f'{ip_port} {lk}: agv取料[fetch]-临时检查finish: {temp_finish} ------------------')
                        else:
                            error_logger.info(f'{ip_port} {lk}: agv取料[fetch]-检测到传篮开始信号 初始心跳异常 xl_num: {xl_num}------------------')
                            return
                        server.get_node(f"ns={namespace};s=GZ_EquipmentUnloading").set_value(1)
                        server.get_node(f"ns={namespace};s=GZ_EquipmentUnloadAllowed").set_value(0)
                        error_logger.info(f'{ip_port} {lk}: agv取料[fetch]-检测到agv传篮开始信号 心跳检测正常 滚动置1 xl_num: {xl_num}------------------')
                        # 增加延时给wcs检测信号的时间
                        time.sleep(5)
                    elif var.endswith('Finish'):
                        error_logger.info(f'{ip_port} {lk}: agv取料[fetch]-检测到agv传篮完成信号{var}-{signal}------------------')
                        # 故障恢复后又重发finish信号导致数量覆盖(1、对接挂掉 2、agv未读到允许离开时挂掉)[针对第二种情况]
                        get_exit_allow = server.get_node(f"ns={namespace};s=GZ_EquipmentUnloadExitAllowed").get_value()
                        if get_exit_allow == 1:
                            error_logger.info(f'{ip_port} {lk}: agv取料[fetch]-检测到agv传篮完成信号{var}-{signal}, 读取到允许离开信号 {get_exit_allow}------------------')
                            server.get_node(f"ns={namespace};s=GZ_EquipmentFaultStop").set_value(0)
                            server.get_node(f"ns={namespace};s=GZ_EquipmentUnloadFinish").set_value(1)
                            server.get_node(f"ns={namespace};s=GZ_EquipmentUnloading").set_value(0)
                            continue
                        server.get_node(f"ns={namespace};s=GZ_EquipmentUnloadFinish").set_value(1)
                        server.get_node(f"ns={namespace};s=GZ_EquipmentUnloading").set_value(0)
                        error_logger.info(f'{ip_port} {lk}: agv取料[fetch]-当前下料数 {xl_num}, 完成标识:{xl_finished}------------------')
                        if xl_finished == 0:
                            # 更新下料时间
                            server.get_node(f"ns={namespace};s=xl_time").set_value(now_time_str)
                            error_logger.info(f'{ip_port} {lk}上料完成后更新最新一次下料时间')
                            # 更新下料数量
                            server.get_node(f"ns={namespace};s=GZ_EquipmentUnloadCasNum").set_value(0)
                            server.get_node(f"ns={namespace};s=xl_finished").set_value(1)
                            with new_capacity.get_lock():
                                new_capacity.value += xl_num
                            error_logger.info(f'{ip_port} {lk}: agv取料[fetch]-取料完成------------------')
                        server.get_node(f"ns={namespace};s=GZ_EquipmentFaultStop").set_value(0)
                        error_logger.info(f'{ip_port} {lk}: agv取料[fetch]-检测到传篮开始信号 恢复故障点位为0 ------------------')
                    elif var.endswith('Exit'):
                        error_logger.info(f'{ip_port} {lk}: agv取料[fetch]-检测到agv请求离开信号{var}-{signal}------------------')
                        server.get_node(f"ns={namespace};s=GZ_EquipmentUnloadExitAllowed").set_value(1)
                        time.sleep(3)
                        server.get_node(f"ns={namespace};s=GZ_EquipmentUnloadFinish").set_value(0)
                        server.get_node(f"ns={namespace};s=xl_finished").set_value(0)
                    else:
                        error_logger.info(f'{ip_port} {lk}: agv取料[fetch]-检测到agv其他信号{var}-{signal}-{xl_num}------------------')
                else:
                    if var.endswith('Exit'):
                        server.get_node(f"ns={namespace};s=GZ_EquipmentUnloadExitAllowed").set_value(0)

    except Exception as e:
        error_logger.error(f'{ip_port} {lk}: agv取料[fetch]-异常-{traceback.format_exc()}')
        raise e
    return True


def common_device_dock(server, ip_port, device, namespace, lk):
    """
    设备对接任务
    :break:
    """
    try:
        # 查询设备状态，异常直接返回
        equip_status = server.get_node(f"ns={namespace};s=GZ_EquipmentProductionStatus").get_value()
        if equip_status in [1, 2, 6]:
            error_logger.warning(f'{ip_port} {lk}: 设备状态{equip_status}异常')
            return
        # 站台屏蔽
        equip_disabled = server.get_node(f"ns={namespace};s=GZ_EquipmentDisable").get_value()
        if equip_disabled == 1:
            error_logger.warning(f'{ip_port} {lk}: 站台已被屏蔽!')
            return
        load_disabled = server.get_node(f"ns={namespace};s=GZ_EquipmentLoadDisable").get_value()
        unload_disabled = server.get_node(f"ns={namespace};s=GZ_EquipmentUnloadDisable").get_value()
        sl_task, xl_task = 0, 0
        if load_disabled == 0:
            sl_task = 1
        if unload_disabled == 0:
            xl_task = 1
        # 进入对接流程
        if any([sl_task, xl_task]):
            capacity = server.get_node(f"ns={namespace};s=GZ_TotaolProductionCapcity").get_value()
            new_capacity = Value('i', capacity)
            p = ThreadPoolExecutor()
            if sl_task != 0:
                p.submit(common_exec_dock, server, ip_port, namespace, device, lk, sl_task, 0, new_capacity)
            if xl_task != 0:
                p.submit(common_exec_dock, server, ip_port, namespace, device, lk, 0, xl_task, new_capacity)
            p.shutdown()
            if new_capacity.value != capacity:
                server.get_node(f"ns={namespace};s=GZ_TotaolProductionCapcity").set_value(new_capacity.value)
    except Exception as e:
        error_logger.error(f'{ip_port} {lk}: 设备对接异常-{e}')


def other_exec_dock(server, ip_port, namespace, device, lk, task_type):
    try:
        store_cancel = server.get_node(f'ns={namespace};s=GZ_StackStoreCancel').get_value()
        fetch_cancel = server.get_node(f'ns={namespace};s=GZ_StackFetchCancel').get_value()
        if task_type == 1:
            # 是否主屏蔽
            store_disabled = server.get_node(f'ns={namespace};s=GZ_StackDisableIn').get_value()
            if store_disabled == 1:
                error_logger.info(f'{ip_port} {device} {lk}: agv堆栈入库[put]-堆栈入库屏蔽++++++++++++++++++')
                return
            if store_cancel == 1:  # 取消进料任务
                server.get_node(f'ns={namespace};s=GZ_StackLoadAllowed').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_StackLoadNotAllowed').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_StackLoading').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_StackLoadFinish').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_StackLoadExitAllowed').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_StackLoadExitNotAllowed').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_StackStoreCommond').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_StackStoreFloorNum').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_StackStoreHeight').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_StackStoreCancel').set_value(0)
                error_logger.info(f'{ip_port} {device} {lk}: agv堆栈送料[put]-取消进料任务++++++++++++++++++')
                return
            # 是否下发任务
            store_commond = server.get_node(f'ns={namespace};s=GZ_StackStoreCommond').get_value()
            store_floor_num = server.get_node(f'ns={namespace};s=GZ_StackStoreFloorNum').get_value()
            store_height = server.get_node(f'ns={namespace};s=GZ_StackStoreHeight').get_value()
            if 0 in [store_commond, store_floor_num, store_height]:
                error_logger.info(f'{ip_port} {device} {lk}: agv堆栈送料[put]-无进料任务++++++++++++++++++')
                return
            # 检查是否全满
            store_full = server.get_node(f'ns={namespace};s=GZ_StackFloor{store_floor_num}FullOrEmpty').get_value()
            finish_flag = server.get_node(f'ns={namespace};s=GZ_StackLoadFinish').get_value()
            exit_flag = server.get_node(f'ns={namespace};s=GZ_StackLoadExitAllowed').get_value()
            if store_full == 1 and finish_flag == 0 and exit_flag == 0:
                error_logger.info(f'{ip_port} {device} {lk}: agv堆栈送料[put]-堆栈全满++++++++++++++++++')
                return
            error_logger.info(f'{ip_port} {device} {lk}: agv堆栈送料[put]-开始上料对接++++++++++++++++++')
            now_time = datetime.now()
            # 轮询变量
            sl_var_list = [f'GZ_AGVPutActionArrived', f'GZ_AGVPutActioning', f'GZ_AGVPutActionFinish', f'GZ_AGVPutActionRquestExit']
            sl_finished = server.get_node(f"ns={namespace};s=sl_finished").get_value()
            for var in sl_var_list:
                signal = server.get_node(f"ns={namespace};s={var}").get_value()
                if signal == 1:
                    if var.endswith('Arrived'):
                        error_logger.info(f'{ip_port} {device} {lk}: agv堆栈送料[put]-检测到agv到达信号 {var}-{signal}++++++++++++++++++')
                        server.get_node(f"ns={namespace};s=GZ_StackLoadAllowed").set_value(1)
                    elif var.endswith('Actioning'):
                        error_logger.info(f'{ip_port} {device} {lk}: agv堆栈送料[put]-检测到agv传篮开始信号 {var}-{signal}++++++++++++++++++')
                        server.get_node(f"ns={namespace};s=GZ_StackLoading").set_value(1)
                        server.get_node(f"ns={namespace};s=GZ_StackLoadAllowed").set_value(0)
                    elif var.endswith('Finish'):
                        error_logger.info(f'{ip_port} {device} {lk}: agv堆栈送料[put]-检测到agv送料完成信号 {var}-{signal}++++++++++++++++++')
                        check_result = check_dock_heart(server, ip_port, device, namespace, lk, task_type='上料', heart_label='GZ_AGVPutHeartBeat')
                        if not check_result:
                            error_logger.error(f'{ip_port} {device} {lk}: agv堆栈送料[put]-心跳检测失败++++++++++++++++++')
                            return
                        server.get_node(f"ns={namespace};s=GZ_StackLoadFinish").set_value(1)
                        server.get_node(f"ns={namespace};s=GZ_StackLoading").set_value(0)
                        if sl_finished == 0:
                            # 变更数量
                            old_num = server.get_node(f"ns={namespace};s=GZ_StackFloor{store_floor_num}CasNum").get_value()
                            new_num = (old_num + 10) if old_num + 10 <= floor_capacity else floor_capacity
                            capacity_state = 1 if new_num == floor_capacity else 2
                            server.get_node(f"ns={namespace};s=GZ_StackFloor{store_floor_num}CasNum").set_value(new_num)
                            server.get_node(f"ns={namespace};s=GZ_StackFloor{store_floor_num}FullOrEmpty").set_value(capacity_state)
                            server.get_node(f"ns={namespace};s=sl_finished").set_value(1)
                            error_logger.info(f'{ip_port} {device} {lk}: agv堆栈送料[put]-变更数量 {old_num}->{new_num}++++++++++++++++++')
                    elif var.endswith('Exit'):
                        error_logger.info(f'{ip_port} {device} {lk}: agv堆栈送料[put]-检测到agv请求离开信号 {var}-{signal}++++++++++++++++++')
                        server.get_node(f"ns={namespace};s=GZ_StackLoadExitAllowed").set_value(1)
                        server.get_node(f"ns={namespace};s=GZ_StackLoadFinish").set_value(0)
                        time.sleep(3)
                        sl_finished = server.get_node(f"ns={namespace};s=sl_finished").set_value(0)
                    else:
                        error_logger.info(f'{ip_port} {device} {lk}: agv堆栈送料[put]-检测到agv其他信号{var}-{signal}++++++++++++++++++')
                else:
                    if var.endswith('Exit'):
                        allow_exit = server.get_node(f"ns={namespace};s=GZ_StackLoadExitAllowed").get_value()
                        if allow_exit == 1:
                            server.get_node(f"ns={namespace};s=GZ_StackLoadExitAllowed").set_value(0)
                            server.get_node(f'ns={namespace};s=GZ_StackStoreCommond').set_value(0)
                            server.get_node(f'ns={namespace};s=GZ_StackStoreFloorNum').set_value(0)
                            server.get_node(f'ns={namespace};s=GZ_StackStoreHeight').set_value(0)
                            server.get_node(f'ns={namespace};s=GZ_StackStoreCancel').set_value(0)
        else:
            # 是否主屏蔽
            fetch_disabled = server.get_node(f'ns={namespace};s=GZ_StackDisableOut').get_value()
            if fetch_disabled == 1:
                error_logger.info(f'{ip_port} {device} {lk}: agv堆栈出库[fetch]-堆栈出库屏蔽------------------')
                return
            if fetch_cancel == 1:  # 取消出料任务
                server.get_node(f'ns={namespace};s=GZ_StackUnloadAllowed').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_StackUnloadNotAllowed').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_StackUnloading').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_StackUnloadFinish').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_StackUnloadExitAllowed').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_StackUnloadExitNotAllowed').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_StackFetchCommond').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_StackFetchFloorNum').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_StackFetchHeight').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_StackFetchCancel').set_value(0)
                error_logger.info(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-取消出料任务------------------')
                return
            fetch_commond = server.get_node(f'ns={namespace};s=GZ_StackFetchCommond').get_value()
            fetch_floor_num = server.get_node(f'ns={namespace};s=GZ_StackFetchFloorNum').get_value()
            fetch_height = server.get_node(f'ns={namespace};s=GZ_StackFetchHeight').get_value()
            if 0 in [fetch_commond, fetch_floor_num, fetch_height]:
                error_logger.info(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-无出料任务------------------')
                return
            # 检查是否全满
            fetch_full = server.get_node(f'ns={namespace};s=GZ_StackFloor{fetch_floor_num}FullOrEmpty').get_value()
            finish_flag = server.get_node(f'ns={namespace};s=GZ_StackUnloadFinish').get_value()
            exit_flag = server.get_node(f'ns={namespace};s=GZ_StackUnloadExitAllowed').get_value()
            if fetch_full == 0 and finish_flag == 0 and exit_flag == 0:
                error_logger.info(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-堆栈全空------------------')
                return
            error_logger.info(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-开始下料对接------------------')
            # 轮询变量
            xl_var_list = [f"GZ_AGVFetchActionArrived", f'GZ_AGVFetchActionBegin', f'GZ_AGVFetchActionFinish', f'GZ_AGVFetchActionRquestExit']
            xl_finished = server.get_node(f"ns={namespace};s=xl_finished").get_value()
            for var in xl_var_list:
                signal = server.get_node(f"ns={namespace};s={var}").get_value()
                if signal == 1:
                    if var.endswith('Arrived'):
                        error_logger.info(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-检测到agv到达信号 {var}-{signal}------------------')
                        server.get_node(f"ns={namespace};s=GZ_StackUnloadAllowed").set_value(1)
                    elif var.endswith('Begin'):
                        error_logger.info(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-检测到传篮开始信号 {var}-{signal}------------------')
                        server.get_node(f"ns={namespace};s=GZ_StackUnloading").set_value(1)
                        server.get_node(f"ns={namespace};s=GZ_StackUnloadAllowed").set_value(0)
                    elif var.endswith('Finish'):
                        error_logger.info(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-检测到agv取货完成信号 {var}-{signal}------------------')
                        check_result = check_dock_heart(server, ip_port, device, namespace, lk, task_type='下料', heart_label='GZ_AGVFetchHeartBeat')
                        if not check_result:
                            error_logger.error(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-心跳检测失败------------------')
                            return
                        server.get_node(f"ns={namespace};s=GZ_StackUnloadFinish").set_value(1)
                        server.get_node(f"ns={namespace};s=GZ_StackUnloading").set_value(0)
                        if xl_finished == 0:
                            # 变更数量
                            old_num = server.get_node(f"ns={namespace};s=GZ_StackFloor{fetch_floor_num}CasNum").get_value()
                            new_num = 0 if old_num < 10 else (old_num - 10)
                            capacity_state = 0 if new_num == 0 else 2
                            server.get_node(f"ns={namespace};s=GZ_StackFloor{fetch_floor_num}CasNum").set_value(new_num)
                            server.get_node(f"ns={namespace};s=GZ_StackFloor{fetch_floor_num}FullOrEmpty").set_value(capacity_state)
                            server.get_node(f"ns={namespace};s=xl_finished").set_value(1)
                            error_logger.info(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-变更数量 {old_num}->{new_num}------------------')
                    elif var.endswith('Exit'):
                        error_logger.info(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-检测到agv请求离开信号 {var}-{signal}------------------')
                        server.get_node(f"ns={namespace};s=GZ_StackUnloadFinish").set_value(0)
                        server.get_node(f"ns={namespace};s=GZ_StackUnloadExitAllowed").set_value(1)
                        time.sleep(3)
                        server.get_node(f"ns={namespace};s=xl_finished").set_value(0)
                    else:
                        error_logger.info(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-检测到agv其他信号{var}-{signal}------------------')
                else:
                    if var.endswith('Exit'):
                        allow_exit = server.get_node(f"ns={namespace};s=GZ_StackUnloadExitAllowed").get_value()
                        if allow_exit == 1:
                            server.get_node(f"ns={namespace};s=GZ_StackUnloadExitAllowed").set_value(0)
                            server.get_node(f'ns={namespace};s=GZ_StackFetchCommond').set_value(0)
                            server.get_node(f'ns={namespace};s=GZ_StackFetchFloorNum').set_value(0)
                            server.get_node(f'ns={namespace};s=GZ_StackFetchHeight').set_value(0)
                            server.get_node(f'ns={namespace};s=GZ_StackFetchCancel').set_value(0)
    except Exception as e:
        error_logger.error(f'{ip_port} {lk}: agv堆栈取料[fetch]-出现异常：{traceback.format_exc()}')
        raise e


def other_device_dock(server, ip_port, device, namespace, lk):
    """
    设备对接任务
    :break:
    """
    try:
        # 判断是入库还是出库对接
        t_pool = ThreadPool()
        for i in [1, 2]:
            t_pool.apply_async(other_exec_dock, args=(server, ip_port, namespace, device, lk, i))
        t_pool.close()
        t_pool.join()
    except Exception as e:
        error_logger.error(f'{ip_port} {device} {lk}: 堆栈对接异常{e}')

