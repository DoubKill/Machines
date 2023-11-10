import time
import traceback
from multiprocessing import Value
from datetime import datetime, timedelta
from multiprocessing.pool import ThreadPool
from concurrent.futures import ThreadPoolExecutor
from databases.excute_db import SQLiteConnector
from config.log_config import error_logger, heart_logger, prepare_logger

tran_time = 10  # 传篮时间


def check_dock_heart(server, ip_port, device, namespace, lk, task_type):
    """检查对接心跳"""
    error_logger.info(f'{ip_port} {device} {lk}: 检查{task_type}对接心跳。。。')
    now_time = datetime.now().replace(microsecond=0)
    now_agv_heart = server.get_node(f"ns={namespace};s=GZ_AGVHeartBeat").get_value()
    last_agv_heart = server.get_node(f"ns={namespace};s=Last_GZ_AGVHeartBeat").get_value()
    heart_time = server.get_node(f"ns={namespace};s=heart_time").get_value()
    heart_check = True
    error_logger.info(f'{ip_port} {device} {lk}: {task_type} 当前AGV心跳: {now_agv_heart}, 上一次AGV心跳: {last_agv_heart}, 心跳记录时间: {heart_time}')
    if (now_time - datetime.strptime(heart_time, '%Y-%m-%d %H:%M:%S')).total_seconds() > 4:
        if last_agv_heart == now_agv_heart:
            heart_check = False
        else:
            server.get_node(f"ns={namespace};s=Last_GZ_AGVHeartBeat").set_value(now_agv_heart)
            server.get_node(f"ns={namespace};s=heart_time").set_value(str(now_time))
    return heart_check


def heart(server, ip_port, device, namespace, lk, equip_type):
    """维持机台心跳"""
    if equip_type == 1:
        equip_heart_label = 'GZ_EquipmentHeartBeat'
    else:
        equip_heart_label = 'GZ_StackHeartBeat'
    heart_logger.info(f'{ip_port} {device} {lk}: 维持机台心跳。。。')
    now_equip_heart = server.get_node(f"ns={namespace};s={equip_heart_label}").get_value()
    heart_logger.info(f'{ip_port} {device} {lk}: 1-当前机台心跳: {now_equip_heart}')
    next_equip_heart = 1 if now_equip_heart == 255 else (now_equip_heart + 1)
    heart_logger.info(f'{ip_port} {device} {lk}: 2-下一次机台心跳: {next_equip_heart}')
    server.get_node(f"ns={namespace};s={equip_heart_label}").set_value(next_equip_heart)
    heart_logger.info(f'{ip_port} {device} {lk}: 设置机台心跳成功。。。')


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
                            # 上料成功更新断料时间
                            obj = SQLiteConnector(f'databases/{lk}.sqlite3')
                            conn, cursor = obj.conn, obj.cursor
                            # now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            try:
                                cursor.execute('BEGIN')
                                already_record = obj.run(
                                    f"""select id, break_start_time from break_records where break_equip_code='{lk}' and break_end_time is Null order by id desc""",
                                    table_name='break_records')
                                if already_record:  # 存在这更新
                                    instance = already_record[0]
                                    sid, break_start_time = instance['id'], instance['break_start_time']
                                    break_consume = (now_time - datetime.strptime(break_start_time, '%Y-%m-%d %H:%M:%S')).seconds
                                    obj.update(f"""update break_records set break_end_time='{now_time_str}', break_consume={break_consume} where id={sid}""")
                                    conn.commit()
                                else:  # 正常对接且不存在插入一条断料时间为0的数据
                                    break_equip_id = server.get_node(f"ns={namespace};s=GZ_EquipmentID").get_value()
                                    obj.insert('break_records', {'break_equip_id': break_equip_id, 'break_equip_code': lk, 'break_start_time': now_time_str,
                                                                 'break_end_time': now_time_str, 'break_consume': 0})
                                    conn.commit()
                            except Exception as e:
                                error_logger.error(f'{ip_port} {lk} 上料完成后更新断料记录异常-{traceback.format_exc()}, 当前时间: {now_time_str}')
                                conn.rollback()
                            else:
                                error_logger.info(f'{ip_port} {lk}上料完成后更新断料记录成功')
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
                            if chute_list == {2}:
                                # 下料成功更新断料时间
                                obj = SQLiteConnector(f'databases/{lk}.sqlite3')
                                conn, cursor = obj.conn, obj.cursor
                                # now_time = datetime.now().replace(microsecond=0)
                                try:
                                    cursor.execute('BEGIN')
                                    already_record = obj.run(
                                        f"""select id, break_start_time from break_records where break_equip_code='{lk}' and break_end_time is Null order by id desc""",
                                        table_name='break_records')
                                    if already_record:  # 存在这更新
                                        instance = already_record[0]
                                        sid, break_start_time = instance['id'], instance['break_start_time']
                                        break_consume = (now_time - datetime.strptime(break_start_time, '%Y-%m-%d %H:%M:%S')).seconds
                                        obj.update(f"""update break_records set break_end_time='{now_time_str}', break_consume={break_consume} where id={sid}""")
                                        conn.commit()
                                    else:  # 正常对接且不存在插入一条断料时间为0的数据
                                        break_equip_id = server.get_node(f"ns={namespace};s=GZ_EquipmentID").get_value()
                                        obj.insert('break_records', {'break_equip_id': break_equip_id, 'break_equip_code': lk, 'break_start_time': now_time_str,
                                                                     'break_end_time': now_time_str, 'break_consume': 0})
                                        conn.commit()
                                except Exception as e:
                                    error_logger.error(f'{ip_port} {lk} 下料完成后更新断料记录异常-{traceback.format_exc()}, 当前时间: {now_time_str}')
                                    conn.rollback()
                                else:
                                    error_logger.info(f'{ip_port} {lk}下料完成后更新断料记录成功')
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
        # 上下料屏蔽
        # lk_id = server.get_node(f"ns={namespace};s=GZ_EquipmentID").get_value()
        # agv_lk_id = server.get_node(f"ns={namespace};s=GZ_AGVEquipmentID").get_value()
        # if lk_id != agv_lk_id:
        #     error_logger.warning(f'{ip_port} {lk}: agv写入料口id异常{agv_lk_id}')
        #     return
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
            # p = ThreadPool()
            # if sl_task != 0:
            #     p.apply_async(common_exec_dock, args=(server, ip_port, namespace, device, lk, sl_task, 0, new_capacity))
            # if xl_task != 0:
            #     p.apply_async(common_exec_dock, args=(server, ip_port, namespace, device, lk, 0, xl_task, new_capacity))
            # p.close()
            # p.join()
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


def insert_data(obj, conn, cursor, ip_port, lk, o_type, process_id, equip_id, material_type, material_out_time, Qtime, in_order_id, update_time):
    is_success, result = True, {}
    try:
        cursor.execute('BEGIN')
        if o_type == 1:
            # 查询有没有空位
            res = obj.run(f'select id from stock_inventory where deposit = 0')
            if not res:
                error_logger.error(f'{ip_port} {lk}堆栈入库操作异常-没有库位可供入库！')
                is_success = False
            else:
                s_id = res[0]['id']
                obj.update(f"""update stock_inventory set process_id='{process_id}', equip_id='{equip_id}', material_type='{material_type}', material_out_time='{material_out_time}', Qtime='{Qtime}', in_order_id='{in_order_id}', deposit=10, is_full=1, update_time='{update_time}' where id='{s_id}'""")
        else:
            res = obj.run(f"""select id from stock_inventory where material_type='{material_type}' and deposit=10 and Qtime / 60 > (material_out_time + storge_time)""")
            if not res:
                error_logger.error(f'{ip_port} {lk}堆栈出库操作异常-没有库存可供出库！')
                is_success = False
            else:
                s_id = res[0]['id']
                obj.update(f"""update stock_inventory set process_id=Null, equip_id=Null, material_type=Null, material_out_time=Null, Qtime=Null, in_order_id=Null, deposit=0, is_full=Null, update_time=Null where id='{s_id}'""")
                result[ip_port] = s_id
        conn.commit()
    except Exception as e:
        error_logger.error(f'{ip_port} {lk}堆栈库操作异常-{traceback.format_exc()}')
        conn.rollback()
    return is_success, result


def other_exec_dock(server, ip_port, namespace, device, lk, task_type):
    try:
        obj = SQLiteConnector(f'databases/{device}.sqlite3')
        success, conn, cursor = False, obj.conn, obj.cursor
        prepare_material = server.get_node(f'ns={namespace};s=prepare_material').get_value()
        prepare_order_id = server.get_node(f'ns={namespace};s=prepare_order_id').get_value()
        prepare_type = server.get_node(f'ns={namespace};s=prepare_type').get_value()
        prepare_equip_id = server.get_node(f'ns={namespace};s=prepare_equip_id').get_value()
        if task_type == 1:
            error_logger.info(f'{ip_port} {device} {lk}: agv堆栈送料[put]-开始上料对接++++++++++++++++++')
            # 检查是否与备料信息一致
            material_type = server.get_node(f"ns={namespace};s=GZ_AGVPutMaterialType").get_value()
            in_order_id = server.get_node(f"ns={namespace};s=GZ_AGVPutOrderID").get_value()
            now_time = datetime.now()
            # 轮询变量
            sl_var_list = [f'GZ_AGVPutActionArrived', f'GZ_AGVPutActionFinish', f'GZ_AGVPutActionRquestExit']
            sl_finished = server.get_node(f"ns={namespace};s=sl_finished").get_value()
            for var in sl_var_list:
                signal = server.get_node(f"ns={namespace};s={var}").get_value()
                if signal == 1:
                    if var.endswith('Arrived'):
                        error_logger.info(f'{ip_port} {device} {lk}: agv堆栈送料[put]-检测到agv到达信号 {var}-{signal}++++++++++++++++++')
                        if material_type != prepare_material:
                            error_logger.info(f'{ip_port} {device} {lk}: agv堆栈送料[put]-检测到agv到达信号 请求入栈物料[{material_type}]与备料物料[{prepare_material}]不完全一致++++++++++++++++++')
                            return
                        server.get_node(f"ns={namespace};s=GZ_StackLoadAllowed").set_value(1)
                        server.get_node(f"ns={namespace};s=GZ_StackLoading").set_value(1)
                    elif var.endswith('Finish'):
                        error_logger.info(f'{ip_port} {device} {lk}: agv堆栈送料[put]-检测到agv送料完成信号 {var}-{signal}++++++++++++++++++')
                        check_result = check_dock_heart(server, ip_port, device, namespace, lk, task_type='上料')
                        if not check_result:
                            error_logger.error(f'{ip_port} {device} {lk}: agv堆栈送料[put]-心跳检测失败++++++++++++++++++')
                            return
                        server.get_node(f"ns={namespace};s=GZ_StackLoadFinish").set_value(1)
                        server.get_node(f"ns={namespace};s=GZ_StackLoadAllowed").set_value(0)
                        server.get_node(f"ns={namespace};s=GZ_StackLoading").set_value(0)
                        if sl_finished == 0:
                            process_id = server.get_node(f"ns={namespace};s=GZ_AGVPutProcessSegmentID").get_value()
                            material_type = server.get_node(f"ns={namespace};s=GZ_AGVPutMaterialType").get_value()
                            equip_id = server.get_node(f"ns={namespace};s=GZ_AGVPutEquitmentID").get_value()
                            material_out_time = server.get_node(f"ns={namespace};s=GZ_AGVPutMaterialProductTime").get_value()
                            Qtime = server.get_node(f"ns={namespace};s=GZ_AGVPutMaterialQTime").get_value()
                            in_order_id = server.get_node(f"ns={namespace};s=GZ_AGVPutOrderID").get_value()
                            error_logger.info(f'{ip_port} {device} {lk}: agv堆栈送料[put]-待入库物料: process_id:{process_id} material_type:{material_type} equip_id:{equip_id} material_out_time:{material_out_time} Qtime:{Qtime} in_order_id:{in_order_id}++++++++++++++++++')

                            update_time = str(now_time - timedelta(minutes=material_out_time))
                            # 写入数据库
                            res, result = insert_data(obj=obj, conn=conn, cursor=cursor, ip_port=ip_port, lk=lk, o_type=task_type, process_id=process_id,
                                                      equip_id=equip_id, material_type=material_type, material_out_time=material_out_time, Qtime=Qtime,
                                                      in_order_id=in_order_id, update_time=update_time)
                            if res:
                                server.get_node(f"ns={namespace};s=sl_finished").set_value(0)
                                success = True
                                server.get_node(f"ns={namespace};s=sl_finished").set_value(1)
                                server.get_node(f'ns={namespace};s=prepare_material').set_value(0)
                                server.get_node(f'ns={namespace};s=prepare_order_id').set_value(0)
                                server.get_node(f'ns={namespace};s=prepare_type').set_value(0)
                                server.get_node(f'ns={namespace};s=GZ_StackRequestTask').set_value(0)
                                error_logger.info(f'{ip_port} {device} {lk}: agv堆栈送料[put]-送料完成++++++++++++++++++')
                            else:
                                error_logger.error(f'{ip_port} {device} {lk}: agv堆栈送料[put]-写入数据库失败{res}, {result}++++++++++++++++++')
                    elif var.endswith('Exit'):
                        error_logger.info(f'{ip_port} {device} {lk}: agv堆栈送料[put]-检测到agv请求离开信号 {var}-{signal}++++++++++++++++++')
                        server.get_node(f"ns={namespace};s=GZ_StackLoadExitAllowed").set_value(1)
                        server.get_node(f"ns={namespace};s=GZ_StackLoadFinish").set_value(0)
                        time.sleep(10)
                        sl_finished = server.get_node(f"ns={namespace};s=sl_finished").set_value(0)
                    else:
                        error_logger.info(f'{ip_port} {device} {lk}: agv堆栈送料[put]-检测到agv其他信号{var}-{signal}------------------')
                else:
                    if var.endswith('Exit'):
                        server.get_node(f"ns={namespace};s=GZ_StackLoadExitAllowed").set_value(0)
        else:
            error_logger.info(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-开始下料对接------------------')
            # 检查是否与备料信息一致
            out_process_id = server.get_node(f"ns={namespace};s=GZ_AGVFetchProcessSegmentID").get_value()
            out_material_type = server.get_node(f"ns={namespace};s=GZ_AGVFetchMaterialType").get_value()
            on_order_id = server.get_node(f"ns={namespace};s=GZ_AGVFetchOrderID").get_value()
            error_logger.info(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-agv请求出料 物料规格: {out_material_type}, 工序: {out_process_id}, 订单id: {on_order_id}------------------')
            res = obj.run(f"""select * from stock_inventory where material_type='{out_material_type}' and deposit=10 and Qtime / 60 > (material_out_time + storge_time)""")
            if not res:
                error_logger.warning(f'{ip_port} {device} {lk}:没有库存可供出库')
                sql_data = {'equip_id': 0, 'material_out_time': 0, 'Qtime': 0, 'storge_time': 0}
            else:
                sql_data = res[0]
            # 设置出库信息
            server.get_node(f"ns={namespace};s=GZ_StackUnloadEquitmentID").set_value(sql_data['equip_id'])
            server.get_node(f"ns={namespace};s=GZ_StackMaterialProductTime").set_value(sql_data['material_out_time'])
            server.get_node(f"ns={namespace};s=GZ_StackUnloadMaterialQTime").set_value(sql_data['Qtime'])
            server.get_node(f"ns={namespace};s=GZ_StackUnloadStorageTime").set_value(sql_data['storge_time'])
            # 轮询变量
            xl_var_list = [f"GZ_AGVFetchActionArrived", f'GZ_AGVFetchActionBegin', f'GZ_AGVFetchActionFinish', f'GZ_AGVFetchActionRquestExit']
            xl_finished = server.get_node(f"ns={namespace};s=xl_finished").get_value()
            for var in xl_var_list:
                signal = server.get_node(f"ns={namespace};s={var}").get_value()
                if signal == 1:
                    if var.endswith('Arrived'):
                        error_logger.info(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-检测到agv到达信号 {var}-{signal}------------------')
                        if out_material_type != prepare_material:
                            error_logger.info(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-检测到agv到达信号 请求出栈物料[{out_material_type}]与备料物料[{prepare_material}]不完全一致++++++++++++++++++')
                            return
                        server.get_node(f"ns={namespace};s=GZ_StackUnloadAllowed").set_value(1)
                    elif var.endswith('Begin'):
                        error_logger.info(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-检测到传篮开始信号 {var}-{signal}------------------')
                        server.get_node(f"ns={namespace};s=GZ_StackUnloading").set_value(1)
                        server.get_node(f"ns={namespace};s=GZ_StackUnloadAllowed").set_value(0)
                    elif var.endswith('Finish'):
                        error_logger.info(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-检测到agv取货完成信号 {var}-{signal}------------------')
                        check_result = check_dock_heart(server, ip_port, device, namespace, lk, task_type='下料')
                        if not check_result:
                            error_logger.error(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-心跳检测失败------------------')
                            return
                        server.get_node(f"ns={namespace};s=GZ_StackUnloadFinish").set_value(1)
                        server.get_node(f"ns={namespace};s=GZ_StackUnloading").set_value(0)
                        if xl_finished == 0:
                            # 写入数据库
                            res, result = insert_data(obj=obj, conn=conn, cursor=cursor, ip_port=ip_port, lk=lk, o_type=task_type, process_id=None,
                                                      equip_id=None, material_type=out_material_type, material_out_time=None, Qtime=None, in_order_id=None, update_time=None)
                            if res:
                                server.get_node(f"ns={namespace};s=xl_finished").set_value(0)
                                # 清理出库信息
                                server.get_node(f"ns={namespace};s=GZ_StackUnloadEquitmentID").set_value(0)
                                server.get_node(f"ns={namespace};s=GZ_StackMaterialProductTime").set_value(0)
                                server.get_node(f"ns={namespace};s=GZ_StackUnloadMaterialQTime").set_value(0)
                                server.get_node(f"ns={namespace};s=GZ_StackUnloadStorageTime").set_value(0)
                                success = True
                                server.get_node(f"ns={namespace};s=xl_finished").set_value(1)
                                server.get_node(f'ns={namespace};s=prepare_material').set_value(0)
                                server.get_node(f'ns={namespace};s=prepare_order_id').set_value(0)
                                server.get_node(f'ns={namespace};s=prepare_type').set_value(0)
                                server.get_node(f'ns={namespace};s=prepare_equip_id').set_value(0)
                                server.get_node(f'ns={namespace};s=prepare_material_out_time').set_value(0)
                                server.get_node(f'ns={namespace};s=prepare_q_time').set_value(0)
                                server.get_node(f'ns={namespace};s=GZ_StackRequestTask').set_value(0)
                                error_logger.info(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-取料完成------------------')
                            else:
                                error_logger.info(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-取料失败{res} {result}------------------')
                    elif var.endswith('Exit'):
                        error_logger.info(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-检测到agv请求离开信号 {var}-{signal}------------------')
                        server.get_node(f"ns={namespace};s=GZ_StackUnloadFinish").set_value(0)
                        server.get_node(f"ns={namespace};s=GZ_StackUnloadExitAllowed").set_value(1)
                        time.sleep(10)
                        server.get_node(f"ns={namespace};s=xl_finished").set_value(0)
                    else:
                        error_logger.info(f'{ip_port} {device} {lk}: agv堆栈取料[fetch]-检测到agv其他信号{var}-{signal}------------------')
                else:
                    if var.endswith('Exit'):
                        server.get_node(f"ns={namespace};s=GZ_StackUnloadExitAllowed").set_value(0)
        cursor.close()
        conn.close()
    except Exception as e:
        error_logger.error(f'{ip_port} {lk}: agv堆栈取料[fetch]-出现异常：{traceback.format_exc()}')
        raise e


def other_device_dock(server, ip_port, device, namespace, lk):
    """
    设备对接任务
    :break:
    """
    try:
        # 上下料屏蔽
        lk_disabled = server.get_node(f"ns={namespace};s=GZ_StackDisable").get_value()
        if lk_disabled == 1:
            error_logger.warning(f'{ip_port} {device} {lk}: 设备已被屏蔽')
            return
        # 是否请求对接
        lk_id = server.get_node(f"ns={namespace};s=GZ_StackID").get_value()
        agv_lk_id = server.get_node(f"ns={namespace};s=GZ_AGVStackID").get_value()
        if lk_id != agv_lk_id:
            error_logger.warning(f'{ip_port} {device} {lk}: 请求对接料口id异常{agv_lk_id}')
            return
        # 判断是入库还是出库对接
        t_pool = ThreadPool()
        for i in [1, 2]:
            t_pool.apply_async(other_exec_dock, args=(server, ip_port, namespace, device, lk, i))
        t_pool.close()
        t_pool.join()
    except Exception as e:
        error_logger.error(f'{ip_port} {device} {lk}: 堆栈对接异常{e}')


def prepare_stock(server, ip_port, device, namespace, lk):
    """
    堆栈备料
    """
    try:
        stack_id = server.get_node(f'ns={namespace};s=GZ_StackID').get_value()
        agv_stack_id = server.get_node(f'ns={namespace};s=GZ_AGVStackID').get_value()
        opera_type = server.get_node(f'ns={namespace};s=GZ_MCSRequestStack').get_value()  # 出入库请求指令 0-默认 1-请求入栈 2-请求出栈 3-取消入栈 4-取消出栈
        opera_map = {0: '默认', 1: '请求入栈', 2: '请求出栈', 3: '取消入栈', 4: '取消出栈'}
        prepare_type = server.get_node(f'ns={namespace};s=prepare_type').get_value()
        prepare_map = {0: '默认', 1: '请求入栈', 2: '请求出栈'}
        if prepare_type == 0 and opera_type == 0:
            return
        if opera_type in [1, 2] and prepare_type != 0:
            prepare_material = server.get_node(f'ns={namespace};s=prepare_material').get_value()
            prepare_order_id = server.get_node(f'ns={namespace};s=prepare_order_id').get_value()
            server.get_node(f'ns={namespace};s=GZ_MCSRequestStackAllowed').set_value(0)
            server.get_node(f'ns={namespace};s=GZ_MCSRequestStackNotAllowed').set_value(1)  # 不允许操作
            prepare_logger.info(f"{ip_port} {device} {lk}: 堆栈[{stack_id}] 已经备料, 拒绝处理申请; 备料类型: {prepare_map.get(prepare_type, '未知备料类型')}, "
                                f"备料规格: {prepare_material}, 备料订单id: {prepare_order_id}")
            return
        material_type = server.get_node(f'ns={namespace};s=GZ_MCSRequestStackMaterialType').get_value()  # 物料规格
        order_in = server.get_node(f'ns={namespace};s=GZ_AGVPutOrderID').get_value()  # 入库订单id
        order_out = server.get_node(f'ns={namespace};s=GZ_AGVFetchOrderID').get_value()  # 出库订单id
        layer1 = server.get_node(f'ns={namespace};s=GZ_AGVPutActionPort1').get_value()  # 1层
        layer2 = server.get_node(f'ns={namespace};s=GZ_AGVPutActionPort2').get_value()  # 2层
        prepare_logger.info(f"{ip_port} {device} {lk}: 当前堆栈[{stack_id}] 操作指令: {opera_type}-{opera_map.get(opera_type, '未知操作类型')}, 物料规格: {material_type}, "
                            f"入库订单id: {order_in}, 出库订单id: {order_out}, 上层: {layer1}, 下层: {layer2}, "
                            f"已经备料: {prepare_type}-{prepare_map.get(prepare_type, '未知备料类型')}")
        obj = SQLiteConnector(f'databases/{device}.sqlite3')
        if opera_type == 1:  # 请求入栈
            # 是否存在空库位
            res = obj.run(f"""select id from stock_inventory where deposit = 0""")
            if not res:
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackAllowed').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackNotAllowed').set_value(1)  # 不允许操作
                prepare_logger.warning(f'{ip_port} {device} {lk}: 堆栈[{stack_id}] 没有足够的库位来入库{material_type}')
            else:
                # 设置备料信息
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackID').set_value(2)  # 接驳口
                server.get_node(f'ns={namespace};s=prepare_material').set_value(material_type)
                server.get_node(f'ns={namespace};s=prepare_order_id').set_value(order_in)
                server.get_node(f'ns={namespace};s=prepare_type').set_value(1)
                server.get_node(f'ns={namespace};s=GZ_StackRequestTask').set_value(1)
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackAllowed').set_value(1)  # 允许操作
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackNotAllowed').set_value(0)
                prepare_logger.warning(f'{ip_port} {device} {lk}: 堆栈[{stack_id}] 处理入栈备料请求成功; 物料规格: {material_type}, 订单id: {order_in}')
        elif opera_type == 2:  # 请求出栈
            # 物料是否有库存
            res = obj.run(f"""select * from stock_inventory where material_type='{material_type}' and deposit=10""")
            if not res:
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackAllowed').set_value(0)  # 允许操作
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackNotAllowed').set_value(1)  # 不允许操作
                prepare_logger.warning(f'{ip_port} {device} {lk}: 堆栈[{stack_id}] 没有足够的物料{material_type}可供出库')
            else:
                # 获取库存信息
                sql_data = res[0]
                equip_id, material_out_time, Qtime = sql_data['equip_id'], sql_data['material_out_time'], sql_data['Qtime']
                # 设置备料信息
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackID').set_value(1)  # 接驳口
                server.get_node(f'ns={namespace};s=prepare_material').set_value(material_type)
                server.get_node(f'ns={namespace};s=prepare_order_id').set_value(order_out)
                server.get_node(f'ns={namespace};s=prepare_type').set_value(2)
                server.get_node(f'ns={namespace};s=prepare_equip_id').set_value(equip_id)
                server.get_node(f'ns={namespace};s=prepare_material_out_time').set_value(material_out_time)
                server.get_node(f'ns={namespace};s=prepare_q_time').set_value(Qtime)
                server.get_node(f'ns={namespace};s=GZ_StackUnloadEquitmentID').set_value(equip_id)  # 出料机台
                server.get_node(f'ns={namespace};s=GZ_StackMaterialProductTime').set_value(material_out_time)  # 下料时间
                server.get_node(f'ns={namespace};s=GZ_StackUnloadMaterialQTime').set_value(Qtime)  # QTime
                server.get_node(f'ns={namespace};s=GZ_StackRequestTask').set_value(1)
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackAllowed').set_value(1)  # 允许操作
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackNotAllowed').set_value(0)  # 不允许操作
                prepare_logger.warning(f'{ip_port} {device} {lk}: 堆栈[{stack_id}] 处理出栈备料请求成功; 物料规格: {material_type}, 订单id: {order_out},'
                                       f'出料机台: {equip_id}, 下料时间: {material_out_time}, QTime: {Qtime}')
        elif opera_type == 3:  # 取消入栈
            # 是否正在对接
            if agv_stack_id != 0:
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackAllowed').set_value(0)  # 允许操作
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackNotAllowed').set_value(1)  # 不允许操作
                prepare_logger.warning(f'{ip_port} {device} {lk}: 堆栈[{stack_id}] 正在入栈对接中，无法撤销')
            else:
                # 取消料与备料不一致不能取消
                # prepare_material = server.get_node(f'ns={namespace};s=prepare_material').get_value()
                # if prepare_material != material_type:
                #     server.get_node(f'ns={namespace};s=GZ_MCSRequestStackNotAllowed').set_value(1)  # 不允许操作
                #     prepare_logger.warning(f'{ip_port} {device} {lk}: 堆栈[{stack_id}] 申请取消入栈物料[{material_type}]与备料[{prepare_material}]不一致，无法撤销')
                #     return
                # 复位备料信息
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackID').set_value(2)  # 接驳口
                server.get_node(f'ns={namespace};s=prepare_material').set_value(0)
                server.get_node(f'ns={namespace};s=prepare_order_id').set_value(0)
                server.get_node(f'ns={namespace};s=prepare_type').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_StackRequestTask').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackAllowed').set_value(1)  # 允许操作
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackNotAllowed').set_value(0)  # 不允许操作
                prepare_logger.warning(f'{ip_port} {device} {lk}: 堆栈[{stack_id}] 取消入栈备料请求成功; 物料规格: {material_type}, 入库订单id: {order_in}, 出库订单id: {order_out}')
        elif opera_type == 4:  # 取消出栈
            # 是否正在对接
            if agv_stack_id != 0:
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackAllowed').set_value(0)  # 允许操作
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackNotAllowed').set_value(1)  # 不允许操作
                prepare_logger.warning(f'{ip_port} {device} {lk}: 堆栈[{stack_id}] 正在出栈对接中，无法撤销')
            else:
                # 取消料与备料不一致不能取消
                # prepare_material = server.get_node(f'ns={namespace};s=prepare_material').get_value()
                # if prepare_material != material_type:
                #     server.get_node(f'ns={namespace};s=GZ_MCSRequestStackNotAllowed').set_value(1)  # 不允许操作
                #     prepare_logger.warning(f'{ip_port} {device} {lk}: 堆栈[{stack_id}] 申请取消出栈物料[{material_type}]与备料[{prepare_material}]不一致，无法撤销')
                #     return
                # 复位备料信息
                equip_id = server.get_node(f'ns={namespace};s=prepare_equip_id').get_value()
                material_out_time = server.get_node(f'ns={namespace};s=prepare_material_out_time').get_value()
                Qtime = server.get_node(f'ns={namespace};s=prepare_q_time').get_value()
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackID').set_value(1)  # 接驳口
                server.get_node(f'ns={namespace};s=prepare_material').set_value(0)
                server.get_node(f'ns={namespace};s=prepare_order_id').set_value(0)
                server.get_node(f'ns={namespace};s=prepare_type').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_StackUnloadEquitmentID').set_value(equip_id)  # 出料机台
                server.get_node(f'ns={namespace};s=GZ_StackMaterialProductTime').set_value(material_out_time)  # 下料时间
                server.get_node(f'ns={namespace};s=GZ_StackUnloadMaterialQTime').set_value(Qtime)  # QTime
                server.get_node(f'ns={namespace};s=GZ_StackRequestTask').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackAllowed').set_value(1)  # 允许操作
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackNotAllowed').set_value(0)  # 不允许操作
                prepare_logger.warning(f'{ip_port} {device} {lk}: 堆栈[{stack_id}] 取消出栈备料请求成功; 物料规格: {material_type}, 入库订单id: {order_in}, '
                                       f'出库订单id: {order_out}, 出料机台: {equip_id}, 下料时间: {material_out_time}, QTime: {Qtime}')
        else:  # 复位
            if prepare_type == 1:  # 复位入栈点位
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackID').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackAllowed').set_value(0)  # 允许操作
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackNotAllowed').set_value(0)  # 不允许操作
                prepare_logger.info(
                    f'{ip_port} {device} {lk}: 堆栈[{stack_id}] [in]复位入栈点位成功-接驳口[GZ_MCSRequestStackID]、允许[GZ_MCSRequestStackAllowed]、不允许[GZ_MCSRequestStackNotAllowed]')
            elif prepare_type == 2:  # 复位出栈点位
                server.get_node(f'ns={namespace};s=GZ_StackUnloadEquitmentID').set_value(0)  # 出料机台
                server.get_node(f'ns={namespace};s=GZ_StackMaterialProductTime').set_value(0)  # 下料时间
                server.get_node(f'ns={namespace};s=GZ_StackUnloadMaterialQTime').set_value(0)  # QTime
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackID').set_value(0)
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackAllowed').set_value(0)  # 允许操作
                server.get_node(f'ns={namespace};s=GZ_MCSRequestStackNotAllowed').set_value(0)  # 不允许操作
                prepare_logger.info(
                    f'{ip_port} {device} {lk}: 堆栈[{stack_id}] [out]复位出栈点位成功-接驳口[GZ_MCSRequestStackID]、允许[GZ_MCSRequestStackAllowed]、不允许[GZ_MCSRequestStackNotAllowed]、'
                    f'出料机台[GZ_StackUnloadEquitmentID]、下料时间[GZ_StackMaterialProductTime]、QTime[GZ_StackUnloadMaterialQTime]')
            else:
                pass
                # prepare_logger.info(f'{ip_port} {device} {lk}: 堆栈[{stack_id}] 无需复位备料点位')
    except Exception as e:
        prepare_logger.info(f'{ip_port} {device} {lk}: 备料任务出现异常: {traceback.format_exc()}')
