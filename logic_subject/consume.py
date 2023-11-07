from datetime import datetime
from config.log_config import api_logger
from config.derorators import api_recorder
from databases.excute_db import SQLiteConnector


@api_recorder
def device_consume_job(server, ip_port, namespace, chute_code, chute_list):
    """
    模拟设备消耗
    :param server:
    :param ip_port:
    :param namespace:
    :param chute_code:
    :param chute_list:
    :return:
    """
    # 心跳断链等异常停止消耗花篮
    agv_fault = server.get_node(f"ns={namespace};s=GZ_AGVFaultStop").get_value()
    fault_stop = server.get_node(f"ns={namespace};s=GZ_EquipmentFaultStop").get_value()
    if fault_stop == 1 or agv_fault == 1:
        api_logger.error(f'{ip_port} {chute_code}: 心跳断链[equip_fault:{fault_stop}, agv_fault: {agv_fault}]停止消耗花篮-------------')
        return
    now_time = datetime.now().replace(microsecond=0)
    now_time_str = now_time.strftime('%Y-%m-%d %H:%M:%S')
    update_info = []
    # 获取节拍
    trains_pitch_time = server.get_node(f"ns={namespace};s=pitch_time").get_value()
    beat = round(trains_pitch_time / 10)
    total_num = server.get_node(f"ns={namespace};s=GZ_TotaolProductionCapcity").get_value()
    agv_equip_id = server.get_node(f"ns={namespace};s=GZ_AGVEquipmentID").get_value()
    chute_list = set(server.get_node(f"ns={namespace};s=chute_list").get_value())
    old_total_num = total_num
    if chute_list == {2}:  # 只下料zrx
        api_logger.info(f'{ip_port} {chute_code}: 开始检测是否达到下料节拍------------------')
        xl_num = server.get_node(f"ns={namespace};s=GZ_EquipmentUnloadCasNum").get_value()
        xl_time = datetime.strptime(server.get_node(f"ns={namespace};s=xl_time").get_value(), '%Y-%m-%d %H:%M:%S')
        patch_time = (now_time - xl_time).seconds
        is_patch = True if patch_time >= beat else False  # 达到节拍判断上料场景
        api_logger.info(f'{chute_code}: 当前下料机台信息 xl_num: {xl_num}, xl_time: {xl_time}, is_patch: {is_patch}')
        if is_patch and xl_num < 10:  # 到节拍
            total_num = server.get_node(f"ns={namespace};s=GZ_TotaolProductionCapcity").get_value()
            update_info += [
                {'node_name': f"ns={namespace};s=xl_time", 'value': now_time_str},
                {'node_name': f"ns={namespace};s=GZ_EquipmentUnloadCasNum", 'value': xl_num + 1},
            ]
            total_num += 1
        if xl_num >= 10 and patch_time > 2 * trains_pitch_time and agv_equip_id == 0:  # 记录断产数据
            # 查询是否存在断料记录，不存在则新建，存在则pass
            obj = SQLiteConnector(f'databases/{chute_code}.sqlite3')
            conn, cursor = obj.conn, obj.cursor
            already_record = obj.run(f"""select id from break_records where break_equip_code='{chute_code}' and break_end_time is Null""",
                                     table_name='break_records')
            if not already_record:
                try:
                    cursor.execute('BEGIN')
                    break_equip_id = server.get_node(f"ns={namespace};s=GZ_EquipmentID").get_value()
                    obj.insert('break_records', {'break_equip_id': break_equip_id, 'break_equip_code': chute_code, 'break_start_time': now_time_str})
                    conn.commit()
                except Exception as e:
                    api_logger.error(f'{ip_port} {chute_code}断料记录异常-{e}')
                    conn.rollback()
                api_logger.error(f'{ip_port} {chute_code}断料记录成功')
    else:
        api_logger.info(f'{ip_port} {chute_code}: 开始检测是否达到节拍++++++++++++++++++')
        sl = server.get_node(f"ns={namespace};s=GZ_EquipmentLoadCasNum").get_value()
        xl = server.get_node(f"ns={namespace};s=GZ_EquipmentUnloadCasNum").get_value()
        hc = server.get_node(f"ns={namespace};s=hc").get_value()
        sl_time = datetime.strptime(server.get_node(f"ns={namespace};s=sl_time").get_value(), '%Y-%m-%d %H:%M:%S')
        xl_time = datetime.strptime(server.get_node(f"ns={namespace};s=xl_time").get_value(), '%Y-%m-%d %H:%M:%S')
        patch_time = (now_time - sl_time).seconds
        is_patch = True if patch_time >= beat else False  # 达到节拍
        api_logger.info(f'{ip_port} {chute_code}: 当前机台信息 sl: {sl}, hc: {hc}, xl: {xl}, sl_time: {sl_time}, xl_time: {xl_time}, is_patch: {is_patch}')
        if hc == 0:  # 缓存位为0,补上料数和缓存数
            if sl != 0:
                update_info += [{'node_name': f"ns={namespace};s=GZ_EquipmentLoadCasNum", 'value': sl - 2 if sl >= 2 else 0},
                                {'node_name': f"ns={namespace};s=hc", 'value': 2 if sl >= 2 else 1}]
            else:  # 记录断料时间(上下一体hc默认时2，下料满10不会变化，只上料的走入这里[zrs...])
                if not agv_equip_id:
                    # 查询是否存在断料记录，不存在则新建，存在则pass
                    obj = SQLiteConnector(f'databases/{chute_code}.sqlite3')
                    conn, cursor = obj.conn, obj.cursor
                    already_record = obj.run(f"""select id from break_records where break_equip_code='{chute_code}' and break_end_time is Null""",
                                             table_name='break_records')
                    if not already_record:
                        try:
                            cursor.execute('BEGIN')
                            break_equip_id = server.get_node(f"ns={namespace};s=GZ_EquipmentID").get_value()
                            obj.insert('break_records', {'break_equip_id': break_equip_id, 'break_equip_code': chute_code, 'break_start_time': now_time_str})
                            conn.commit()
                        except Exception as e:
                            api_logger.error(f'{ip_port} {chute_code}断料记录异常-{e}')
                            conn.rollback()
                        api_logger.error(f'{ip_port} {chute_code}断料记录成功')
        else:  # 缓存位不为0
            if is_patch:
                if xl == 10:
                    api_logger.info(f"{ip_port} {chute_code} 达到节拍但下料已满10个")
                    # # 上下一体重置上料数是0
                    # if chute_list == {1, 2}:
                    #     server.get_node(f"ns={namespace};s=GZ_EquipmentLoadCasNum").set_value(0)
                    #     api_logger.info(f"{ip_port} {chute_code} 达到节拍但下料已满10个---重置上料数为0(预防finish恢复后重复发)")
                    return
                if sl > 2:
                    sl_num = sl - 2 if hc == 1 else sl
                    hc_num = 3 - hc
                else:  # sl = 0 or sl = 1 or sl = 2
                    if sl == 2:
                        sl_num = sl if hc == 2 else 0
                        hc_num = hc - 1 if hc == 2 else 2
                    elif sl == 1:
                        sl_num = sl if hc == 2 else 0
                        hc_num = hc - 1 if hc == 2 else 1
                    else:
                        sl_num = 0
                        hc_num = hc - 1
                # 节拍结束后的上下一体 上料数为0且没有对接agv认为断产
                if chute_list == {1, 2} and sl_num == 0 and patch_time > 2 * trains_pitch_time and not agv_equip_id:
                    # 查询是否存在断料记录，不存在则新建，存在则pass
                    obj = SQLiteConnector(f'databases/{chute_code}.sqlite3')
                    conn, cursor = obj.conn, obj.cursor
                    already_record = obj.run(f"""select id from break_records where break_equip_code='{chute_code}' and break_end_time is Null""",
                                             table_name='break_records')
                    if not already_record:
                        try:
                            cursor.execute('BEGIN')
                            break_equip_id = server.get_node(f"ns={namespace};s=GZ_EquipmentID").get_value()
                            obj.insert('break_records',
                                       {'break_equip_id': break_equip_id, 'break_equip_code': chute_code, 'break_start_time': now_time_str})
                            conn.commit()
                        except Exception as e:
                            api_logger.error(f'{ip_port} {chute_code}断料记录异常-{e}')
                            conn.rollback()
                        api_logger.error(f'{ip_port} {chute_code}断料记录成功')
                update_info += [
                    {'node_name': f"ns={namespace};s=GZ_EquipmentLoadCasNum", 'value': sl_num},
                    {'node_name': f"ns={namespace};s=hc", 'value': hc_num},
                    {'node_name': f"ns={namespace};s=sl_time", 'value': now_time_str}
                ]
                if chute_list == {1, 2}:
                    update_info += [
                        {'node_name': f"ns={namespace};s=GZ_EquipmentUnloadCasNum", 'value': xl + 1},
                        {'node_name': f"ns={namespace};s=xl_time", 'value': now_time_str},
                    ]
                total_num += 1
    if total_num != old_total_num:
        update_info.append({'node_name': f"ns={namespace};s=GZ_TotaolProductionCapcity", 'value': total_num})
    # 更新
    if update_info:
        api_logger.info(f"{ip_port} 开始执行{chute_code}更新操作,操作数据: {update_info}")
        for i in update_info:
            node_name, value = i['node_name'], i['value']
            server.get_node(node_name).set_value(value)

