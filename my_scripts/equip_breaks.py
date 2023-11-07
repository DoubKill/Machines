import os
import re
import sys
import numpy as np
import pandas as pd
from datetime import datetime
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from databases.excute_db import SQLiteConnector
from config.json_config import JsonConf

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
databases_file_path = os.path.join(BASE_DIR, 'databases')
save_path = os.path.join(BASE_DIR, 'my_scripts')


class BreakStatics(object):

    def get_files(self):
        config = JsonConf.load(os.path.join(BASE_DIR, 'config', 'config.json'))
        files = []
        for device, v in config.items():
            if device.startswith('DZ'):
                continue
            lk_list = [os.path.join(databases_file_path, f"{i['lk_code']}.sqlite3") for i in v]
            files.extend(lk_list)
        return files

    def analyze(self, start_time, end_time):
        data = {}
        files = self.get_files()
        for file_path in files:
            equip_code = re.split(r'/|.sqlite3', file_path)[-2]
            db = SQLiteConnector(file_path)
            single_data = db.run(f"""select * from break_records where break_start_time >= '{start_time}' and break_start_time <= '{end_time}'""", 'break_records')
            data[equip_code] = single_data
        return data

    def run(self, st, et):
        data = self.analyze(st, et)
        if data:
            now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f_path = os.path.join(save_path, f'断产数据_{now_time}.xlsx')
            summary_data = {}
            pd_writer = pd.ExcelWriter(f_path, engine='openpyxl')
            for key, value in data.items():
                # 断产数据
                keyword = re.split('[0|1]', key)[0]
                if keyword not in summary_data:
                    summary_data[keyword] = {"工序": keyword, "总次数": None, "断料次数": None, "断料次数占比": None, "最小断料时间(s)": None, "最大断料时间(s)": None,
                                             "总断料时长(s)": None, "平均断料时间(s)": None}
                _df = pd.DataFrame(value)
                if _df.empty:  # 添加空数据
                    h_df = pd.DataFrame({'机台名称': [], '机台编号': [], '断料开始时间': [], '断料结束时间': [], '断料时长(s)': []})
                else:
                    df_sorted = _df.sort_values(by=['break_start_time'], ascending=[True])
                    df_sorted.replace(to_replace=np.nan, value=None, inplace=True)
                    max_time = df_sorted.loc[df_sorted['break_consume'] != 0, 'break_consume'].max()
                    min_time = df_sorted.loc[df_sorted['break_consume'] != 0, 'break_consume'].min()
                    mean_time = df_sorted.loc[df_sorted['break_consume'] != 0, 'break_consume'].mean()
                    s_max = None if np.isnan(max_time) else max_time
                    s_min = None if np.isnan(min_time) else min_time
                    s_mean = None if np.isnan(mean_time) else round(mean_time, 2)
                    # 非0行的数量
                    not_zero = len(df_sorted.loc[df_sorted['break_consume'] != 0])
                    break_times = len(value)
                    ratio = round(not_zero / break_times, 2) if break_times != 0 else 0
                    df_summary = pd.DataFrame({'break_equip_code': ['断产次数', '对接次数', '断料比例', '最大断产时间', '最小断产时间', '平均断产时间'],
                                               'break_equip_id': ['', '', '', '', '', ''], 'break_start_time': ['', '', '', '', '', ''],
                                               'break_end_time': ['', '', '', '', '', ''],
                                               'break_consume': [not_zero, break_times, ratio, s_max, s_min, s_mean]})
                    s_df = pd.concat([df_sorted, df_summary], ignore_index=True, sort=False)
                    h_df = s_df.rename(columns={'break_equip_code': '机台名称', 'break_equip_id': '机台编号', 'break_start_time': '断料开始时间',
                                                'break_end_time': '断料结束时间', 'break_consume': '断料时长(s)'})[['机台名称', '机台编号', '断料开始时间', '断料结束时间', '断料时长(s)']]
                    # 汇总数据
                    s_process_data = summary_data[keyword]
                    s_process_data['总次数'] = break_times if s_process_data['总次数'] is None else (s_process_data['总次数'] + break_times)
                    s_process_data['断料次数'] = (s_process_data['断料次数'] + not_zero) if s_process_data['断料次数'] else (not_zero if not_zero else None)
                    s_process_data['断料次数占比'] = round(s_process_data['断料次数'] / s_process_data['总次数'], 2) if s_process_data['总次数'] and s_process_data['总次数'] != 0 and s_process_data['断料次数'] else 0
                    s_process_data['最小断料时间(s)'] = s_min if s_process_data['最小断料时间(s)'] is None else (min(s_process_data['最小断料时间(s)'], s_min) if s_min else s_process_data['最小断料时间(s)'])
                    s_process_data['最大断料时间(s)'] = s_max if s_process_data['最大断料时间(s)'] is None else (max(s_process_data['最大断料时间(s)'], s_max) if s_max else s_process_data['最大断料时间(s)'])
                    not_zero_times = df_sorted.loc[df_sorted['break_consume'] != 0, 'break_consume'].sum()
                    s_process_data['总断料时长(s)'] = s_process_data['总断料时长(s)'] if not_zero_times == 0 else (not_zero_times if s_process_data['总断料时长(s)'] is None else (s_process_data['总断料时长(s)'] + not_zero_times))
                    s_process_data['平均断料时间(s)'] = round(s_process_data['总断料时长(s)'] / s_process_data['断料次数'], 2) if all([s_process_data['总断料时长(s)'], s_process_data['断料次数']]) else None
                h_df.to_excel(pd_writer, sheet_name=key, index=False)
            # 写入汇总数据
            if summary_data:
                summary_df = pd.DataFrame(summary_data.values())
                summary_df.to_excel(pd_writer, sheet_name='工序断产汇总', index=False)
                # 移动到第一位
                workbook = pd_writer.book
                worksheet = workbook['工序断产汇总']
                sheets = workbook.sheetnames
                sheets.insert(0, sheets.pop(sheets.index('工序断产汇总')))
                workbook._sheets = [workbook[sheet_name] for sheet_name in sheets]
            # 设置列宽
            workbook = pd_writer.book
            # 遍历每个工作表，设置列宽
            for sheet_name in pd_writer.sheets:
                worksheet = workbook[sheet_name]
                for column_cells in worksheet.columns:
                    max_length = 0
                    # 获取第一行单元格的数据抬头长度
                    header_cell = column_cells[0]
                    max_length = max(max_length, len(str(header_cell.value)))
                    adjusted_width = max_length + 12  # 可以根据需要进行微调
                    column_letter = column_cells[0].column_letter

                    # 设置整列的宽度
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            pd_writer.close()
            print('导出断产数据成功！')
        else:
            print('检查是否存在数据库文件！')


if __name__ == '__main__':
    try:
        st = input('请输入过滤时间(格式：yy-mm-dd hh:mm:ss), 不输入则默认导出2023-01-01 00:00:00以后的数据：')
        et = input('请输入过滤时间(格式：yy-mm-dd hh:mm:ss), 不输入则默认导出2025-01-01 00:00:00之前的数据：')
        if not st:
            st = '2023-01-01 00:00:00'
        if not et:
            et = '2025-01-01 00:00:00'
        st_strip_time = datetime.strptime(st, '%Y-%m-%d %H:%M:%S')
        et_strip_time = datetime.strptime(et, '%Y-%m-%d %H:%M:%S')
        if et_strip_time < st_strip_time:
            raise Exception('输入的时间有误，请重新输入！')
        BreakStatics().run(st, et)
    except Exception as e:
        print(f'导出断产数据失败！{e}')
    else:
        print('导出断产数据成功！')
