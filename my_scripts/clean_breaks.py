import os
import sys
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from databases.excute_db import SQLiteConnector
from config.json_config import JsonConf

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
databases_file_path = os.path.join(BASE_DIR, 'databases')


class BreaksClean(object):

    def get_files(self):
        config = JsonConf.load(os.path.join(BASE_DIR, 'config', 'config.json'))
        files = []
        for device, v in config.items():
            if device.startswith('DZ'):
                continue
            lk_list = [os.path.join(databases_file_path, f"{i['lk_code']}.sqlite3") for i in v]
            files.extend(lk_list)
        return files

    def run(self, f_time):
        files = self.get_files()
        for file_path in files:
            db = SQLiteConnector(file_path)
            db.update(f"""delete from break_records where break_start_time <= '{f_time}'""")


if __name__ == '__main__':
    try:
        filter_time = input('请输入过滤时间[删除本时间之前的数据](格式：yy-mm-dd hh:mm:ss):')
        if not filter_time:
            print('时间不可为空！')
        else:
            strip_time = datetime.strptime(filter_time, '%Y-%m-%d %H:%M:%S')
            BreaksClean().run(filter_time)
    except Exception as e:
        print(f'清除断产数据失败！{e}')
    else:
        print('清除断产数据成功！')
