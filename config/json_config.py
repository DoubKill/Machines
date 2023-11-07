import os
import json


class JsonConf(object):
    """json配置文件类"""

    @staticmethod
    def load(file_path):
        data = {}
        if os.path.exists(file_path):
            with open(file_path, encoding='utf-8') as json_file:
                try:
                    data = json.load(json_file)
                except Exception as e:
                    print(e)
        return data

    @staticmethod
    def set(data_dict, save_file_name):
        with open(save_file_name, 'w+') as json_file:
            json.dump(data_dict, json_file, indent=4)
            json_file.truncate()
