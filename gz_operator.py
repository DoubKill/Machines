import argparse
import multiprocessing
import os
import sys
import psutil
import subprocess
from task.snapshot import backup_data
from my_scripts.clean_dz import clean_stock

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def kill_processes():
    try:
        backup_data('config_exit_save.json')
        pid_file = os.path.join(BASE_DIR, 'pid.txt')
        # 读取进程号
        with open(pid_file, 'r') as f:
            pid = int(f.read())
        # 获取进程对象
        process = psutil.Process(pid)

        # 获取所有子进程
        children = process.children(recursive=True)

        # 杀死所有子进程
        for child in children:
            child.kill()
        process.kill()
        # 检查是否有进程残留"
        main_path = os.path.join(BASE_DIR, 'main.py')
        sub = subprocess.Popen(f"ps -ef | grep -v grep | grep 'python {main_path}' | wc -l", shell=True, stdout=subprocess.PIPE)
        output, error_msg = sub.communicate()
        result = int(output.decode('utf-8').strip())
        if result != 0:
            print("存在残留进程, 请手动清除！输入命令: ps -ef | grep -v grep | grep 'python %s' | awk '{print $2}' | xargs kill -9" % main_path)
        else:
            print(f'进程{pid} 所有{len(children)}子进程已杀死!')
    except Exception as e:
        print(f"清除进程时出现异常: {e}")
        return


def run_process(key_word):
    main_file = os.path.join(BASE_DIR, 'main.py')
    out_file = os.path.join(BASE_DIR, 'output.log')
    pid_file = os.path.join(BASE_DIR, 'pid.txt')
    # 启动进程
    if sys.platform.startswith('win'):
        with open(out_file, 'w') as output_file:
            with open('pid.txt', 'w') as pid_file:
                process = subprocess.Popen(['python', main_file, key_word], stdout=output_file, stderr=subprocess.STDOUT, shell=True)
                pid_file.write(str(process.pid))
    else:
        subprocess.Popen(f'nohup python {main_file} {key_word} > {out_file} 2>&1 & echo $! > {pid_file}', shell=True)


if __name__ == '__main__':
    multiprocessing.freeze_support()
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('-o', "--action", type=str, help='根据-o参数来决定操作')
        args, remaining_argv = parser.parse_known_args()
        keyword = args.action
        if keyword == 'kill':
            kill_processes()
        elif keyword in ['common', 'backup', 'exit_save']:
            run_process(keyword)
        else:
            print('缺少运行参数！kill(退出所有服务)、common(启动所有服务)、backup(备份数据启动服务)、exit_save(退出服务保存配置启动服务)')
    except Exception as e:
        print(f"运行出现异常: {e}")
        sys.exit(1)
