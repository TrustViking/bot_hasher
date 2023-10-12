#!/usr/bin/env python3
#
from typing import Coroutine, List, Optional
from time import strftime
from os.path import basename, join, isfile, abspath, dirname
from os import listdir
import sys
from sys import platform, argv, path, version_info
from subprocess import run as sub_run
from subprocess import CalledProcessError
from asyncio import create_subprocess_shell, gather, run 
from pynvml import nvmlInit, nvmlDeviceGetCount, nvmlDeviceGetHandleByIndex, nvmlDeviceGetMemoryInfo, nvmlShutdown
from psutil import virtual_memory
#
from bot_env.bot_init import LogInitializer, BotInitializer, ConfigInitializer
from bot_env.decorators import safe_await_execute, safe_execute


class Start(ConfigInitializer):
    """Module for START"""
    countInstance=0
    #
    def __init__(self):
        Start.countInstance += 1
        self.countInstance = Start.countInstance
        self.cls_name = self.__class__.__name__
        # Logger
        self.config_path = join(dirname(abspath(__file__)), 'config.json')
        self.log_init = LogInitializer()
        self.logger = self.log_init.initialize(self.config_path)
        #
        self.folder_logfile = self.logger.folder_logfile
        self.logfile = self.logger.logfile
        self.loglevel = self.logger.loglevel
        # config
        self.config = self.read_config(self.config_path)
        #
        self.name_maker_db = self.config.get('name_maker_db')
        self.folder_db = self.config.get('folder_db')
        self.name_db = self.config.get('name_db')
        self.folder_video = self.config.get('folder_video')
        self.folder_kframes = self.config.get('folder_kframes')
        self.pause_minut_hasher = self.config.get('pause_minut_hasher')
        #
        self._print()

        #

    # выводим № объекта
    def _print(self):
        msg = (
            f"\nStarted at {strftime('%X')}\n"
            f'[{__name__}|{self.cls_name}] countInstance: [{self.countInstance}]\n'
            f'platform: [{platform}]\n'
            f'\nAttributes:\n'
            )

        attributes_to_print = [
            'folder_db',
            'name_db',
            'folder_logfile',
            'logfile',
            'loglevel',
            'folder_video',
            'folder_kframes',
            'pause_minut_hasher',
        ]

        for attr in attributes_to_print:
            # "Attribute not found" будет выведено, если атрибут не существует
            value = getattr(self, attr, "Attribute not found")  
            msg += f"{attr}: {value}\n"

        print(msg)
        self.logger.log_info(msg)


    # выводим состояние системы
    def system_status(self):
        file_start = basename(argv[0]) #  [start_hasher.py]
        print(f'\n[{__name__}|{self.cls_name}] Start...\n')  
        # Получение абсолютного пути к текущему исполняемому файлу
        file_path = abspath(__file__) #  [D:\linux\bots\bot_hasher\start_hasher.py]
        # Получение пути к директории, в которой находится текущий файл
        current_directory = dirname(file_path)
        msg = (
                f'File: [{file_start}]\n'
                f'Current_directory: [{current_directory}]\n'
                f'Path file: [{file_path}]\n'
                f'Data memory:'
                )
        print(msg)
        memory = virtual_memory()
        for field in memory._fields:
            print(f"{field}: {getattr(memory, field)}")    


    # логирование информации о памяти
    def log_memory(self):
        self.logger.log_info(f'****************************************************************')
        self.logger.log_info(f'*Data RAM {basename(argv[0])}: [{virtual_memory()[2]}%]')
        # Инициализируем NVML для сбора информации о GPU
        nvmlInit()
        deviceCount = nvmlDeviceGetCount()
        self.logger.log_info(f'\ndeviceCount [{deviceCount}]')
        for i in range(deviceCount):
            handle = nvmlDeviceGetHandleByIndex(i)
            meminfo = nvmlDeviceGetMemoryInfo(handle)
            self.logger.log_info(f"#GPU [{i}]: used memory [{int(meminfo.used / meminfo.total * 100)}%]")
            self.logger.log_info(f'****************************************************************\n')
        # Освобождаем ресурсы NVML
        nvmlShutdown()


    # написание python
    def get_python_command(self):
        system = sys.platform
        version_python = sys.version_info
        if system == 'win32':
            return 'python'
        elif system == 'linux':
            if version_python[0] == 3:
                return 'python3'
            else:
                return 'python'
        elif system == 'darwin':  # для MacOS
            if version_python[0] == 3:
                return 'python3'
            else:
                return 'python'
        else:
            # для других систем
            if version_python[0] == 3:
                return 'python3'
            else:
                return 'python'


    # List of file names that match all patterns in the name
    def get_names_files(self, patterns_inname: List[str]) -> List[str]:
        directory_path = dirname(abspath(__file__))
        nfiles = sorted(listdir(directory_path))
        return [nfile for nfile in nfiles if all(pattern in nfile for pattern in patterns_inname)]


    # Command line string for running scripts
    def cmd_string(self, name_scripts: List[str]) -> Optional[List[str]]:
        directory = dirname(abspath(__file__))
        full_paths = [join(directory, name_script) for name_script in name_scripts]
        
        notexist_paths = [full_path for full_path in full_paths if not isfile(full_path)]
        if notexist_paths:
            print(f'\n[{__name__}|{self.cls_name}] ERROR: Files do not exist at these paths: {notexist_paths}')
        
        exist_paths = [full_path for full_path in full_paths if isfile(full_path)]
        if not exist_paths:
            print(f'\n[{__name__}|{self.cls_name}] ERROR: No existing files! exist_paths: {exist_paths}')
            return None
        
        name_lang = self.get_python_command()
        return [f'{name_lang} {exist_path}' for exist_path in exist_paths]


    # Список скриптов для запуска
    def bot_scripts(self, patterns_inname: List[str])-> Optional[List[str]]:
        names_bots = self.get_names_files(patterns_inname)
        return self.cmd_string(names_bots)


    # Асинхронная функция для запуска скрипта
    async def subprocess_script(self, script: str):
        @safe_await_execute(logger=self.logger, name_method='subprocess_script')
        async def _subprocess_script(script):
            self.logger.log_info(f'[{__name__}|{self.cls_name}] start script: {script}')
            print(f'\n[{__name__}|{self.cls_name}] start script: {script}')

            process = await create_subprocess_shell(script)
            if not process:
                print(f'\n[{__name__}|{self.cls_name}] ERROR process: {process}')
                return None
            
            await process.wait()

        return await _subprocess_script(script)
    

# MAIN **************************
# Главная асинхронная функция
async def main():
    #
    # Создаем экземпляр класса Start
    start = Start()
    start.log_memory() # логирование информации о памяти
    start.system_status() # выводим состояние системы
    # создаем таблицы БД
    scripts = start.bot_scripts(['script', '.py'])
    tasks = [start.subprocess_script(script) for script in scripts]
    await gather(*tasks)
    #
    # Запускаем скрипты асинхронно
    # scripts = start.scripts_args() # Список скриптов с аргументами для запуска
    # tasks = [start.subprocess_script(script) for script in scripts]
    # await gather(*tasks)

# Запускаем главную асинхронную функцию
if __name__ == "__main__":
    run(main())





    



