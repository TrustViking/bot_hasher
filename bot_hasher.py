#!/usr/bin/env python3 
#
from typing import Coroutine, Callable, Union, Set, Tuple, List, Optional, Any, Dict
from time import sleep, strftime
from datetime import datetime as dt
from asyncio import run 
from pynvml import nvmlInit, nvmlDeviceGetCount, nvmlDeviceGetHandleByIndex, nvmlDeviceGetMemoryInfo, nvmlShutdown
from logging import getLevelName
from os.path import basename, join, abspath, dirname, getmtime, isdir
from os import makedirs, listdir, remove, getmtime, join
import shutil
from sys import platform, argv, path
from psutil import virtual_memory
from sqlalchemy.engine.result import Row
#
from data_base.table_db import DiffTable
from bot_env.bot_init import LogInitializer, BotInitializer, ConfigInitializer
from bot_env.decorators import safe_await_execute, safe_execute
from data_base.base_db import MethodDB
from handlers.client import  HandlersBot
from videohasher.comparator import Comparison
from bot_env.mod_log import Logger
#
#
#
class HasherVid(ConfigInitializer):
    """Modul for hasher video"""
    countInstance=0
    #
    def __init__(self):
        HasherVid.countInstance += 1
        self.countInstance = HasherVid.countInstance
        self.cls_name = self.__class__.__name__
        self.abspath = dirname(abspath(__file__))
        # config
        self.config_path = join(dirname(abspath(__file__)), 'config.json')
        self.config = self.read_config(self.config_path)
        self.folder_video = self.config.get('folder_video')
        self.folder_kframes = self.config.get('folder_kframes') 
        self.pause_minut_hasher = self.config.get('pause_minut_hasher') 
        # Logger
        self.log_init = LogInitializer()
        self.logger = self.log_init.initialize(self.config_path)
        # Импорт словаря {имя таблицы : таблица}  
        self.name_table = DiffTable.tables
        # MethodDB
        self.methodDb=MethodDB(self.logger, self.config_path)
        # Comparison
        self.cmp=Comparison(self.config_path, self.logger, self.methodDb)
        #
        # создаем рабочие пути
        self.save_file_path=join(path[0], self.folder_video)
        self.path_save_keyframe=join(path[0], self.folder_kframes)
        self.create_directory([self.save_file_path, self.path_save_keyframe])
        self.days_del=2
        self.time_del = 24 * 60 * 60 * self.days_del #  
        self._print()
    #

    # выводим атрибуты объекта
    def _print(self):
        msg = (
            f"\nStarted at {strftime('%X')}\n"
            f'[{__name__}|{self.cls_name}] countInstance: [{self.countInstance}]\n'
            f'platform: [{platform}]\n'
            f'\nAttributes:\n'
            )

        attributes_to_print = [
            'cls_name',
            'abspath',
            'config_path',
            'log_init',
            'logger',
            'bot_init',
            'bot',
            'dp',
            'router',
            'name_table',
            'methodDb',
            'cmp',
            'folder_video',
            'folder_kframes',
            'pause_minut_hasher',
            'save_file_path',
            'path_save_keyframe',

        ]

        for attr in attributes_to_print:
            # "Attribute not found" будет выведено, если атрибут не существует
            value = getattr(self, attr, "Attribute not found")  
            msg += f"{attr}: {value}\n"

        print(msg)
        self.logger.log_info(msg)
#
    
    def create_directory(self, paths: list[str]):
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _create_directory():
            """
            Создает директорию, если она не существует

            Аргументы:
            - paths: список строк, каждая из которых является путем к директории, 
            которую необходимо создать.
            """
            _ = [makedirs(path,  exist_ok=True) for path in paths]
        return _create_directory()

    
    # выбираем из таблицы скачанные видео, но еще не хэшировали 
    async def rows4diff (self, 
                        name_table: str, 
                        one_column_name: str, 
                        one_params_status: str,
                        two_column_name: str,
                        two_params_status: str) -> Optional[List[Row]]:
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def  _rows4diff():
            async_results = await self.methodDb.read_data_two( 
                            name_table = name_table,  
                            one_column_name = one_column_name, 
                            one_params_status = one_params_status,
                            two_column_name = two_column_name, 
                            two_params_status = two_params_status,
                                                        )
            rows = async_results.fetchall()
            if not rows: 
                return None
            return rows
        return await _rows4diff()


    async def kwargs_rows4diff (self) -> Optional[List[Row]]:
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def  _kwargs_rows4diff():
            kwargs = {
                    'name_table' : 'diff',  
                    'one_column_name' : 'dnld', 
                    'one_params_status' : 'dnlded',
                    'two_column_name' : 'in_work', 
                    'two_params_status' : 'not_diff',
                    }
            return await self.rows4diff(**kwargs)
        return await _kwargs_rows4diff()



    def delete_old_files(self, directories: List[str], time_delete: int) -> List[str]:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _delete_old_files():
            
            deleted_files_and_dirs = []  # список для хранения путей к удаленным файлам и папкам
            
            for directory in directories:
                # множество имен файлов и папок, которые находятся на диске
                set_nfile_dir = set(listdir(directory))
                # текущее время
                current_time = dt.now().timestamp()
                
                for name_file in set_nfile_dir:
                    
                    full_path = join(directory, name_file)
                    
                    # время последнего изменения файла или папки
                    file_mod_time = getmtime(full_path)
                    
                    # если файл или папка старше time_delete
                    if current_time - file_mod_time > time_delete:
                        if isdir(full_path):
                            shutil.rmtree(full_path)
                            deleted_files_and_dirs.append(full_path)
                            print(f"\n[{__name__}|{self.cls_name}] Директория {full_path} успешно удалена.")
                            self.logger.log_info(f"[{__name__}|{self.cls_name}] Директория {full_path} успешно удалена.")
                        else:
                            remove(full_path)
                            deleted_files_and_dirs.append(full_path)
                            print(f"\n[{__name__}|{self.cls_name}] Файл {full_path} успешно удалён.") 
                            self.logger.log_info(f"[{__name__}|{self.cls_name}] Файл {full_path} успешно удалён.") 
            
            return deleted_files_and_dirs
        return _delete_old_files()


    # выводим состояние системы
    def system_status(self):
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def _system_status():
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
        return _system_status()


    # логирование информации о памяти
    def log_memory(self):
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def _log_memory():
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
        return _log_memory()

#
# MAIN **************************
async def main():
    # Создаем экземпляр класса Start
    # создаем объект и в нем регистрируем хэндлеры Клиента
    hasher=HasherVid()
    hasher.log_memory() # логирование информации о памяти
    hasher.system_status() # выводим состояние системы
    minut = hasher.pause_minut_hasher
    while True:
        print(f'\nБот по хэшированию видео ждет {minut} минут(ы) ...')
        sleep (int(60*minut))
        print(f'\nСодержание таблиц в БД...')
        await hasher.methodDb.print_tables(['diff'])
        
        # удаляем все файлы, которые старше...
        delete_files = hasher.delete_old_files([hasher.save_file_path, hasher.path_save_keyframe], hasher.time_del)
        if delete_files:
            for delete_file in delete_files:
                print(f'\n[main HasherVid] delete_file: {delete_file}')

        # формируем список сравнений из таблицы
        rows = await hasher.kwargs_rows4diff()
        if not rows: 
            continue
        ### точка входа сравнения файлов 
        ### надо будет добавить параллельное выполнение
        for row in rows:
            path2file = await hasher.cmp.files_comparison(row)
            if not path2file:
                print(f'\n[main HasherVid] Ooooooooppppppssssss...')


if __name__ == "__main__":
    run(main())


