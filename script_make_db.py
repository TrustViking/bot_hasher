#!/usr/bin/env python3
#
from time import strftime
from os.path import basename, join, abspath, dirname, isfile
from os import makedirs
from sys import platform, argv, path
from asyncio import run 
from pynvml import nvmlInit, nvmlDeviceGetCount, nvmlDeviceGetHandleByIndex, nvmlDeviceGetMemoryInfo, nvmlShutdown
from psutil import virtual_memory
from sqlalchemy.ext.asyncio import create_async_engine, AsyncAttrs, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import MetaData, Table, Column, Integer, String, Float
#
from data_base.table_db import DiffTable
from bot_env.bot_init import LogInitializer, BotInitializer, ConfigInitializer
from bot_env.decorators import safe_await_alchemy_exe
#
#
class Make_db(AsyncAttrs, DeclarativeBase, ConfigInitializer):
    """Module for making data base"""
    countInstance=0
    #
    def __init__(self):
        Make_db.countInstance += 1
        self.countInstance = Make_db.countInstance
        self.cls_name = self.__class__.__name__
        self.os_abspath = dirname(abspath(__file__))
        self.sys_abspath = path[0]
        # self.abspath = dirname(abspath(__file__))

        # Logger
        self.config_path = join(dirname(abspath(__file__)), 'config.json')
        self.log_init = LogInitializer()
        self.logger = self.log_init.initialize(self.config_path)
        
        # config
        self.config = self.read_config(self.config_path)
        
        # Определение структуры таблицы 
        self.metadata = DiffTable.metadata
        
        # Импорт словаря {имя таблицы : таблица}  
        self.name_table = DiffTable.tables
        
        # директория для БД
        self.path_db = join(self.os_abspath,  self.config.get('folder_db'))
        self.create_directory([self.path_db]) 
        
        # абсолютный путь к файлу БД
        self.full_path_db = join(self.path_db, self.config.get('name_db'))
        
        # url файла БД
        self.db_url = f'sqlite+aiosqlite:///{self.full_path_db}'
        
        # создаем объект engine для связи с БД
        self.engine = create_async_engine(self.db_url)
        
        # Создаем асинхронный объект Session 
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)
        
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
            'cls_name',
            'os_abspath',
            'sys_abspath',
            'path_db',
            'full_path_db'
        ]

        for attr in attributes_to_print:
            # "Attribute not found" будет выведено, если атрибут не существует
            value = getattr(self, attr, "Attribute not found")  
            msg += f"{attr}: {value}\n"
        print(msg)
        self.logger.log_info(msg)

    # создаем директорию, если такой папки нет
    def create_directory(self, paths: list[str]):
        """
        Создает директорию для хранения video и ключевых кадров, 
        если она не существует

         Аргументы:
        - paths: список строк, каждая из которых является путем к директории, которую необходимо создать.
        """
        _ = [makedirs(path,  exist_ok=True) for path in paths]


    # Создание таблицы в базе данных
    async def create_tables(self, meta_data: MetaData):
        # @safe_await_execute(logger=self.logger, name_method='create_tables')
        @safe_await_alchemy_exe(logger=self.logger, name_method='create_tables')
        async def _create_tables(meta_data: MetaData):
            async with self.engine.begin() as connect:
                await connect.run_sync(meta_data.create_all)
        return await _create_tables(meta_data)

    # выводим состояние системы
    def system_status(self):
        file_start = basename(argv[0]) #  [start_hasher.py]
        print(f'\n[{__name__}|{self.cls_name}] Create tables in the database...\n')  
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

    def check_file(self, full_paths: list[str], comment: str = None):
        """
        Проверяем наличие файла в директории 

         Аргументы:
        - paths: список строк, каждая из которых является полным путем к файлу, который необходимо проверить.
        """
        notexist_paths = [full_path for full_path in full_paths if not isfile(full_path)]
        if notexist_paths:
            print(f'\n[{__name__}|{self.cls_name}] ERROR: Files do not exist at these paths: {notexist_paths}')
            raise FileNotFoundError(f'\n[{self.cls_name}] Files not found: {notexist_paths}')
        else: 
            for full_path in full_paths: 
                print(f'\n[{__name__}|{self.cls_name}] {comment}: {full_path}')

# 
# MAIN **************************
async def main():
    # Создаем экземпляр класса Start
    mkdb=Make_db()
    mkdb.log_memory() # логирование информации о памяти
    mkdb.system_status() # выводим состояние системы
     
    # Создание таблицы в базе данных
    await mkdb.create_tables(mkdb.metadata)
    # проверяем создание файла БД
    mkdb.check_file([mkdb.full_path_db], 'File Data Base EXIST') 

if __name__ == "__main__":
    run(main())


