#!/usr/bin/env python3 
#
 

from asyncio import run 
from pynvml import nvmlInit, nvmlDeviceGetCount, nvmlDeviceGetHandleByIndex, nvmlDeviceGetMemoryInfo, nvmlShutdown
from time import strftime
from os import makedirs
from os.path import basename, join, abspath, dirname, isfile
from sys import platform, argv, path
from psutil import virtual_memory
from typing import Coroutine
from sqlalchemy.engine.result import Row
#
from data_base.table_db import DiffTable
from bot_env.bot_init import LogInitializer, BotInitializer, ConfigInitializer
from bot_env.decorators import safe_await_execute, safe_execute
from data_base.base_db import MethodDB
from handlers.client import  HandlersBot

#
#
class Telega(ConfigInitializer):
    """Modul for TELEGRAM"""
    countInstance=0
    #
    def __init__(self):

        Telega.countInstance += 1
        self.countInstance = Telega.countInstance
        self.cls_name = self.__class__.__name__
        self.abspath = dirname(abspath(__file__))
        # config
        self.config_path = join(dirname(abspath(__file__)), 'config.json')
        self.config = self.read_config(self.config_path)
        # Logger
        self.log_init = LogInitializer()
        self.logger = self.log_init.initialize(self.config_path)
        # Bot, Dispatcher, Router
        self.bot_init = BotInitializer(self.logger)
        self.bot_init.initialize_bot(self.config_path)
        self.bot = self.bot_init.bot
        self.dp = self.bot_init.dp
        self.router = self.bot_init.router
        #
        # Импорт словаря {имя таблицы : таблица}  
        self.name_table = DiffTable.tables
        # MethodDB
        self.methodDb=MethodDB(self.logger, self.config_path)
        # HandlersBot
        self.folder_video = self.config.get('folder_video')
        self.folder_kframes = self.config.get('folder_kframes') 
        self.client = HandlersBot(self.logger, self.folder_video, self.folder_kframes)
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
            'folder_video',
            'folder_kframes',
            'client',
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
#
    # создаем директорию, если такой папки нет
    def create_directory(self, paths: list[str]):
        """
        Создает директорию для хранения video и ключевых кадров, 
        если она не существует

         Аргументы:
        - paths: список строк, каждая из которых является путем к директории, которую необходимо создать.
        """
        _ = [makedirs(path,  exist_ok=True) for path in paths]
#
    # # обертка для безопасного выполнения методов
    # # async def safe_execute(self, coroutine: Callable[..., Coroutine[Any, Any, T]]) -> T:
    # async def safe_await_execute(self, coroutine: Coroutine, name_func: str = None):
    #     try:
    #         return await coroutine
    #     except Exception as eR:
    #         print(f'\nERROR[Handlers4bot {name_func}] ERROR: {eR}') 
    #         self.Logger.log_info(f'\nERROR[Handlers4bot {name_func}] ERROR: {eR}') 
    #         return None

    ### запускаем клиент бот-телеграм
    async def client_work(self):
        await self.safe_await_execute(self.client.register_handlers_client(), 'client_work')            

    # отправляем сообщение
    async def send_msg(self, row: Row, msg: str):
        chat_id = str(row.chat_id)
        username=str(row.username)
        message = await self.safe_await_execute(self.bot.send_message(chat_id=chat_id, text=msg), 'send_msg')       
        if not message:
                print(f'\n[Telega send_msg] не удалось отправить пользователю [{username}] сообщение')
                return None
        return message  

    # выводим состояние системы
    def system_status(self):
        print(f'\nСтарт приложения...\n') 
        file_start = basename(argv[0])
        path_start = path[0]
        msg = (
            f'File: [{file_start}]\n'
            f'Path: [{path_start}]\n'
            f'Data memory:'
                )
        print(msg)
        memory = virtual_memory()
        for field in memory._fields:
            print(f"{field}: {getattr(memory, field)}")    

    # логирование информации о памяти
    def log_memory(self):
        self.Logger.log_info(f'****************************************************************')
        self.Logger.log_info(f'*Data RAM {basename(argv[0])}: [{virtual_memory()[2]}%]')
        # Инициализируем NVML для сбора информации о GPU
        nvmlInit()
        deviceCount = nvmlDeviceGetCount()
        self.Logger.log_info(f'\ndeviceCount [{deviceCount}]')
        for i in range(deviceCount):
            handle = nvmlDeviceGetHandleByIndex(i)
            meminfo = nvmlDeviceGetMemoryInfo(handle)
            self.Logger.log_info(f"#GPU [{i}]: used memory [{int(meminfo.used / meminfo.total * 100)}%]")
            self.Logger.log_info(f'****************************************************************\n')
        # Освобождаем ресурсы NVML
        nvmlShutdown()


# MAIN **************************
async def main():
    # Создаем экземпляр класса Telega 
    # создаем объект и в нем регистрируем хэндлеры Клиента
    telega=Telega()  
    telega.log_memory() # логирование информации о памяти
    telega.system_status() # выводим состояние системы
    # регистрируем обработчики
    await telega.client_work()
    drop_pending_updates = await telega.bot.delete_webhook(drop_pending_updates=True)
    print(f'\n[telega main] сбросили ожидающие обновления: [{drop_pending_updates}]')
    # обработчики
    await telega.dp.start_polling(telega.bot)
    print(f'\n[telega main] закончили start_polling...')
    #
#
#
if __name__ == "__main__":
    run(main())

