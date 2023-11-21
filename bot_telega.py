#!/usr/bin/env python3 
#
from asyncio import run 
from pynvml import nvmlInit, nvmlDeviceGetCount, nvmlDeviceGetHandleByIndex, nvmlDeviceGetMemoryInfo, nvmlShutdown
from time import strftime
from os import makedirs
from os.path import basename, join, abspath, dirname
from sys import platform, argv
from psutil import virtual_memory
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
    """Modul for TELEGRAM Bot"""
    countInstance=0
    #
    def __init__(self):
        super().__init__()
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
        # Bot init
        self.bot_init = BotInitializer(self.logger)
        self.bot_init.initialize_bot()
        # Bot
        self.bot = self.bot_init.get_bot()
        # Dispatcher
        self.dp = self.bot_init.get_dp()
        #
        # Импорт словаря {имя таблицы : таблица}  
        self.name_table = DiffTable.tables
        # MethodDB
        self.methodDb=MethodDB(self.logger, self.config_path)
        # HandlersBot
        self.folder_video = self.config.get('folder_video')
        self.folder_kframes = self.config.get('folder_kframes') 
        self.client = HandlersBot(self.config_path, self.logger, self.bot, self.dp, self.methodDb)
        # Router
        self.router = self.client.router
        #
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
            # 'name_table',
            'methodDb',
            'folder_video',
            'folder_kframes',
            'client',
            'router',
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


    # отправляем сообщение
    async def send_msg(self, row: Row, msg: str):
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _send_msg():
            chat_id = str(row.chat_id)
            username=str(row.username)
            message = await self.bot.send_message(chat_id, msg)       
            if not message:
                    print(f'\n[Telega send_msg] не удалось отправить пользователю [{username}] сообщение')
                    return None
            return message  
        return await  _send_msg()



# MAIN **************************
async def main():
    # Создаем экземпляр класса Telega 
    # создаем объект и в нем регистрируем хэндлеры Клиента
    telega=Telega()  
    telega.log_memory() # логирование информации о памяти
    telega.system_status() # выводим состояние системы

    bot = telega.bot
    dp = telega.dp
    router = telega.router
    # регистрируем обработчики
    dp.include_routers(router)
    # await telega.client_work()
    drop_pending_updates = await bot.delete_webhook(drop_pending_updates=True)
    print(f'\n[telega main] сбросили ожидающие обновления: [{drop_pending_updates}]')
    # старт опроса
    await dp.start_polling(bot)
    print(f'\n[telega main] закончили start_polling...')
    #
#
#
if __name__ == "__main__":
    run(main())

