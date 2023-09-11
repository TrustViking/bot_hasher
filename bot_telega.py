#!/usr/bin/env python3 
#
import logging, os, sys
from aiogram.utils import executor
from argparse import ArgumentParser
from time import sleep, time
from sqlalchemy.engine.result import Row

#
from bot_env.create_obj4bot import dp, bot
from bot_env.mod_log import Logger
from handlers.client import  Handlers4bot
from data_base.base_db import BaseDB

#
#
class Telega:
    """Modul for TELEGRAM"""
    countInstance=0
    #
    def __init__(self, 
                 log_file='telega_log.md',  
                 log_level=logging.DEBUG,
                 folder_video= 'diff_video',
                 folder_kframes = 'diff_kframes',
                 ):
        Telega.countInstance += 1
        self.countInstance = Telega.countInstance
        self.log_file=log_file
        self.log_level=log_level
        self.folder_video=folder_video
        self.folder_kframes=folder_kframes
        # Разбор аргументов
        self._arg_parser()
        # Logger
        self.Logger = Logger(log_file=self.log_file, log_level=self.log_level)
        self.Db=BaseDB(logger=self.Logger)
        # Client
        self.client = Handlers4bot(logger=self.Logger, 
                                   folder_video=self.folder_video,
                                   folder_kframes=self.folder_kframes,

                                   )
        self.bot=bot
        self._print()
    #
    # выводим № объекта
    def _print(self):
        print(f'\n[Telega] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'\n[Telega] countInstance: [{self.countInstance}]\n')
        print(f'Аргументы:\n'
              f'log_file: {self.log_file}\n'
              f'log_level: {self.log_level}\n'
              f'folder_video: {self.folder_video}\n'
              f'folder_kframes: {self.folder_kframes}\n'
              )
#
    # добавление аргументов строки
    def _arg_added(self, parser: ArgumentParser):
        # Добавление аргументов
        parser.add_argument('-v', '--folder_video', type=str, help='Папка для видео')
        parser.add_argument('-k', '--folder_kframes', type=str, help='Папка для схожих кадров')
        parser.add_argument('-f', '--log_file', type=str, help='Имя журнала логгирования')
        parser.add_argument('-l', '--log_level', type=str, help='Уровень логгирования')

    # Разбор аргументов строки
    def _arg_parser(self):
        # Инициализация парсера аргументов
        parser = ArgumentParser()
        # добавление аргументов строки
        self._arg_added(parser)
        args = parser.parse_args()

        if args.log_file: self.log_file=args.log_file
        # (CRITICAL, ERROR, WARNING,INFO, DEBUG)
        if args.log_level: self.log_level=int(args.log_level)
        if args.folder_video: self.folder_video=args.folder_video
        if args.folder_kframes: self.folder_kframes=args.folder_kframes
#
    # обертка для безопасного выполнения методов
    # async def safe_execute(self, coroutine: Callable[..., Coroutine[Any, Any, T]]) -> T:
    async def safe_await_execute(self, coroutine, name_func: str = None):
        try:
            return await coroutine
        except Exception as eR:
            print(f'\nERROR[Handlers4bot {name_func}] ERROR: {eR}') 
            self.Logger.log_info(f'\nERROR[Handlers4bot {name_func}] ERROR: {eR}') 
            return None

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

# MAIN **************************
async def main(_):
    print(f'\n**************************************************************************')
    print(f'\nБот вышел в онлайн')
    # создаем объект и в нем регистрируем хэндлеры Клиента
    telega=Telega()  
    telega.Logger.log_info(f'\n[main] Создали объект {telega}')
    print(f'\n[main] Создали объект {telega}')
    await telega.client_work()
    #
#
if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=main)


