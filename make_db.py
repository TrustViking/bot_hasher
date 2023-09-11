#!/usr/bin/env python3
#
import asyncio, os, sys, logging
import subprocess
import asyncio, aiosqlite
from sqlalchemy import MetaData, Table, Column, Integer, String, create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.future import select
from sqlalchemy.exc import SQLAlchemyError
from concurrent.futures import ProcessPoolExecutor
from bot_env.mod_log import Logger
from data_base.table_db import metadata, tables
from data_base.base_db import BaseDB 
#
#
class Make_db(AsyncAttrs, DeclarativeBase):
    """Module for making data base"""
    countInstance=0
    #
    def __init__(self, 
                 log_file='mkdb_log.md', 
                 log_level=logging.DEBUG,
                 db_file='db_file.db',
                 ):
        Make_db.countInstance += 1
        self.countInstance = Make_db.countInstance
        # Logger
        self.Logger = Logger(log_file=log_file, log_level=log_level)
        # Определение структуры таблицы из другого модуля
        self.metadata = metadata
        # Импорт параметров таблицы из другого модуля
        self.table = tables
        # создаем путь для БД
        self.db_file = os.path.join(sys.path[0], 'db_storage', db_file)
        # создаем директорию для БД, если такой папки нет
        self._create_db_directory() 
        # формируем путь к лог файлу 'sqlalchemy'
        self.log_path = os.path.join(self.Logger.log_path, 'sqlalchemy')
        self.rows=None
        # создаем url БД
        self.db_url = f'sqlite+aiosqlite:///{self.db_file}'
        # создаем объект engine для связи с БД
        self.engine = create_async_engine(self.db_url, logging_name=self.log_path)
        # Создаем асинхронный объект Session 
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)
        self._print()
        #     
    # выводим № объекта
    def _print(self):
        print(f'[Make_db] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'[Make_db] countInstance: [{self.countInstance}]')
#
    # создаем директорию для БД, если такой папки нет
    def _create_db_directory(self, path=None):
        if not path:
            db_dir = os.path.dirname(self.db_file)
        else: 
            db_dir = os.path.dirname(path)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)

    # Создание таблицы в базе данных
    async def create_table(self):
        async with self.engine.begin() as connect:
            try:
                # Создаем все таблицы в базе данных
                await connect.run_sync(self.metadata.create_all)
                for name_table in list(self.table.keys()):
                    # проверяем структуру таблицы в БД
                    if not isinstance(self.table.get(name_table), Table):
                        if name_table is not list(self.table.keys()):
                            print(f'[Make_db] [create_table] name_table [{name_table}] is not in dictionary Table')
                        else: 
                            print(f'[Make_db] [create_table] Object in self.table[{name_table}] is not Table. \nObject type: {type(self.table.get(name_table))}')
                        return None
                    else:
                        print(f'[Make_db] [create_table] table[{name_table}]: {type(self.table.get(name_table))}')
            except SQLAlchemyError as e:
                print(f'Error occurred during Table creation: {e}')

# 
# MAIN **************************
async def main():
    print(f'\nСоздаем таблицы в базе данных...')
    # создаем объект класса 
    mkdb=Make_db() 
    # Создание таблицы в базе данных
    await mkdb.create_table() 
    

if __name__ == "__main__":
    asyncio.run(main())



    # # Создание таблицы в базе данных
    # async def create_table(self):
    #     async with self.engine.begin() as conn:
    #         await conn.run_sync(self.metadata.create_all)
    #     #
    #     for name_table in list(self.table.keys()):
    #         # проверяем структуру таблицы в БД
    #         if not isinstance(self.table.get(name_table), Table()):
    #             if name_table is not list(self.table.keys()):
    #                 print(f'[Make_db] [create_table] name_table [{name_table}] is not in dictionary Table')
    #             else: 
    #                 print(f'[Make_db] [create_table] Object in self.table[{name_table}] is not Table. \nObject type: {type(self.table.get(name_table))}')
    #             return None
    #         else:
    #             print(f'[Make_db] [create_table] table[{name_table}]: {type(self.table.get(name_table))}')

    # Запускаем процессы скачивания асинхронно
    # loop = asyncio.get_event_loop()
    # tasks = []
    # with ProcessPoolExecutor(max_workers=y2b.max_download) as executor:
    #     for url in list_url_video_y2b:
    #         task = loop.run_in_executor(executor, y2b.run_yt_dlps, url)
    #         tasks.append(task)

    #     # Ожидаем завершения всех процессов
    #     for future in asyncio.as_completed(tasks):
    #         result = await future
    #         print(f'[main] result: {result}')


        #self.url = f'https://www.youtube.com/watch?v={video_id}'
        #command = ["yt-dlp", 
        # "--progress", 
        # "--socket-timeout", 
        # "999999", 
        # "--no-playlist", 
        # "--merge-output-format", 
        # "mp4", 
        # "--format", 
        # "bv[ext=mp4][height=1080]+ba[ext=m4a]/bestaudio", 
        # "--output", 
        # "/media/ara/linux/snipper/bot_snipper/video_source/%(channel)s/%(title)s.f%(format_id)s.%(resolution)s.%(fps)s.%(ext)s", 
        # url]
        #command = f"yt-dlp --progress --socket-timeout 999999 --no-playlist --merge-output-format mp4 --format 'bv[ext=mp4][height=1080]+ba[ext=m4a]/bestaudio' --output '/media/ara/linux/snipper/bot_snipper/video_source/%(channel)s/%(title)s.f%(format_id)s.%(resolution)s.%(fps)s.%(ext)s' {url}"

# async def main(_):
#     print(f'Бот по скачиванию начал мониторить задачи...')
#     telega=Telega() # создаем объект и в нем регистрируем хэндлеры Клиента, 
#     telega.Logger.log_info(f'\n[main] Создали объект Telega()')
#     print(f'[main] Создали объект Telega()')
# #
# if __name__ == "__main__":
#     executor.start_polling(dp, skip_updates=True, on_startup=main)




