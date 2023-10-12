#!/usr/bin/env python3 
#
from aiogram.types import InputFile, InputMediaPhoto
from argparse import ArgumentParser
from asyncio import run 
from logging import getLevelName
from os.path import join, getmtime, basename
from os import makedirs, listdir, remove
from psutil import virtual_memory
from pynvml import nvmlInit, nvmlDeviceGetCount, nvmlDeviceGetHandleByIndex, nvmlDeviceGetMemoryInfo, nvmlShutdown
from sys import platform, argv, path
from sqlalchemy.engine.result import Row
from time import sleep, strftime
from typing import Coroutine, Callable, Any
#
from bot_env.create_obj4bot import dp, bot
from bot_env.mod_log import Logger
# from handlers.client import  Handlers4bot
from data_base.base_db import BaseDB

#
#
class Sender:
    """Modul for sended message to users"""
    countInstance=0
    #
    def __init__(self, 
                folder_logfile = 'logs',
                logfile='hashervid_log.md', 
                loglevel='DEBUG',
                folder_db = 'db_storage',
                name_db = 'db_file.db',
                folder_video= 'diff_video',
                folder_kframes = 'diff_kframes',
                pause_minut=1,
                 ):
        Sender.countInstance += 1
        self.countInstance = Sender.countInstance
        # args Logger
        self.folder_logfile = folder_logfile
        self.logfile=logfile
        self.loglevel=loglevel
        # args BaseDB
        self.folder_db = folder_db
        self.name_db = name_db
        # Handlers4bot
        self.folder_video=folder_video
        self.folder_kframes=folder_kframes
        self.pause_minut = pause_minut
        # Разбор аргументов
        self._arg_parser()
        # Logger
        self.Logger = Logger(self.folder_logfile, self.logfile, self.loglevel)
        # BaseDB
        self.Db=BaseDB(self.Logger, self.folder_db, self.name_db)
        # Handlers4bot
        # self.client = Handlers4bot(self.Logger, self.folder_video, self.folder_kframes)
        self.bot=bot
        self._print()
    #
    # выводим № объекта
    def _print(self):
        print(f'\n[HasherVid] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'\n[HasherVid] countInstance: [{self.countInstance}]\n')
        msg = (f"Started at {strftime('%X')}\n"
              f'platform: [{platform}]\n'
              f'\nАргументы:\n'
              f'folder_logfile: {self.folder_logfile}\n'
              f'logfile: {self.logfile}\n'
              f'loglevel: {self.loglevel}\n'
              f'folder_db: {self.folder_db}\n'
              f'name_db: {self.name_db}\n'
              f'folder_video: {self.folder_video}\n'
              f'folder_kframes: {self.folder_kframes}\n'
              f'pause_minut: {self.pause_minut}\n'
              )
#
    # добавление аргументов командной строки
    def _arg_added(self, parser: ArgumentParser):
        # Добавление аргументов
        parser.add_argument('-fd', '--folder_db', type=str, help='Папка для БД')
        parser.add_argument('-nd', '--name_db', type=str, help='Имя файла БД')
        #
        parser.add_argument('-fv', '--folder_video', type=str, help='Папка для видео')
        parser.add_argument('-fk', '--folder_kframes', type=str, help='Папка для схожих кадров')
        #
        parser.add_argument('-fl', '--folder_logfile', type=str, help='Папка для логов')
        parser.add_argument('-lf', '--logfile', type=str, help='Имя журнала логгирования')
        parser.add_argument('-ll', '--loglevel', type=str, help='Уровень логирования')
        # 
        parser.add_argument('-pm', '--pause_minut', type=int, help='Пауза в работе бота')

    def _arg_parser(self):
        # Инициализация парсера аргументов
        parser = ArgumentParser()
        # добавление аргументов строки
        self._arg_added(parser)
        args = parser.parse_args()
        
        if args.folder_db: 
            self.folder_db=args.folder_db
        if args.name_db: 
            self.name_db=args.name_db

        if args.folder_video: 
            self.folder_video=args.folder_video
        if args.folder_kframes: 
            self.folder_kframes=args.folder_kframes
        #
        if args.folder_logfile: 
            self.folder_logfile=args.folder_logfile
        if args.logfile: 
            self.logfile=args.logfile
        if args.loglevel: 
            self.loglevel=getLevelName(args.loglevel.upper()) # (CRITICAL, ERROR, WARNING,INFO, DEBUG)
        #
        if args.pause_minut: 
            self.pause_minut=args.pause_minut
#
    # обертка для безопасного выполнения методов
    # async def safe_execute(self, coroutine: Callable[..., Coroutine[Any, Any, T]]) -> T:
    async def safe_await_execute(self, coroutine: Coroutine, name_func: str = None):
        try:
            return await coroutine
        except Exception as eR:
            print(f'\nERROR[Sender {name_func}] ERROR: {eR}') 
            self.Logger.log_info(f'\nERROR[Sender {name_func}] ERROR: {eR}') 
            return None

    # синхронная обертка для безопасного выполнения методов
    def safe_execute(self, func: Callable[..., Any], name_func: str = None):
        try:
            return func()
        except Exception as eR:
            print(f'\nERROR[HasherVid {name_func}] ERROR: {eR}')
            self.Logger.log_info(f'\nERROR[HasherVid {name_func}] ERROR: {eR}')
            return None


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

    # запускаем клиент бот-телеграм
    # async def client_work(self):
    #     try:
    #         await self.client.register_handlers_client()            
    #     except Exception as eR:
    #         print (f'[Sender] error: {eR}')
    #         self.Logger.log_info(f'[Sender] error: {eR}')   
    #
    # есть схожие ключевые кадры, но не отправленные 
    async def rows4send (self):
        try:
            async_results = await self.Db.read_data_two( 
                            name_table = 'diff',  
                            one_column_name = 'save_sim_img', 
                            one_params_status = 'saved',
                            two_column_name = 'sender_user', 
                            two_params_status = 'not_sender',
                                                        )
        except Exception as eR:
            print(f"\n[Sender rows4send] Не удалось прочитать таблицу diff: {eR}")
            self.Logger.log_info(f"\n[Sender rows4send] Не удалось прочитать таблицу diff: {eR}")
            return None
        rows = async_results.fetchall()
        if not rows: return None
        return rows

    # нет ключевых кадров
    async def rows_notkframes(self):
        try:
            async_results = await self.Db.read_data_two( 
                            name_table = 'diff',  
                            one_column_name = 'result_kframe', 
                            one_params_status = 'not_kframe',
                            two_column_name = 'sender_user',
                            two_params_status = 'not_sender',
                                                        )
        except Exception as eR:
            print(f"\n[Sender rows_notkframes] Не удалось прочитать таблицу diff: {eR}")
            self.Logger.log_info(f"\n[Sender rows_notkframes] Не удалось прочитать таблицу diff: {eR}")
            return None
        rows = async_results.fetchall()
        if not rows: return None
        return rows

    # нет схожих кадров
    async def rows_notsimilar(self):
        try:
            async_results = await self.Db.read_data_two( 
                            name_table = 'diff',  
                            one_column_name = 'result_diff', 
                            one_params_status = 'not_similar',
                            two_column_name = 'sender_user',
                            two_params_status = 'not_sender',
                                                        )
        except Exception as eR:
            print(f"\n[Sender rows_notsimilar] Не удалось прочитать таблицу diff: {eR}")
            self.Logger.log_info(f"\n[Sender rows_notsimilar] Не удалось прочитать таблицу diff: {eR}")
            return None
        rows = async_results.fetchall()
        if not rows: return None
        return rows

    # отправляем схожие ключевые кадры
    async def send_kframes (self, row: Row):
        sended_pair=[]
        path_folder = str(row.path_sim_img)
        chat_id = str(row.chat_id)
        nfiles = sorted(listdir(path_folder))
        for i in range(0, len(nfiles), 2):
            sleep(5) # иначе выдает ошибку при очень частых сообщениях ERROR: Flood control exceeded
            # name_2 = str('id'+str(index)+'_'+xhash+'_d'+str(int(d))+'_'+str(threshold)+'_2.png')
            nfiles_1 = nfiles[i].split('_') # id0, dhash, d10, 12, 1.png
            nfiles_2 = nfiles[i+1].split('_')
            id_1 = nfiles_1[0][2:] # '_id'+str(index)
            id_2 = nfiles_2[0][2:] # '_id'+str(index)
            method_hash_1 = nfiles_1[1] # xhash
            method_hash_2 = nfiles_2[1] # xhash
            d_1 = nfiles_1[2][1:] # '_d'+str(int(d))
            d_2 = nfiles_2[2][1:] # '_d'+str(int(d))
            threshold_1 = nfiles_1[3] # порог схожести
            threshold_2 = nfiles_2[3] # порог схожести
            if id_1 != id_2:
                print(f'\nERROR [Sender send_kframes] В отобранных ключевых кадрах не совпала id пары: \n'
                      f'id_1: {id_1} и d_2: {d_2}')
                continue
            if method_hash_1 != method_hash_2:
                print(f'\nERROR [Sender send_kframes] В отобранных ключевых кадрах не совпал метод пары: \n'
                      f'method_hash_1: {method_hash_1} и method_hash_2: {method_hash_2}')
                continue
            if d_1 != d_2:
                print(f'\nERROR [Sender send_kframes] В отобранных ключевых кадрах не совпало расстояние: \n'
                      f'd_1: {d_1} и d_2: {d_2}')
                continue
            if threshold_1 != threshold_2:
                print(f'\nERROR [Sender send_kframes] В отобранных ключевых кадрах не совпал порог схожести: \n'
                      f'd_1: {threshold_1} и d_2: {threshold_2}')
                continue
            
            msg=(
                f'id: {id_1} '
                f'метод: {method_hash_1} \U0001F447 '
                f'расстояние: {d_1} '
                f'порог: {threshold_1} '
                 )
            message = await self.send_msg(row, msg)
            if not message:
                print(f'\n[Sender send_kframes] Не отправили пользователю сообщение: {msg}')
                continue
            full_path_1 = join(path_folder, nfiles[i])
            full_path_2 = join(path_folder, nfiles[i+1] if i + 1 < len(nfiles) else None)
            # name = str(xhash+'_id'+str(index)+'_d'+str(int(d))+'_2.png')
            file_kframe_1=InputMediaPhoto(media=InputFile(full_path_1))
            file_kframe_2=InputMediaPhoto(media=InputFile(full_path_2))
            # media_group = MediaGroup([file_kframe_1, file_kframe_2])
            media_group = [file_kframe_1, file_kframe_2]
            message_group = await self.safe_await_execute(
                            self.bot.send_media_group(chat_id=chat_id, 
                                                      media=media_group,
                                                      reply_to_message_id=message.message_id,
                                                      allow_sending_without_reply=True), 
                                                      'send_kframes')
            
            if not message_group:
                print(f'\n[Sender send_kframes] не удалось отправить пользователю [{row.username}] файл: {full_path_1}')
            else: sended_pair.append((nfiles[i], nfiles[i+1]))
        return nfiles, sended_pair  

    # отправляем сообщение
    async def send_msg(self, row: Row, msg: str):
        chat_id = str(row.chat_id)
        username=str(row.username)
        message = await self.safe_await_execute(self.bot.send_message(chat_id=chat_id, text=msg), 'send_msg')       
        if not message:
                print(f'\n[Sender send_msg] не удалось отправить пользователю [{username}] сообщение')
                return None
        return message  

# MAIN **************************
async def main():
    sender=Sender() 
    sender.log_memory() # логирование информации о памяти
    sender.system_status() # выводим состояние системы
    #
    minut=sender.pause_minut
    while True:
        print(f'\nБот по отправке схожих ключевых кадров ждет {minut} минут(ы) ...')
        sleep (int(60*minut))
        print(f'\nСодержание таблиц в БД...')
        await sender.Db.print_tables('diff')
        # сохраненные и не отправленные kframes
        rows_send = await sender.rows4send()
        if rows_send: 
            for row in rows_send:
                sended = await sender.send_kframes(row)
                if not sended:
                    print(f'\n[Sender main] Не отправили пользователю [{row.username}] схожие кадры')
                else:
                    nfiles, sended_pair = sended 
                    print(f'\n[Sender main] Из [{int(len(nfiles)/2)}] пар файлов отправили [{len(sended_pair)}] схожих изображений')
                    # отмечаем в таблице diff факт отправки ответа пользователю
                    diction = {'sender_user':'sender'}
                    if not await sender.Db.update_table_date_chatid(['diff'], row.date_message, row.chat_id, diction):
                        print(f'\nERROR [Sender main] не отметили в таблице отправку файлов: {nfiles}')
                              
        # нет ключевых кадров
        rows_notkfram = await sender.rows_notkframes()
        if rows_notkfram: 
            for row in rows_notkfram:
                msg = (f'В Вашем грустном кино ключевых кадров для сравнения не определили. \n'
                       f'Либо уменьшите порог определения ключевых кадров либо этих кадров вообще нет' )
                messege = await sender.send_msg(row, msg)
                if not messege:
                    print(f'\n[Sender main] Не отправили пользователю сообщение')
                else: 
                    # отмечаем в таблице diff факт отправки ответа пользователю
                    diction = {'sender_user':'sender'}
                    if not await sender.Db.update_table_date_chatid(['diff'], row.date_message, row.chat_id, diction):
                        print(f'\nERROR [Sender main] не отметили в таблице отправку cообщения: {messege}')
        
        # нет схожих пар ключевых кадров
        rows_notsimilars = await sender.rows_notsimilar()
        if rows_notsimilars:
            for row in rows_notsimilars:
                msg = ( 
                    f'СХОЖИХ КАДРОВ НЕ ОПРЕДЕЛИЛИ \n'
                    f'Порог ключевых кадров: [{row.threshold_kframes}]\n'
                    f'В первом видео ключевых кадров: [{row.num_kframe_1}]\n'
                    f'Во втором видео ключевых кадров: [{row.num_kframe_2}]\n'
                    f'Порог схожести кадров: [{row.hash_factor}]\n'
                    #
                    f'\nМетод: [{row.phash}]\n'
                    f'Порог расстояний между ключевыми кадрами: [{row.phash_threshold}]\n'
                    f'Минимальное значение в матрице: [{row.phash_min_distance}]\n'
                    f'Максимальное значение в матрице: [{row.phash_max_distance}]\n'
                    f'Размерность матрицы: [{row.phash_matrix_shape}]\n'
                    #
                    f'\nМетод: [{row.dhash}]\n'
                    f'Порог расстояний между ключевыми кадрами: [{row.dhash_threshold}]\n'
                    f'Минимальное значение в матрице: [{row.dhash_min_distance}]\n'
                    f'Максимальное значение в матрице: [{row.dhash_max_distance}]\n'
                    f'Размерность матрицы: [{row.dhash_matrix_shape}]\n'
                    #
                    f'\nМетод: [{row.wavelet}]\n'
                    f'Порог расстояний между ключевыми кадрами: [{row.wavelet_threshold}]\n'
                    f'Минимальное значение в матрице: [{row.wavelet_min_distance}]\n'
                    f'Максимальное значение в матрице: [{row.wavelet_max_distance}]\n'
                    f'Размерность матрицы: [{row.wavelet_matrix_shape}]\n'
                    #
                    f'\nМетод: [{row.marrihildreth}]\n'
                    f'Порог расстояний между ключевыми кадрами: [{row.marrihildreth_threshold}]\n'
                    f'Минимальное значение в матрице: [{row.marrihildreth_min_distance}]\n'
                    f'Максимальное значение в матрице: [{row.marrihildreth_max_distance}]\n'
                    f'Размерность матрицы: [{row.marrihildreth_matrix_shape}]\n'
                    #
                    f'\nМетод: [{row.blockhash}]\n'
                    f'Порог расстояний между ключевыми кадрами: [{row.blockhash_threshold}]\n'
                    f'Минимальное значение в матрице: [{row.blockhash_min_distance}]\n'
                    f'Максимальное значение в матрице: [{row.blockhash_max_distance}]\n'
                    f'Размерность матрицы: [{row.blockhash_matrix_shape}]\n'
                    f'\nЛибо увеличьте порог определения схожих кадров либо этих кадров вообще нет'
                    )
                # отправляем сообщение пользователю
                message = await sender.send_msg(row, msg)
                if not message:
                    print(f'\n[Sender main] Не отправили пользователю сообщение')
                else:
                    # отмечаем в таблице diff факт отправки ответа пользователю
                    diction = {'sender_user':'sender'}
                    if not await sender.Db.update_table_date_chatid(['diff'], row.date_message, row.chat_id, diction):
                        print(f'\nERROR [Sender main] не отметили в таблице отправку cообщения: {messege}')
#
if __name__ == "__main__":
    run(main())




