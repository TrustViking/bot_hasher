#!/usr/bin/env python3 
#

from aiogram.types import InputFile, InputMediaPhoto
# from argparse import ArgumentParser
from asyncio import run 
# from logging import getLevelName
from os.path import basename, join, abspath, dirname
from os import listdir
from psutil import virtual_memory
from pynvml import nvmlInit, nvmlDeviceGetCount, nvmlDeviceGetHandleByIndex, nvmlDeviceGetMemoryInfo, nvmlShutdown
import sys
from sys import argv
from sqlalchemy.engine.result import Row
from time import sleep, strftime
# from typing import Coroutine, Callable, Any
from typing import Optional

#
# from bot_env.create_obj4bot import dp, bot
# from bot_env.mod_log import LogBot
from data_base.table_db import DiffTable
from bot_env.bot_init import LogInitializer, BotInitializer, ConfigInitializer
from bot_env.decorators import safe_await_execute, safe_execute
from data_base.base_db import MethodDB

#
#
class Sender(ConfigInitializer):
    """Modul for sended message to users"""
    countInstance=0
    #
    def __init__(self):
        super().__init__()
        Sender.countInstance += 1
        self.countInstance = Sender.countInstance

        self.cls_name = self.__class__.__name__
        self.abspath = dirname(abspath(__file__))
        # config
        self.config_path = join(dirname(abspath(__file__)), 'config.json')
        self.config = self.read_config(self.config_path)
        self.folder_video = self.config.get('folder_video')
        self.folder_kframes = self.config.get('folder_kframes') 
        self.pause_minut_sender = self.config.get('pause_minut_sender') 
        # Logger
        self.log_init = LogInitializer()
        self.logger = self.log_init.initialize(self.config_path)
        # Импорт словаря {имя таблицы : таблица}  
        self.name_table = DiffTable.tables
        # MethodDB
        self.methodDb=MethodDB(self.logger, self.config_path)
        # Bot
        self.bot_init = BotInitializer(self.logger)
        self.bot_init.initialize_bot()
        self.bot = self.bot_init.get_bot()
        self._print()
    
    #
    # выводим № объекта
    def _print(self):
        msg = (
            f"\nStarted at {strftime('%X')}\n"
            f'[{__name__}|{self.cls_name}] countInstance: [{self.countInstance}]\n'
            f'platform: [{sys.platform}]\n'
            f'\nAttributes:\n'
            )

        attributes_to_print = [
            'cls_name',
            'abspath',
            'config_path',
            'config',
            'folder_video',
            'folder_kframes',
            'pause_minut_sender',
            'log_init',
            'logger',
            'methodDb',
        ]

        for attr in attributes_to_print:
            # "Attribute not found" будет выведено, если атрибут не существует
            value = getattr(self, attr, "Attribute not found")  
            msg += f"{attr}: {value}\n"

        print(msg)
        self.logger.log_info(msg)


    # выводим состояние системы
    def system_status(self):
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def _system_status():
            print(f'\n[{__name__}|{self.cls_name}] Start...\n')  
            file_start = basename(argv[0]) #  [start_hasher.py]
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
    # есть схожие ключевые кадры, но не отправленные 
    async def rows4send (self):
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _rows4send ():
            async_results = await self.methodDb.read_data_two( 
                            name_table = 'diff',  
                            one_column = 'save_sim_img', 
                            one_params = 'saved',
                            two_column = 'sender_user', 
                            two_params = 'not_sender',
                            )
            rows = async_results.fetchall()
            if not rows: 
                return None
            return rows
        return await _rows4send()


    # нет ключевых кадров
    async def rows_notkframes(self):
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _rows_notkframes():
            async_results = await self.methodDb.read_data_two( 
                            name_table = 'diff',  
                            one_column = 'result_kframe', 
                            one_params = 'not_kframe',
                            two_column = 'sender_user',
                            two_params = 'not_sender',
                                                        )
            rows = async_results.fetchall()
            if not rows: 
                return None
            return rows
        return await _rows_notkframes()


    # нет схожих кадров
    async def rows_notsimilar(self):
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _rows_notsimilar():
            async_results = await self.methodDb.read_data_two( 
                            name_table = 'diff',  
                            one_column = 'result_diff', 
                            one_params = 'not_similar',
                            two_column = 'sender_user',
                            two_params = 'not_sender',
                                                        )
            rows = async_results.fetchall()
            if not rows: 
                return None
            return rows
        return await _rows_notsimilar()


    # отправляем схожие ключевые кадры
    async def send_kframes (self, row: Row):
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _send_kframes ():
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
                    print(f'\nERROR [{__name__}|{self.cls_name}] В отобранных ключевых кадрах не совпала id пары: \n'
                        f'id_1: {id_1} и d_2: {d_2}')
                    continue
                if method_hash_1 != method_hash_2:
                    print(f'\nERROR [{__name__}|{self.cls_name}] В отобранных ключевых кадрах не совпал метод пары: \n'
                        f'method_hash_1: {method_hash_1} и method_hash_2: {method_hash_2}')
                    continue
                if d_1 != d_2:
                    print(f'\nERROR [{__name__}|{self.cls_name}] В отобранных ключевых кадрах не совпало расстояние: \n'
                        f'd_1: {d_1} и d_2: {d_2}')
                    continue
                if threshold_1 != threshold_2:
                    print(f'\nERROR [{__name__}|{self.cls_name}] В отобранных ключевых кадрах не совпал порог схожести: \n'
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
                    print(f'\n[{__name__}|{self.cls_name}] Не отправили пользователю сообщение: {msg}')
                    continue
                
                full_path_1 = join(path_folder, nfiles[i])
                full_path_2 = join(path_folder, nfiles[i+1] if i + 1 < len(nfiles) else None)
                # name = str(xhash+'_id'+str(index)+'_d'+str(int(d))+'_2.png')
                file_kframe_1=InputMediaPhoto(media=InputFile(full_path_1))
                file_kframe_2=InputMediaPhoto(media=InputFile(full_path_2))
                # media_group = MediaGroup([file_kframe_1, file_kframe_2])
                media_group = [file_kframe_1, file_kframe_2]
                message_group = await self.bot.send_media_group(
                                                chat_id=chat_id, 
                                                media=media_group,
                                                reply_to_message_id=message.message_id,
                                                allow_sending_without_reply=True,
                                                )
                if not message_group:
                    print(f'\n[{__name__}|{self.cls_name}] не удалось отправить пользователю [{row.username}] файл: {full_path_1}')
                else: sended_pair.append((nfiles[i], nfiles[i+1]))
            return nfiles, sended_pair  
        return await _send_kframes()

    # отправляем сообщение
    async def send_msg(self, row: Row, msg: str):
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _send_msg ():
            chat_id = str(row.chat_id)
            username=str(row.username)
            message = await self.bot.send_message(chat_id=chat_id, text=msg)
            if not message:
                print(f'\n[{__name__}|{self.cls_name}] не удалось отправить пользователю [{username}] сообщение')
                return None
            return message
        return await _send_msg()

    def making_msg(self, row: Row)-> Optional[str]:
        """
        Формирует сообщение о результатах анализа видео.

        Этот метод принимает объект 'Row', содержащий данные об анализе видео,
        и формирует из них информативное сообщение. В случае успеха возвращает строку,
        в противном случае - None.

        Args:
            row (Row): Объект 'Row', содержащий данные анализа.

        Returns:
            Optional[str]: Сформированное сообщение о результатах анализа или None, если сообщение не было сформировано.
        """        
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def _making_msg():
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
            return msg
        return _making_msg()




# MAIN **************************
async def main():
    sender=Sender() 
    sender.log_memory() # логирование информации о памяти
    sender.system_status() # выводим состояние системы
    #
    minut=sender.pause_minut_sender
    while True:
        print(f'\nБот по отправке схожих ключевых кадров ждет {minut} минут(ы) ...')
        sleep (int(60*minut))
        
        print(f'\nСодержание таблиц в БД...')
        await sender.methodDb.print_tables(['diff'])
        
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
                    if not await sender.methodDb.update_table_date_chatid(['diff'], row.date_message, row.chat_id, diction):
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
                    if not await sender.methodDb.update_table_date_chatid(['diff'], row.date_message, row.chat_id, diction):
                        print(f'\nERROR [Sender main] не отметили в таблице отправку cообщения: {messege}')
        
        # нет схожих пар ключевых кадров
        rows_notsimilars = await sender.rows_notsimilar()
        if rows_notsimilars:
            for row in rows_notsimilars:
                msg = sender.making_msg(row)

                # отправляем сообщение пользователю
                message = await sender.send_msg(row, msg)
                if not message:
                    print(f'\n[Sender main] Не отправили пользователю сообщение')
                else:
                    # отмечаем в таблице diff факт отправки ответа пользователю
                    diction = {'sender_user':'sender'}
                    if not await sender.methodDb.update_table_date_chatid(['diff'], row.date_message, row.chat_id, diction):
                        print(f'\nERROR [Sender main] не отметили в таблице отправку cообщения: {messege}')
#
if __name__ == "__main__":
    run(main())




