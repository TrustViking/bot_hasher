#!/usr/bin/env python3 
#
import logging, os, sys, asyncio
# from aiogram.utils import executor
from argparse import ArgumentParser
from time import sleep, time
from sqlalchemy.engine.result import Row
from aiogram.types import InputFile, MediaGroup, InputMediaPhoto
#
from bot_env.create_obj4bot import bot
from bot_env.mod_log import Logger
from handlers.client import  Handlers4bot
from data_base.base_db import BaseDB

#
#
class Sender:
    """Modul for sended message to users"""
    countInstance=0
    #
    def __init__(self, 
                 log_file='sender_log.md',  
                 log_level=logging.DEBUG,
                 folder_video= 'diff_video',
                 folder_kframes = 'diff_kframes',
                 ):
        Sender.countInstance += 1
        self.countInstance = Sender.countInstance
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
        print(f'\n[Sender] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'\n[Sender] countInstance: [{self.countInstance}]\n')
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
        # print(f'type parser: {type(parser)}')
        # добавление аргументов строки
        self._arg_added(parser)
        args = parser.parse_args()

        if args.log_file: self.log_file=args.log_file
        # (CRITICAL, ERROR, WARNING,INFO, DEBUG)
        # print(f'args.log_level: {args.log_level}')
        # if args.log_level: self.log_level=logging.getLevelName(args.log_level.upper())
        if args.log_level: self.log_level=int(args.log_level)
        # print(f'self.log_level: {self.log_level}')
        if args.folder_video: self.folder_video=args.folder_video
        if args.folder_kframes: self.folder_kframes=args.folder_kframes
#
    # обертка для безопасного выполнения методов
    # async def safe_execute(self, coroutine: Callable[..., Coroutine[Any, Any, T]]) -> T:
    async def safe_await_execute(self, coroutine, name_func: str = None):
        try:
            return await coroutine
        except Exception as eR:
            print(f'\nERROR[Sender {name_func}] ERROR: {eR}') 
            self.Logger.log_info(f'\nERROR[Sender {name_func}] ERROR: {eR}') 
            return None

    # запускаем клиент бот-телеграм
    async def client_work(self):
        try:
            await self.client.register_handlers_client()            
        except Exception as eR:
            print (f'[Sender] error: {eR}')
            self.Logger.log_info(f'[Sender] error: {eR}')   
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
        nfiles = sorted(os.listdir(path_folder))
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
            full_path_1 = os.path.join(path_folder, nfiles[i])
            full_path_2 = os.path.join(path_folder, nfiles[i+1] if i + 1 < len(nfiles) else None)
            # name = str(xhash+'_id'+str(index)+'_d'+str(int(d))+'_2.png')
            file_kframe_1=InputMediaPhoto(media=InputFile(full_path_1))
            file_kframe_2=InputMediaPhoto(media=InputFile(full_path_2))
            media_group = MediaGroup([file_kframe_1, file_kframe_2])
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
    print(f'\n**************************************************************************')
    # print(f'\nБот вышел в онлайн')
    # создаем объект и в нем регистрируем хэндлеры Клиента
    sender=Sender()  
    sender.Logger.log_info(f'\n[main] Создали объект {sender}')
    print(f'\n[main] Создали объект {sender}')
    #
    minut=1
    while True:
        print(f'\nБот по отправке схожих ключевых кадров ждет {minut} минут(ы) ...')
        sleep (int(60*minut))
        print(f'\nСодержание таблиц в БД...')
        await sender.Db.print_data('diff')
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
                              
        # нет kframes
        rows_notkfram = await sender.rows_notkframes()
        if rows_notkfram: 
            for row in rows_notkfram:
                msg = (f'Ключевых кадров для сравнения не определили. \n'
                       f'Либо уменьшите порог определения ключевых кадров либо этих кадров вообще нет' )
                messege = await sender.send_msg(row, msg)
                if not messege:
                    print(f'\n[Sender main] Не отправили пользователю сообщение')
                else: 
                    # отмечаем в таблице diff факт отправки ответа пользователю
                    diction = {'sender_user':'sender'}
                    if not await sender.Db.update_table_date_chatid(['diff'], row.date_message, row.chat_id, diction):
                        print(f'\nERROR [Sender main] не отметили в таблице отправку cообщения: {messege}')
        
        # нет similar_pair
        rows_notsimilars = await sender.rows_notsimilar()
        if rows_notsimilars:
            for row in rows_notsimilars:
                threshold_kframes=row.threshold_kframes
                hash_factor=row.hash_factor
                num_kframe_1=row.num_kframe_1
                num_kframe_2=row.num_kframe_2
                msg = (f'Схожих кадров не определили \n'
                       f'Порог ключевых кадров: [{threshold_kframes}]\n'
                       f'В первом видео ключевых кадров: [{num_kframe_1}]\n'
                       f'Во втором видео ключевых кадров: [{num_kframe_2}]\n'
                       f'Порог схожести кадров: [{hash_factor}]\n'
                       f'Либо увеличьте порог определения схожих кадров либо этих кадров вообще нет' )
                messege = await sender.send_msg(row, msg)
                if not messege:
                    print(f'\n[Sender main] Не отправили пользователю сообщение')
                else:
                    # отмечаем в таблице diff факт отправки ответа пользователю
                    diction = {'sender_user':'sender'}
                    if not await sender.Db.update_table_date_chatid(['diff'], row.date_message, row.chat_id, diction):
                        print(f'\nERROR [Sender main] не отметили в таблице отправку cообщения: {messege}')
#
if __name__ == "__main__":
    asyncio.run(main())




