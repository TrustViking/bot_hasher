#!/usr/bin/env python3 
#
from pyrogram import Client
from time import sleep
import os, asyncio, logging
from bot_env.mod_log import Logger
from data_base.base_db import BaseDB

class Snd:
    """Modul for sended video"""
    countInstance=0
    #
    def __init__(self, 
                 log_file='snd_log.txt', 
                 log_level=logging.DEBUG,
                 ):
        Snd.countInstance += 1
        self.countInstance = Snd.countInstance
        # Logger
        self.Logger = Logger(log_file=log_file, log_level=log_level)
        self.Db = BaseDB(logger=self.Logger)
        #
        self.api_id = os.getenv('TELEGRAM_API_ID')
        self.api_hash = os.getenv('TELEGRAM_API_HASH')
        self.bot_token = os.getenv('TELEGRAM_TOKEN')
        self.group = os.getenv('TELEGRAM_GROUP')
        #
        self._print()
    #
    # выводим № объекта
    def _print(self):
        print(f'\n[Snd] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'\n[Snd] countInstance: [{self.countInstance}]')

    # Функция, которая будет вызываться для отображения прогресса
    def progress_bar(self, current, total):
        print(f"\rПрогресс отправки: {current * 100 / total:.1f}%", end="", flush=True)

    # обертка для безопасного выполнения методов
    # async def safe_execute(self, coroutine: Callable[..., Coroutine[Any, Any, T]]) -> T:
    async def safe_execute(self, coroutine, name_func: str = None):
        try:
            print(f'\n***Dnld safe_execute: выполняем обертку ****')
            return await coroutine
        except Exception as eR:
            print(f'\nERROR[Dnld {name_func}] ERROR: {eR}') 
            self.Logger.log_info(f'\nERROR[Dnld {name_func}] ERROR: {eR}') 
            return None

    # отправляем фрагмент в группу Snipper 
    async def send2group(self, path: str):
        file_id=''
        message=None
        async with Client('F16', api_id=self.api_id, api_hash=self.api_hash) as app:
            # Отправляем видео в группу Snipper 
            message = await app.send_video(chat_id=self.group, 
                                           video=path,
                                           caption=path, 
                                           progress=self.progress_bar)
        if not message:
            print(f"\nERROR [Snd: send2group] Видео не отправлено! message: {message}")
            return None
        # Получаем file_id видео
        file_id = message.video.file_id
        if file_id:
            print(f"\n[Snd: send2group] id: {file_id} отправлено в группу")
            return file_id
        else: 
            print(f"\nERROR [Snd: send2group] Видео не отправлено! file_id: {file_id}")
            return None
    

    # получаем список отработанных фрагментов, 
    # отправляем их пользователям и записываем в БД
    async def send_frag_group(self):
        try:
            # получаем список отработанных, но не отправленных фрагментов
            cursor_result = await self.Db.read_data_two(name_table='frag',
                                                        one_column_name='in_work_frag', 
                                                        one_params_status='fraged',
                                                        two_column_name='send', 
                                                        two_params_status='not_send',
                                                        )
        except Exception as eR:
            print(f'\nERROR[Snd send_frag_group read_data_two] ERROR: {eR}')
            self.Logger.log_info(f'\nERROR[Snd send_frag_group read_data_two] ERROR: {eR}')
            return None
        if not cursor_result: 
            # print(f'\n[Snd: send_frag_group] В таблице [frag] нет фрагментов для отправки cursor_result: {cursor_result}')
            return None
        # список объектов <class 'sqlalchemy.engine.row.Row'>
        rows=cursor_result.fetchall()
        if not rows: 
            # print(f'\n[Snd: send_frag_group] В таблице [frag] нет фрагментов для отправки rows: {rows}')
            return None
        
        # # список путей к фрагментам на диске
        path_frags = [str(row.path_frag) for row in rows]
        list_sended=[]
        # отправляем пользователям фрагменты
        for path_frag in path_frags:
            try:
                # отправляем в группу фрагмент видеофайла
                file_id = await self.send2group(path_frag)
            except Exception as eR:
                print(f'\nERROR[Snd send_frag_users send_frag] ERROR: {eR}')
                self.Logger.log_info(f'\nERROR[Snd send_frag_users send_frag] ERROR: {eR}')
                return None
            if not file_id:
                print(f'\n[Snd send_frag_users send_frag] не отправили в группу видео')
                return None
            # записываем в БД: diction = {'send': 'sended'}
            diction = {'send': 'sended', 
                        'send2group_file_id': file_id}
            if not await self.Db.update_table_path(['task', 'frag'], path_frag, diction):
                print(f'\n[Snd send_frag_users update_table] не записали в таблицы [task, frag] отметки об отправке')
                return None
            list_sended.append(file_id) 
        return list_sended

#
# MAIN **************************
async def main():
    print(f'\n**************************************************************************')
    print(f'\nБот готов отправлять видео пользователям')
    snd=Snd() 
    #
    minut=0.4
    while True:
        #
        print(f'\nБот по отправке ссылок на видео ждет {minut} минут(ы) ...')
        sleep (int(60*minut))
        # получаем список отработанных фрагментов, 
        # отправляем их пользователям и записываем в БД
        sended = await snd.send_frag_group()
        if not sended:
            print(f'\n[Snd main] пока нечего отправить пользователям ...')
        else: 
            print(f'\n[Snd main] for user sended: {sended}')

        print(f'\nСодержание таблиц в БД...')
        await snd.Db.print_data('task')
        await snd.Db.print_data('frag') 


if __name__ == "__main__":
    asyncio.run(main())









