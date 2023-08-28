#!/usr/bin/env python3
#
from time import sleep, time
import asyncio, os, sys, logging
from sqlalchemy.exc import SQLAlchemyError
import multiprocessing.dummy as multiprocessing
import subprocess
import shlex
from yt_dlp import YoutubeDL
from concurrent.futures import ProcessPoolExecutor 
from bot_env.mod_log import Logger
from data_base.base_db import BaseDB
#
#
class Dnld:
    """Module for downloading video file from youtube"""
    countInstance=0
    #
    def __init__(self, 
                 log_file='dnld_log.txt', 
                 log_level=logging.DEBUG,
                 max_download=5,
                 ):
        Dnld.countInstance += 1
        self.countInstance = Dnld.countInstance
        # Logger
        self.Logger = Logger(log_file=log_file, log_level=log_level)
        self.Db = BaseDB(logger=self.Logger)
        self.youtube_url_prefix = 'https://www.youtube.com/watch?v='
        self.max_download = max_download
        self.downloaded_file_path = os.path.join(sys.path[0], 'video_source')
        self._print()
        self._create_dnld_directory()

    # выводим № объекта
    def _print(self):
        print(f'[Dnld] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'[Dnld] countInstance: [{self.countInstance}]')
        #     
    def _create_dnld_directory(self):
        """
        Создает директорию для хранения скачанных видеофайлов, 
        если она не существует
        """
        # dnld_dir = os.path.dirname(self.downloaded_file_path)
        if not os.path.exists(self.downloaded_file_path):
            os.makedirs(self.downloaded_file_path)
    #
    # обертка для безопасного выполнения методов
    # async def safe_execute(self, coroutine: Callable[..., Coroutine[Any, Any, T]]) -> T:
    async def safe_execute(self, coroutine, name_func: str = None):
        try:
            print(f'\n***Dnld safe_execute: {name_func} выполняем обертку ****')
            return await coroutine
        except Exception as eR:
            print(f'\nERROR[Dnld {name_func}] ERROR: {eR}') 
            self.Logger.log_info(f'\nERROR[Dnld {name_func}] ERROR: {eR}') 
            return None

    # читаем ссылки из таблицы task, которые не закачаны
    async def vid_from_task(self):
        # rows type: <class 'sqlalchemy.engine.cursor.CursorResult'>
        cursor_result = await self.safe_execute(self.Db.read_data_one('task', 'in_work_download', 'not_download'), 'vid_from_task')
        if not cursor_result: return set()
        rows=cursor_result.fetchall()
        return {str(row.video_id) for row in rows}

    # читаем ссылки из таблицы dnld, которые уже закачаны
    async def vid_dnlded(self):
        cursor_result = await self.safe_execute(self.Db.read_data_one('dnld', 'in_work_download', 'downloaded'), 'vid_dnlded')
        if not cursor_result: return None
        rows=cursor_result.fetchall()
        return {str(row.video_id) for row in rows}
    #
    # формируем список video_id для записи в таблицу dnld
    async def update_tasks(self):
        # множество неотработанных закачек в таблице task
        vid_from_task=set() 
        vid_from_task = await self.safe_execute(self.vid_from_task(), 'update_tasks') 
        if not vid_from_task: return set()
        # все данные из таблицы dnld
        cursor_result = await self.safe_execute(self.Db.data_table('dnld'), 'update_tasks')
        if not cursor_result: return set()
        rows=cursor_result.fetchall()
        # множество из таблицы dnld
        vid_from_dnld=set() 
        vid_from_dnld={str(row.video_id) for row in rows}
        return vid_from_task - vid_from_dnld
        #
    # заполняем таблицу dnld 
    async def fill_dnld(self, vid4dnld: set):
        data={}
        diction4db={}
        for video_id in vid4dnld:
            diction4db['time_task']=int(time())
            diction4db['video_id']=video_id
            diction4db['url_video_y2b']=f'https://www.youtube.com/watch?v={video_id}'
            diction4db['in_work_download']='not_download'
            diction4db['path_download']='not_path'
            # записываем в таблицу dnld
            if not await self.safe_execute(self.Db.insert_data('dnld', diction4db), 'fill_dnld'):
                print(f'\n[Dnld fill_dnld] Ошибка записи в dnld новой строки {diction4db}')
            data[video_id]=diction4db
            diction4db={}
        return data

    # обновляем задачи в таблице dnld
    async def update_dnld(self):
        # новые задачи из таблицы task
        vid4dnld = await self.safe_execute(self.update_tasks(), 'update_dnld')
        # дописываем новые задачи в таблицу dnld
        if not vid4dnld: return None
        return await self.safe_execute(self.fill_dnld(vid4dnld), 'update_dnld')

    # из таблицы dnld создаем словарь vid-url для закачек
    async def vid4dnld(self):
        # ссылки из таблицы dnld, которые не закачаны
        cursor_result = await self.safe_execute(self.Db.read_data_one('dnld', 'in_work_download', 'not_download'), 'vid4dnld')
        if not cursor_result: return None
        rows=cursor_result.fetchall()
        return {str(row.video_id): f'{self.youtube_url_prefix}{str(row.video_id)}' for row in rows}

    # создаем список команд для yt_dlp
    def make_arg(self, vid_url: dict):
        arg_list=[]
        for vid, url in vid_url.items():
            name_file=f'{vid}.%(resolution)s.%(fps)s.%(ext)s'
            path_save_video=os.path.join(sys.path[0], 'video_source', name_file)
            ydl_opts = {
                'format': 'bv[ext=mp4][height=1080]+ba[ext=m4a]/bestaudio',
                'merge_output_format': 'mp4',
                'socket_timeout': 999999,
                'no_playlist': True,
                'outtmpl': path_save_video,
                # 'progress_hooks': [self.progress_hook],
                'nooverwrites': True,
                        }
            arg_list.append((ydl_opts, url, vid))
        return arg_list
    # 
    def progress_hook(self, d):
        """
            'status': состояние загрузки, может быть 'downloading', 'finished', или 'error'.
            'filename': имя текущего загружаемого файла.
            'tmpfilename': временное имя файла (если используется).
            'downloaded_bytes': количество уже загруженных байт.
            'total_bytes': общее количество байт в файле (если известно).
            'speed': текущая скорость загрузки в байтах в секунду.
            'eta': оставшееся время до завершения загрузки в секундах.
            '_percent_str': процент завершения загрузки в строковом формате, например, '50.3%'.
            'elapsed': время, прошедшее с начала загрузки.
        """
        if d['status'] == 'downloading':
            print(f"[Dnld progress_hook] Downloading {d['filename']} - {d['_percent_str']} complete")
        if d['status'] == 'finished':
            # self.downloaded_file_path = f"Downloaded: {d['filename']}"
            print(f"[Dnld progress_hook] Finished: {d['filename']}")
        if d['status'] == 'error':
            # self.downloaded_file_path = f"Downloaded: {d['filename']}"
            print(f"[Dnld progress_hook] ERROR downloaded_bytes: {d['downloaded_bytes']}")
# 
    # запускаем скачивание yt_dlp
    def run_dlp(self, arg: tuple):
        params, url, vid = arg
        try:
            with YoutubeDL(params) as ydl:
                ydl.download([url])
                outtmpl=params['outtmpl']
                path_save_video=outtmpl['default']
                return os.path.dirname(path_save_video), vid
        except Exception as eR:
            error_message = f'\nERROR[Dnld run_dlp] ERROR: {eR}'
            print(error_message) 
            self.Logger.log_info(error_message) 
            return error_message
    #    
    # проверяем скачал ли run_dlp файл и отмечаем в task и dnld
    async def check_result_dlp(self, path: str, vid: str):
        list_file_dir=[(vid, fname) for fname in os.listdir(path) if vid in fname]
        if not list_file_dir: return None
        if len(list_file_dir) == 1:
            vid, fname = list_file_dir[0]
        else:
            print("\nERROR [Dnld dnld_file] Ошибка: Найдено больше одного файла или ни одного")
        # Находим в таблице 'dnld' строки с vid и записываем словарь значений
        diction={'in_work_download': 'downloaded', 
                'path_download': os.path.join(path, fname),
                'update_time': int(time())}
        if not await self.Db.update_table_vid(['dnld', 'task'], vid, diction):
            print(f'\nERROR [Dnld dnld_file] обновить dnld-task [in_work_download: downloaded] не получилось')
            return None
        return vid, fname
    #
    # если есть в таблице dnld, но нет на диске, скачиваем 
    async def check_dnld_file(self):
        # список объектов <class 'sqlalchemy.engine.row.Row'>
        async_results = await self.safe_execute(self.Db.read_data_one('dnld', 'in_work_download', 'downloaded'), 'check_dnld_file')
        if not async_results: return None
        rows=async_results.fetchall()
        # video_id отмеченные в dnld как скачанные
        vid_dnlded = {row.video_id for row in rows}
        # имена файлов, которые находятся на диске 
        file_in_dir=set(os.listdir(self.downloaded_file_path))
        print(f'\n[Dnld: check_dnld_file] На диске путь: [{self.downloaded_file_path}] \n'
              f'                        Хранится [{len(file_in_dir)}] файлов')
        # формируем множество video_id, которые есть на диске
        vid_in_disk=set()
        for file_name in file_in_dir:
            for vid in vid_dnlded:
                if vid in file_name:
                    vid_in_disk.add(vid)
        # из множества имен в БД вычитаем имена, которые есть на диске
        vid4dnld=vid_dnlded-vid_in_disk
        if not vid4dnld: return None
        # есть файлы, которые не скачаны
        print(f'\n[Dnld check_dnld_file] Надо закачать {vid4dnld}')
        # вносим изменения в dnld, ссылки, которые по факту не закачаны
        diction={'in_work_download': 'not_download', 
                 'path_download': 'not_path',
                 'update_time': int(time())}
        for vid in vid4dnld:
            if not await self.Db.update_table_vid(['dnld', 'task'], vid, diction): 
                print(f'\nERROR [Dnld check_dnld_file] в dnld-task обновить {vid} не получилось {diction}')
                return None
        return vid4dnld
    #
    # удаляем временные файлы после скачивания dlp
    async def delete_non_mp4_files(self):
        #  множество имен файлов, которые находятся на диске 
        set_file_dir=set(os.listdir(self.downloaded_file_path))
        non_mp4_files = [file for file in set_file_dir if not file.endswith('.mp4')]
        if not non_mp4_files: return None
        for file in non_mp4_files:
            full_path=os.path.join(self.downloaded_file_path, file)
            try:
                os.remove(full_path)
                self.Logger.log_info(f"[Dnld delete_non_mp4_files] Файл {full_path} успешно удалён.") 
                return non_mp4_files
            except Exception as eR:
                print(f"[Dnld delete_non_mp4_files] Не удалось удалить файл {full_path}: {eR}")
                self.Logger.log_info(f'\nERROR[Dnld delete_non_mp4_files] ERROR: {eR}') 
                return None

    # скачиваем файлы и проверяем их на диске
    async def dlp(self, command_list: list):
        try:
            with multiprocessing.Pool(self.max_download) as pool:
                # results = pool.map(self.run_dlp, command_list)
                return pool.map(self.run_dlp, command_list)
                # return results
        except Exception as eR:
            print(f'\nERROR[Dnld dlp] ERROR: {eR}') 
            self.Logger.log_info(f'\nERROR[Dnld dlp] ERROR: {eR}') 
            return None


# MAIN **************************
async def main():
    print(f'\n**************************************************************************')
    print(f'\nБот по скачиванию начал мониторить задачи...\n')
    dnld=Dnld() 
    minut=0.2
    while True:
        #
        print(f'\nБот по скачиванию ждет {minut} минут(ы) ...')
        sleep (int(60*minut))
        print(f'\nСодержание таблиц в БД...')
        await dnld.Db.print_data('task')
        await dnld.Db.print_data('dnld')
        
        try:
            # обновляем задачи таблицы dnld
            await dnld.update_dnld()
            # формируем список-закачек из таблицы dnld
            vid_url = await dnld.vid4dnld()
        except Exception as eR:
            print(f'\nERROR[Dnld main] ERROR: {eR}') 
            dnld.Logger.log_info(f'\nERROR[Dnld main] ERROR: {eR}') 
        
        # проверка есть ли видео для закачивания
        if not vid_url: 
            # сверяем БД и файлы на диске, удаляем временные файлы dlp
            await dnld.check_dnld_file()
            # await dnld.check_db_disk()
            await dnld.delete_non_mp4_files()            
            continue
        
        # создаем список команд для yt_dlp
        command_list=dnld.make_arg(vid_url)
        # скачиваем файлы 
        results = await dnld.dlp(command_list)
        if not results:
            print(f"\nERROR [Dnld main] Ошибка при скачивании dnld.dlp(vid_url)")
            continue
        # проверяем скачанные файлы на диске
        for result in results:
            # проверяем скачал ли run_dlp файл на диск
            # print(f'\n[Dnld main] result: {result}')
            if not await dnld.check_result_dlp(*result):
                print(f"\nERROR [Dnld main] result: {result} update full_parth_vid is NONE")
        await dnld.check_dnld_file()
        await dnld.delete_non_mp4_files()


if __name__ == "__main__":
    asyncio.run(main())



    #
    # если есть на диске, но нет в task, отмечаем как скачаный 
    # async def check_db_disk (self):
    #     async_results = await self.Db.read_data_one(
    #                         name_table = 'task', 
    #                         column_name = 'in_work_download', 
    #                         params_status = 'not_download')
    #     # список объектов <class 'sqlalchemy.engine.row.Row'>
    #     if not async_results: return None
    #     rows=async_results.fetchall()
    #     # из task video_id (не скачанные)  
    #     vid4dnld = {row.video_id for row in rows}
    #     # print(f'\n[Dnld: check_db_disk] В таблице [dnld] не закачаны ссылки {vid4dnld}')
    #     # имена файлов, которые находятся на диске 
    #     file_dir=set(os.listdir(self.downloaded_file_path))
    #     print(f'\n[Dnld: check_db_disk] На диске закачано {len(file_dir)} файлов')
    #     # if len(file_dir)<30: 
    #     #     for i in file_dir: print(i)
    #     if len(file_dir) < 30: [print(i) for i in file_dir]
    #     # выбираем файлы из БД, которые скачаны, но не отмечены
    #     set_db_disk=[]
    #     for file_name in set_file_dir:
    #         for vid in vid4dnld:
    #             if vid in file_name:
    #                 set_db_disk.append((vid, file_name))
    #     # из множества имен в БД вычитаем имена, которые есть на диске
    #     # set_vid4dnld=vid_dnld-set_in_disk
    #     if not set_db_disk:
    #         # print(f'\n[Dnld check_db_disk] dnld совпадает с файлами на диске')
    #         return None
    #     # есть файлы, которые не отмечены
    #     print(f'\n[Dnld check_db_disk] dnld не совпадает с файлами на диске. \nНадо отметить {set_db_disk}')
    #     # вносим изменения в dnld, ссылки, которые по факту не закачаны
    #     for vid, file_name in set_db_disk:
    #         diction={'in_work_download': 'downloaded', 
    #                 'path_download': os.path.join(self.downloaded_file_path, file_name),
    #                 'update_time': int(time()),
    #                 }
    #         if not await self.Db.update_table_vid(['dnld', 'task'], vid, diction): 
    #             print(f'\nERROR [Dnld check_dnld_file] в dnld-task обновить {vid} не получилось {diction}')
    #         # if not await self.Db.update_table_vid('task', vid, diction):
    #         #     print(f'\nERROR [Dnld check_dnld_file] в task обновить {vid} не получилось {diction}')
    #     return set_db_disk
