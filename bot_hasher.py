#!/usr/bin/env python3 
#
from time import sleep, time
import datetime
from PIL import Image

from concurrent.futures import ThreadPoolExecutor
from sqlalchemy.engine.result import Row
import os, sys, asyncio, logging
from moviepy.editor import VideoFileClip, AudioFileClip
# from moviepy.editor import VideoFileClip, concatenate_videoclips, TextClip 
from moviepy.video.fx import all as vfx
import cv2
import numpy as np
from videomaker.comparator import VidCompar
from bot_env.mod_log import Logger
from data_base.base_db import BaseDB
#
#
#
class HasherVid:
    """Modul for hasher video"""
    countInstance=0
    #
    def __init__(self,
                 folder_video,
                 folder_kframes, 
                 log_file='hashervid_log.txt', 
                 log_level=logging.DEBUG,
                 hash_length_factor=0.25, # множитель (0.3*len(hash)) для определения порога расстояния Хэминга  
                 threshold_keyframes=0.25, # порог (больше порог, меньше кадров) для гистограммы ключевых кадров (0-1)
                 withoutlogo=False,
                #  max_download=3,
                 ):
        HasherVid.countInstance += 1
        self.countInstance = HasherVid.countInstance
        self.withoutlogo=withoutlogo
        # Logger
        self.Logger = Logger(log_file=log_file, log_level=log_level)
        self.Db = BaseDB(logger=self.Logger)
        self.save_file_path=os.path.join(sys.path[0], folder_video)
        self.path_save_keyframe=os.path.join(sys.path[0], folder_video, folder_kframes)
        self.Cmp=VidCompar(save_file_path=self.save_file_path,
                           path_save_keyframe=self.path_save_keyframe,
                           hash_length_factor=hash_length_factor, 
                           threshold_keyframes=threshold_keyframes,
                           withoutlogo=withoutlogo)
        # self.max_download = max_download
        self.days_del=2
        self.time_del = 24 * 60 * 60 * self.days_del #  
        # self.square_size=190
        self._create_save_directory()
        self._print()
    #
    # выводим № объекта
    def _print(self):
        print(f'[HasherVid] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'[HasherVid] countInstance: [{self.countInstance}]')
#
    # проверка директории для фрагментов видео
    def _create_save_directory(self, path: str = None):
        """
        Создает директорию для хранения video и ключевых кадров, 
        если она не существует
        """
        if not path:
            path=self.save_file_path
        if not os.path.exists(path):
            os.makedirs(path)
    #
    # асинхронная обертка для безопасного выполнения методов
    # async def safe_execute(self, coroutine: Callable[..., Coroutine[Any, Any, T]]) -> T:
    async def safe_await_execute(self, coroutine, name_func: str = None):
        try:
            # print(f'\n***HasherVid safe_await_execute: выполняем обертку ****')
            return await coroutine
        except Exception as eR:
            print(f'\nERROR[HasherVid {name_func}] ERROR: {eR}') 
            self.Logger.log_info(f'\nERROR[HasherVid {name_func}] ERROR: {eR}') 
            return None

    # синхронная обертка для безопасного выполнения методов
    def safe_execute(self, func, name_func: str = None):
        try:
            # print(f'\n***HasherVid safe_execute: выполняем обертку ****')
            return func
        except Exception as eR:
            print(f'\nERROR[HasherVid {name_func}] ERROR: {eR}') 
            self.Logger.log_info(f'\nERROR[HasherVid {name_func}] ERROR: {eR}') 
            return None

    # из 'diff' скачанные видео, но не сравнивали 
    async def rows4diff (self):
        # отбираем в таблице 'frag' скачанные, но не фрагментированные 
        try:
            async_results = await self.Db.read_data_two( 
                            name_table = 'diff',  
                            one_column_name = 'dnld', 
                            one_params_status = 'dnlded',
                            two_column_name = 'in_work', 
                            two_params_status = 'not_diff',
                                                        )
        except Exception as eR:
            print(f"\n[HasherVid rows4diff] Не удалось прочитать таблицу diff: {eR}")
            self.Logger.log_info(f"\n[HasherVid rows4diff] Не удалось прочитать таблицу diff: {eR}")
            return None
        rows = async_results.fetchall()
        if not rows: return None
        return rows
            #
    # удаляет все файлы, которые старше одного дня 
    async def delete_old_files(self):
        # множество имен файлов, которые находятся на диске
        set_nfile_dir = set(os.listdir(self.save_file_path))
        # текущее время
        current_time = datetime.datetime.now().timestamp()
        for name_file in set_nfile_dir:
            full_path = os.path.join(self.save_file_path, name_file)
            # время последнего изменения файла
            file_mod_time = os.path.getmtime(full_path)
            # если файл старше self.time_del
            if current_time - file_mod_time > self.time_del:
                try:
                    os.remove(full_path)
                    print(f"\n[Hashervid delete_old_files] Файл {full_path} успешно удалён.") 
                    self.Logger.log_info(f"[hashervid delete_old_files] Файл {full_path} успешно удалён.") 
                except Exception as eR:
                    print(f"\n[Hashervid delete_old_files] Не удалось удалить файл {full_path}: {eR}")
                    self.Logger.log_info(f'\nERROR [Hashervid delete_old_files] ERROR: {eR}')

#
# MAIN **************************
async def main():
    print(f'\n**************************************************************************')
    print(f'\nБот готов сравнивать видео')
    hasher=HasherVid(folder_video='diff_video', folder_kframes='keyframes') 
    minut=1
    while True:
        print(f'\nБот по сравнению видео ждет {minut} минут(ы) ...')
        sleep (int(60*minut))
        print(f'\nСодержание таблиц в БД...')
        await hasher.Db.print_data('diff')
        # удаляем все файлы, которые старше...
        await hasher.delete_old_files()

        # формируем список сравнений из таблицы diff
        rows = await hasher.safe_await_execute(hasher.rows4diff(), '[HasherVid main] hasher.rows4diff') 
        if not rows: continue
        # точка входа сравнения файлов 
        # надо будет добавить параллельное выполнение
        for row in rows:
            path2file = await hasher.Cmp.compar_vid_hash(row)
            if not path2file:
                print(f'\n[HasherVid compar_vid_hash] Не записали на диск схожие кадры')


if __name__ == "__main__":
    asyncio.run(main())


