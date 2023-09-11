#!/usr/bin/env python3 
#
from time import sleep, time
import datetime
from PIL import Image
import argparse
from argparse import ArgumentParser
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
                 folder_video= 'diff_video',
                 folder_kframes = 'diff_kframes',
                 log_file='hashervid_log.md', 
                 log_level=logging.DEBUG,
                 hash_factor=0.2, # множитель (0.n*len(hash)) для определения порога расстояния Хэминга  
                 threshold_keyframes=0.3, # порог (больше порог, меньше кадров) для гистограммы ключевых кадров (0-1)
                 withoutlogo=False,
                 logo_size=180,
                 ):
        HasherVid.countInstance += 1
        self.countInstance = HasherVid.countInstance
        #
        self.log_file=log_file
        self.log_level=log_level
        self.folder_video=folder_video
        self.folder_kframes=folder_kframes
        self.hash_factor=hash_factor
        self.threshold_keyframes=threshold_keyframes
        self.withoutlogo=withoutlogo
        self.logo_size=logo_size
        
        # Разбор аргументов
        self._arg_parser()
        
        # Logger
        self.Logger = Logger(self.log_file, self.log_level)
        self.Db = BaseDB(logger=self.Logger)
        #
        self.save_file_path=os.path.join(sys.path[0], self.folder_video)
        self.path_save_keyframe=os.path.join(sys.path[0], self.folder_kframes)
        # создаем класс VidCompar
        self.Cmp=self._cmp_def()
        self.days_del=2
        self.time_del = 24 * 60 * 60 * self.days_del #  
        self._create_save_directory()
        self._print()
    #
    # создаем класс VidCompar
    def _cmp_def(self):
        return VidCompar(
                        save_file_path=self.save_file_path,
                        path_save_keyframe=self.path_save_keyframe,
                        log_file=self.log_file,
                        hash_factor=self.hash_factor, 
                        threshold_keyframes=self.threshold_keyframes,
                        withoutlogo=self.withoutlogo,
                        logo_size=self.logo_size
                            )

    # выводим № объекта
    def _print(self):
        print(f'\n[HasherVid] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'\n[HasherVid] countInstance: [{self.countInstance}]\n')
        print(f'Аргументы:\n'
              f'log_file: {self.log_file}\n'
              f'log_level: {self.log_level}\n'
              f'folder_video: {self.folder_video}\n'
              f'folder_kframes: {self.folder_kframes}\n'
              f'hash_factor: {self.hash_factor}\n'
              f'threshold_keyframes: {self.threshold_keyframes}\n'
              f'logo_size: {self.logo_size}\n'
              f'withoutlogo: {self.withoutlogo}\n'
              )
#
    # добавление аргументов строки
    def _arg_added(self, parser: ArgumentParser):
        # Добавление аргументов
        parser.add_argument('-v', '--folder_video', type=str, help='Папка для видео')
        parser.add_argument('-k', '--folder_kframes', type=str, help='Папка для схожих кадров')
        parser.add_argument('-f', '--log_file', type=str, help='Имя журнала логгирования')
        parser.add_argument('-l', '--log_level', type=str, help='Уровень логгирования')
        # множитель (0.n*len(hash)) для определения порога расстояния Хэминга  
        parser.add_argument('-m', '--hash_factor', type=float, help='Множитель порога')
        # порог (больше порог, меньше кадров) для гистограммы ключевых кадров (0-1)
        parser.add_argument('-t', '--threshold_keyframes', type=float, help='Порог ключевых кадров')
        parser.add_argument('-z', '--logo_size', type=int, help='Cторона квадрата для удаления лого')
        parser.add_argument('--withoutlogo', action='store_true', help='Удаляем лого')

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
        #
        if args.folder_video: self.folder_video=args.folder_video
        if args.folder_kframes: self.folder_kframes=args.folder_kframes
        if args.hash_factor: self.hash_factor=args.hash_factor
        if args.threshold_keyframes: self.threshold_keyframes=args.threshold_keyframes
        if args.logo_size: self.logo_size=args.logo_size
        if args.withoutlogo: self.withoutlogo=True
    
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
    hasher=HasherVid() 
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


