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
                 log_file='hashervid_log.txt', 
                 log_level=logging.DEBUG,
                 threshold=20,
                 threshold_keyframes=0.3,
                #  max_download=3,
                 ):
        HasherVid.countInstance += 1
        self.countInstance = HasherVid.countInstance
        # Logger
        self.Logger = Logger(log_file=log_file, log_level=log_level)
        self.Db = BaseDB(logger=self.Logger)
        self.Cmp=VidCompar(threshold=threshold, threshold_keyframes=threshold_keyframes)
        # self.max_download = max_download
        self.days_del=2
        self.time_del = 24 * 60 * 60 * self.days_del #  
        # self.square_size=190
        self.save_file_path=os.path.join(sys.path[0], 'diff_video')
        self.path_save_keyframe=os.path.join(sys.path[0], 'diff_video', 'keyframes')
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

    # точка входа сравнения файлов
    # надо будет добавить параллельное выполнение
    async def comparator_files(self, rows: Row, withoutlogo=True):
        result=[]
        for row in rows:
            date=str(row.date_message)
            path_first=row.path_file_first
            path_second=row.path_file_second
            
            # удаляем лого
            if withoutlogo:
                path_first = self.Cmp.del_logo(path_first)
                if not path_first:
                    print(f'\n[VidCompar is_video_unique] лого не удалили c первого видео')
                    return None
                path_second = self.Cmp.del_logo(path_second)
                if not path_second:
                    print(f'\n[VidCompar is_video_unique] лого не удалили со второго видео')
                    return None
            else: print(f'\n[VidCompar is_video_unique] Лого не будем удалять\n')
            
            # сравниваем видео
            similars = self.Cmp.is_video_unique(path_first, path_second)
            if not similars:
                print(f'\n[HasherVid comparator_files] Похожих ключевых файлов не обнаружено\n'
                      f'Это совсем разные файлы')
                return None
            # создаем и проверяем место сохранения ключевых схожих кадров
            path2file = os.path.join(self.path_save_keyframe, date.replace(' ', '_').replace(':', '_'))
            self._create_save_directory(path2file)
            #
            similar_pairs, similar_frames = similars
            # сохраняем на диск похожие ключевые кадры
            for index, similar_frame in enumerate(similar_frames):
                #
                frame_1, frame_2 = similar_frame
                image_pil_1 = Image.fromarray(np.uint8(frame_1), 'RGB')
                image_pil_2 = Image.fromarray(np.uint8(frame_2), 'RGB')
                full_name_file_1 = os.path.join(path2file, 'keyframe_'+str(index)+'_1.png') 
                full_name_file_2 = os.path.join(path2file, 'keyframe_'+str(index)+'_2.png') 
                # сохранение в формате PNG
                self.safe_execute(image_pil_1.save(full_name_file_1), 'comparator_files')
                self.safe_execute(image_pil_2.save(full_name_file_2), 'comparator_files')
            if not await self.Db.update_table_date_chatid(['diff'], date, row.chat_id, {'in_work':'diff'}):
                print(f'\nERROR [HasherVid comparator_files] отметить в таблице сравнение файлов \n{path_first} \n'
                      f'{path_second} не получилось')
                continue
            result.append((path_first, path_second))
        return result  


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
        try:
            rows = await hasher.rows4diff() 
        except Exception as eR:
            print(f'\nERROR[HasherVid main] ERROR: {eR}') 
            hasher.Logger.log_info(f'\nERROR[HasherVid main] ERROR: {eR}') 
        if not rows: continue
        # есть задачи, убираем лого и делаем сравнение
        diff = await hasher.comparator_files(rows, withoutlogo=False)
        if not diff: 
            print(f'\n[HasherVid main] ERROR не сделали сравнение ...')
            hasher.Logger.log_info(f'\n[HasherVid main] ERROR не сделали сравнение ...')

if __name__ == "__main__":
    asyncio.run(main())


