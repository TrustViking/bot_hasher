#!/usr/bin/env python3 
#
from time import sleep, time
import datetime
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy.engine.result import Row
import os, sys, asyncio, logging
from moviepy.editor import VideoFileClip, AudioFileClip
# from moviepy.editor import VideoFileClip, concatenate_videoclips, TextClip 
from moviepy.video.fx import all as vfx
import cv2
import numpy as np


from bot_env.mod_log import Logger
from data_base.base_db import BaseDB
#
#
#
class Mov:
    """Modul for video composition"""
    countInstance=0
    #
    def __init__(self, 
                 log_file='mov_log.txt', 
                 log_level=logging.DEBUG,
                 max_download=3,
                 ):
        Mov.countInstance += 1
        self.countInstance = Mov.countInstance
        # Logger
        self.Logger = Logger(log_file=log_file, log_level=log_level)
        self.Db = BaseDB(logger=self.Logger)
        self.max_download = max_download
        self.save_file_path = os.path.join(sys.path[0], 'video_frag')
        self.time_del = 24 * 60 * 60
        self.square_size=190
        self._create_mov_dir()
        self._print()
        
    #
    # выводим № объекта
    def _print(self):
        print(f'[Mov] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'[Mov] countInstance: [{self.countInstance}]')
#
    # проверка директории для фрагментов видео
    def _create_mov_dir(self):
        """
        Создает директорию для хранения скачанных видеофайлов, 
        если она не существует
        """
        if not os.path.exists(self.save_file_path):
            os.makedirs(self.save_file_path)
    #
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
    
    # метод проверки строки из таблицы frag на повторяемость
    # сверяет date_message и user_id, надо переделать, 
    # когда будет создаваться несколько вариантов фрагментов
    # <class 'sqlalchemy.engine.row.Row'>
    async def check_table_frag (self, row: Row):
            # print(f'\n*******[Mov check_table_frag] type(row): {type(row)}')
            try:
                results_check = await self.Db.read_data_two ( 
                                        name_table='frag', 
                                        one_column_name='date_message', 
                                        one_params_status=row.date_message,
                                        two_column_name='user_id', 
                                        two_params_status=row.user_id,
                                        )
            except Exception as eR:
                print(f'\nERROR[Mov check_table_frag] ERROR: {eR}') 
                self.Logger.log_info(f'\nERROR[Mov check_table_frag] ERROR: {eR}') 
                return None
            row_check=results_check.fetchone()
            if row_check:
                if row.date_message==row_check.date_message and row.user_id==row_check.user_id:
                    print(f'\n[Mov: check_table_frag] В таблице [frag] повторяется задача: {row_check}\n'
                          f'date_message: {row.date_message} - check: {row_check.date_message}\n'
                          f'user_id: {row.user_id} - check {row_check.user_id}')
                    return None
                else: 
                    print(f'\n[Mov: check_table_frag] row_check: {row_check} \nrow: {row} ')
                    return None
            else: 
                return row

    # заполняем таблицу frag на базе task
    async def making_table_frag (self):
        # отбираем скачанные, но не отработанные ссылки в таблице 'task'
        try:
            async_results = await self.Db.read_data_two( 
                            name_table = 'task',  
                            one_column_name = 'in_work_download', 
                            one_params_status = 'downloaded',
                            two_column_name = 'in_work_frag', 
                            two_params_status = 'not_frag')
        except Exception as eR:
            print(f'\nERROR[Mov making_table_frag] ERROR: {eR}') 
            self.Logger.log_info(f'\nERROR[Mov making_table_frag] ERROR: {eR}') 
            return None
        # список объектов <class 'sqlalchemy.engine.row.Row'>
        rows=async_results.fetchall()
        if not rows: return None
        data=[] # список словарей переданных в таблицу frag
        for row in rows:
            # метод проверки строки из таблицы frag на повторяемость
            if not await self.check_table_frag(row): continue
            #
            # формируем имена фрагментов (скачанные и не фрагментированные) -> frag
            str_name=str(row.video_id + '_' + row.user_id + row.date_message)
            # print(f'\n[Mov: insert_frag] str_name: {str_name}')
            name_frag="".join(str_name.replace(":", "").replace(" ", "").replace("-", ""))
            #  записываем имя фрагмента в таблицу task          
            diction = {'name_frag': name_frag}
            if not await self.Db.update_table_date(['task'], row.date_message, row.user_id, diction):
                print(f'\n[Mov making_table_frag] не записали {name_frag} в таблицу [task]')
                continue
            # новые строки (задачи) -> frag 
            diction = {
                        'date_message': row.date_message,
                        'username': row.username,
                        'user_id': row.user_id,
                        'chat_id': row.chat_id,
                        'time_task': int(time()), 
                        'url_video_y2b': row.url_video_y2b,
                        'video_id': row.video_id,
                        'timestamp_start': row.timestamp_start,
                        'timestamp_end': row.timestamp_end,
                        'in_work_download': row.in_work_download,
                        'path_download': row.path_download,
                        'in_work_frag': 'not_frag',
                        'name_frag': name_frag,
                        'path_frag': 'not_frag',
                        'send': 'not_send',
                        'send2group_file_id': 'not_id',
                        'resend': 'not_resend',
                        'resend_file_id': 'not_id',
                        }
            # новые строки (задачи) -> frag 
            try:
                if not await self.Db.insert_data('frag', diction):
                    print(f'\n[Mov: making_table_frag] в таблицу [frag] не получилось записать данные: {diction}')
                    continue
            except Exception as eR:
                print(f'\nERROR[Mov making_table_frag insert_data] ERROR: {eR}') 
                self.Logger.log_info(f'\nERROR[Mov making_table_frag insert_data] ERROR: {eR}') 
                return None
            data.append(diction)
        return data # список записанных словарей в таблицу frag

    # task -> frag, frag -> rows 
    # из 'frag' скачанные строки, но не фрагментированные 
    async def rows4frag (self):
        # отбираем в таблице 'frag' скачанные, но не фрагментированные 
        try:
            async_results = await self.Db.read_data_two( 
                            name_table = 'frag',  
                            one_column_name = 'in_work_download', 
                            one_params_status = 'downloaded',
                            two_column_name = 'in_work_frag', 
                            two_params_status = 'not_frag',
                                                        )
        except Exception as eR:
            print(f"\n[Mov rows4frag read_data_two] Не удалось прочитать таблицу frag: {eR}")
            self.Logger.log_info(f"[Mov rows4frag read_data_two] Не удалось прочитать таблицу frag: {eR}")
            return None
        rows = async_results.fetchall()
        if not rows: return None
        return rows

    # вырезаем фрагмент по временным меткам 
    # в формате строки "чч:мм:сс" '01:03:05.35'
    def slice_frag4time (self, name_file: str,  path_file: str, t_start: str, t_end: str, ext = '.mp4'):
        save_full_path = os.path.join(self.save_file_path, name_file+ext)
        try:
            # Обрезка видео по временным меткам
            clip = VideoFileClip(filename=path_file).subclip(t_start, t_end)
            # frag = clip.subclip(t_start, t_end)
            clip.write_videofile(filename=save_full_path, bitrate="8000k", audio_bitrate="384k")
            # дубликат
            # clip.write_videofile(filename=save_full_path, bitrate="8000k", audio_bitrate="384k")
            clip.close()
            return save_full_path
        except Exception as eR:
            print(f"[Mov slice_frag4time] Не удалось создать фрагмент {path_file}: {eR}")
            self.Logger.log_info(f"[Mov slice_frag4time] Не удалось создать фрагмент {path_file}: {eR}")
            return None

    # убираем лого
    def  del_logo (self, full_path: str):
        # Отделение аудио и сохранение во временный файл
        with VideoFileClip(full_path) as video_clip:
            audio_clip = video_clip.audio
            full_path_audio = full_path + '.ogg'
            audio_clip.write_audiofile(full_path_audio)

        # Чтение в буфер 'video' из файла с помощью OpenCV
        video = cv2.VideoCapture(full_path)
        # данные видеоряда
        fps = int(video.get(cv2.CAP_PROP_FPS))
        fourcc = cv2.VideoWriter.fourcc(*'mp4v')
        frame_count = int(video.get(cv2.CAP_PROP_FRAME_COUNT))
        frame_size = (int(video.get(cv2.CAP_PROP_FRAME_WIDTH)), int(video.get(cv2.CAP_PROP_FRAME_HEIGHT)))
        width, height = frame_size
        print(f'\n[Mov del_logo] video: {video} \n'
              f'fps: {fps} \nfourcc: {fourcc} \n'
              f'frame_size: {frame_size} \nframe_count: {frame_count}')        
        
        # удаляем фраг с лого
        if video and fps and fourcc and frame_size and os.path.isfile(full_path):
            os.remove(full_path)
        else: print(f'\nERROR [Mov del_logo] video: {video} \nfps: {fps} \nfourcc: {fourcc} \nframe_size: {frame_size}')

        # Размер квадрата маски
        # square_size = 190 # px
        # Координаты верхнего левого угла квадрата маски
        x1, y1 = width-self.square_size, 0
        # Координаты нижнего правого угла квадрата маски
        x2, y2 = width, self.square_size
        full_path_noaudio = full_path+'_noaudio.mp4'
        # Определение выходного файла без лого, но и без звука
        out_file = cv2.VideoWriter(full_path_noaudio, fourcc, fps, frame_size)
        # чтение буфера video
        try:
            while video.isOpened():
                # type(frame): <class 'numpy.ndarray'>
                # frame.shape: (1080, 1920, 3)
                ret, frame = video.read()
                if not ret: break
                # Создание маски для области логотипа
                mask = np.zeros_like(frame[:, :, 0])
                mask[y1:y2, x1:x2] = 255
                # Применение инпейнтинга для удаления логотипа
                result = cv2.inpaint(frame, mask, inpaintRadius=15, flags=cv2.INPAINT_TELEA)
                out_file.write(result)
        except Exception as eR:
            print(f"\nERROR [Mov del_logo] ERROR инпейнтинг для удаления логотипа: {eR}")
            self.Logger.log_info(f"\nERROR [Mov del_logo] ERROR инпейнтинг для удаления логотипа: {eR}")
            return None
        finally:
                if video: video.release()
                if out_file: out_file.release()
        
        # склеиваем видео без лого и звука
        # загружаем аудио в MoviePy
        with AudioFileClip(full_path_audio) as audio_clip:
            # загружаем видео без лого в MoviePy
            with VideoFileClip(full_path_noaudio) as video_clip:
                # Сохраняем видео без лого + аудио
                video_clip = video_clip.set_audio(audio_clip)
                video_clip.write_videofile(filename=full_path, 
                                           bitrate="8000k", 
                                           audio_bitrate="384k")
                # удаляем фраг без лого и звука, а также файл звука
                if os.path.isfile(full_path_noaudio) and os.path.isfile(full_path_audio):
                    os.remove(full_path_noaudio)
                    os.remove(full_path_audio)
                else: 
                    print(f'\nERROR [Mov del_logo] нет файла full_path: {full_path} \nfull_path_audio: {full_path_audio}')
        print(f'\n[Mov del_logo] logo удалили')
        return full_path

        
    # фрагментация, убираем лого и сохраняем на диске
    async def making_fragments (self, rows):
        print(f'\n*******[Mov making_fragments] type(rows): {type(rows)}')
        # список кортежей с аргументами для нарезки по временным меткам 
        # исходя из полного пути скачанного исходника path_download
        timestamps=[(row.name_frag, row.path_download, row.timestamp_start, row.timestamp_end) for row in rows]
        date_messages = [row.date_message for row in rows]
        user_ids = [row.user_id for row in rows]
        path_frags=[] # список полных путей к фрагментам
        for timestamp, date_message, user_id in zip(timestamps, date_messages, user_ids):
            path_frag = self.slice_frag4time(*timestamp)
            if not path_frag: continue 
            # убираем лого из видео
            path_frag = self.del_logo(path_frag)
            if not path_frag: continue 
            # записываем в таблицы ['task', 'frag']
            diction = {'in_work_frag': 'fraged', 'path_frag': path_frag}
            if not await self.Db.update_table_date(['task', 'frag'], date_message, user_id, diction):
                print(f'\n[Mov: making_fragments] не получилось записать [{path_frag}] в [task, frag]: ')
            path_frags.append(path_frag)
        return path_frags
            #
    # удаляет все файлы, которые старше одного дня 
    async def delete_old_files(self):
        # множество имен файлов, которые находятся на диске
        set_file_dir = set(os.listdir(self.save_file_path))
        # текущее время
        current_time = datetime.datetime.now().timestamp()
        for file in set_file_dir:
            full_path = os.path.join(self.save_file_path, file)
            # время последнего изменения файла
            file_mod_time = os.path.getmtime(full_path)
            # если файл старше self.time_del
            if current_time - file_mod_time > self.time_del:
                try:
                    os.remove(full_path)
                    print(f"[Dnld delete_old_files] Файл {full_path} успешно удалён.") 
                    self.Logger.log_info(f"[Dnld delete_old_files] Файл {full_path} успешно удалён.") 
                except Exception as eR:
                    print(f"[Dnld delete_old_files] Не удалось удалить файл {full_path}: {eR}")
                    self.Logger.log_info(f'\nERROR[Dnld delete_old_files] ERROR: {eR}')

#
# MAIN **************************
async def main():
    print(f'\n**************************************************************************')
    print(f'\nБот готов обрабатывать видео')
    mov=Mov() 
    minut=0.3
    while True:
        #
        print(f'\nБот по отработке видео ждет {minut} минут(ы) ...')
        sleep (int(60*minut))
        print(f'\nСодержание таблиц в БД...')
        await mov.Db.print_data('task')
        await mov.Db.print_data('frag')
        # удаляем все файлы, которые старше...
        await mov.delete_old_files()

        # заполняем таблицу frag на базе task
        # task -> frag 
        try:
            if not await mov.making_table_frag():
                print(f'\n[Mov main] В таблице [task] нет ссылок для фрагментации')
        except Exception as eR:
            print(f"\n[Mov main] Не удалось записать в таблицу frag: {eR}")
            mov.Logger.log_info(f"\n[Mov main] Не удалось записать в таблицу frag: {eR}")
            continue
        
        # task -> frag -> rows 
        # из 'frag' скачанные строки, но не фрагментированные 
        try:
            rows = await mov.rows4frag() 
        except Exception as eR:
            print(f'\nERROR[Mov main] ERROR: {eR}') 
            mov.Logger.log_info(f'\nERROR[Mov main] ERROR: {eR}') 
        if not rows: continue
        # есть задачи, делаем фрагментацию и убираем лого
        path_frags = await mov.making_fragments(rows)
        if not path_frags: 
            print(f'\n[Mov main] ERROR не сделали фрагментацию ...')
            mov.Logger.log_info(f'\n[Mov main] ERROR не сделали фрагментацию ...')



if __name__ == "__main__":
    asyncio.run(main())
