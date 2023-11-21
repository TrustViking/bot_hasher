
from typing import Union, Set, Tuple, List, Optional, Any
from cv2 import VideoCapture 
from cv2 import compareHist, calcHist, cvtColor 
from cv2 import COLOR_BGR2RGB, HISTCMP_BHATTACHARYYA
from os import makedirs
from time import strftime
from sys import platform
import numpy as np
from sqlalchemy.engine.result import Row

from bot_env.mod_log import LogBot 
from bot_env.decorators import safe_execute, safe_await_execute
from data_base.base_db import MethodDB
from data_base.table_db import DiffTable



class Kframes: 
    
    countInstance=0
    
    def __init__(self,
                logger: LogBot,
                method_db: MethodDB, 
                 ):
        Kframes.countInstance += 1
        self.countInstance = Kframes.countInstance
        self.cls_name = self.__class__.__name__
        self.logger = logger
        self.method_db = method_db
        self.name_table = DiffTable.name_table
        #
        self._print()


    # выводим атрибуты объекта
    def _print(self):
        msg = (
            f"\nStarted at {strftime('%X')}\n"
            f'[{__name__}|{self.cls_name}] countInstance: [{self.countInstance}]\n'
            f'platform: [{platform}]\n'
            f'\nAttributes:\n'
            )

        attributes_to_print = [
            'cls_name',
            'logger',
            'method_db',
            'name_table',
        ]

        for attr in attributes_to_print:
            # "Attribute not found" будет выведено, если атрибут не существует
            value = getattr(self, attr, "Attribute not found")  
            msg += f"{attr}: {value}\n"

        print(msg)
        self.logger.log_info(msg)



    # создаем директорию, если такой папки нет
    def create_directory(self, paths: list[str]):
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _create_directory():
            """
            Создает директорию, если она не существует

            Аргументы:
            - paths: список строк, каждая из которых является путем к директории, 
            которую необходимо создать.
            """
            _ = [makedirs(path,  exist_ok=True) for path in paths]
        return _create_directory()


    ### извлечение ключевых кадров
    def get_keyframes(self, video_path: str, threshold_keyframes: float) -> List[np.ndarray]:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _get_keyframes():
            # список ключевых кадров
            keyframes = []
            cap = VideoCapture(video_path)
            ret, prev_frame = cap.read()
            prev_hist = calcHist([prev_frame], [0, 1, 2], None, [8, 8, 8], [0, 256, 0, 256, 0, 256])
            while True:
                ret, frame = cap.read()
                if not ret:
                    break
                # гистограмма текущего кадра
                curr_hist = calcHist([frame], [0, 1, 2], None, [8, 8, 8], [0, 256, 0, 256, 0, 256])
                # сравниваем предыдущую и текущую гистограммы
                hist_diff = compareHist(prev_hist, curr_hist, HISTCMP_BHATTACHARYYA)
                # сравнение с пороговым значением ключевого кадра
                if hist_diff > threshold_keyframes:
                    rgb_frame = cvtColor(frame, COLOR_BGR2RGB)
                    keyframes.append(rgb_frame)
                # текущий отработанный кадр становится предыдущим
                prev_hist = curr_hist

            cap.release()

            if not len(keyframes):
                print(f'\n[{__name__}|{self.cls_name}] \n'
                    f'При пороге {threshold_keyframes} '
                    f'нет ключевых кадров в файле: \n{video_path}. \n'
                    f'Надо уменьшить порог определения ключевых кадров или '
                    f'это видео не имеет их вообще')
                return set()
            return keyframes
        return _get_keyframes()

    # при определении ключевых кадров видео получили ОШИБКУ 
    # записываем в БД 
    async def save_db_error(self, first_video: bool, **kwargs):
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def  _save_db_error():
            
            if first_video:
                diction={'in_work':'ERROR', 'num_kframe_1':'ERROR', 'result_kframe':'ERROR'}
            elif not first_video:
                diction={'in_work':'ERROR', 'num_kframe_2':'ERROR', 'result_kframe':'ERROR'}
            
            table = kwargs.get('table')
            date_msg = kwargs.get('date_msg')
            chat_id = kwargs.get('chat_id')

            if not await self.method_db.update_table_date_chatid([table], date_msg, chat_id, diction):
                print(f'\nERROR [{__name__}|{self.cls_name}] отметить в {table} ERROR при сравнении файлов не получилось\n')
        
        return await _save_db_error()


    # отсутствие ключевых кадров видео записываем в БД 
    async def save_db_notkframe(self, first_video: bool, **kwargs):
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def  _save_db_notkframe():
            
            if first_video:
                diction = {'in_work':'diff', 'num_kframe_1':str(0), 'num_kframe_2':'?', 'result_kframe':'not_kframe', 'result_diff':'not_similar'}
            elif not first_video:
                diction = {'in_work':'diff', 'num_kframe_2':str(0), 'result_kframe':'not_kframe', 'result_diff':'not_similar'}

            table = kwargs.get('table')
            date_msg = kwargs.get('date_msg')
            chat_id = kwargs.get('chat_id')

            if not await self.method_db.update_table_date_chatid([table], date_msg, chat_id, diction):
                print(f'\nERROR [{__name__}|{self.cls_name}] отметить в {table} отсутствие ключевых кадров не получилось\n')
        
        return await _save_db_notkframe()


    # результаты определения ключевых кадров видео записываем в БД 
    async def save_db(self, first_video: bool, **kwargs):
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def  _save_db():
            
            if first_video:
                keyframes = kwargs.get('keyframes_first')
                diction={'num_kframe_1':str(len(keyframes))}
            elif not first_video:
                keyframes = kwargs.get('keyframes_second')
                diction={'num_kframe_2':str(len(keyframes))}
            
            table = kwargs.get('table')
            date_msg = kwargs.get('date_msg')
            chat_id = kwargs.get('chat_id')

            if not await self.method_db.update_table_date_chatid([table], date_msg, chat_id, diction):
                print(f'\nERROR [{__name__}|{self.cls_name}] отметить в {table} ключевые кадры не получилось\n')
        
        return await _save_db()


    # анализируем результаты определения ключевых кадров видео 
    # результаты анализа записываем в БД 
    async def analiz_kframes(self, first_video: bool, **kwargs) -> Union[None, Set[None], List[np.ndarray]]:
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def  _analiz_kframes():
            
            if first_video:
                keyframes = kwargs.get("keyframes_first", None)
            elif not first_video:
                keyframes = kwargs.get("keyframes_second", None)

            # ошибка при определении ключевых кадров
            if keyframes is None:
                print(f'\nERROR[{__name__}|{self.cls_name}] ERROR в первом видео при определении ключевых кадров')
                await self.save_db_error(first_video, kwargs)
                return None
            
            # не определелили ключевые кадры
            elif isinstance(keyframes, set):
                print(f'\n[{__name__}|{self.cls_name}] нет ключевых кадров в первом видео')
                await self.save_db_notkframe(first_video, kwargs)
                return set()
            
            # есть ключевые кадры в видео
            elif isinstance(keyframes, list) and keyframes:
                if first_video:
                    num_vid = 'В первом' 
                elif not first_video:
                    num_vid = 'Во втором' 

                threshold_kframes = kwargs.get('threshold_kframes', None)
                print(f'\nПороговое значение определения ключевых кадров: [{threshold_kframes}]\n'                  
                    f'{num_vid} видео определили [{len(keyframes)}] ключевых кадров')
                await self.save_db(first_video, kwargs)
                return keyframes
            
            # Непонятное значение keyframes
            else:
                print(f'\nНепонятное значение keyframes: [{keyframes}]\n') 
                return None                
        return await _analiz_kframes()


    # делаем два списка ключевых кадров
    async def get_kframes_videos(self, **kwargs)-> Union[Set[None], None, Tuple[List[np.ndarray], List[np.ndarray]]]:
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def  _get_kframes_videos():
            path_first = kwargs.get('path_first')
            path_second = kwargs.get('path_second')
            threshold_kframes = kwargs.get('threshold_kframes')

            #  первый список ключевых кадров
            keyframes_first = self.get_keyframes(path_first, threshold_kframes)
            
            kwargs['table'] = self.name_table
            kwargs['keyframes_first'] = keyframes_first

            result_analiz = await self.analiz_kframes(True, **kwargs)
            # не определелили ключевые кадры
            if isinstance(result_analiz, set):
                return set() 
            # ERROR
            elif result_analiz is None:
                return None

            #  второй список ключевых кадров
            keyframes_second = self.get_keyframes(path_second, threshold_kframes)
            
            kwargs['keyframes_second']=keyframes_second
            
            result_analiz = await self.analiz_kframes(False, **kwargs)
            
            # не определелили ключевые кадры
            if isinstance(result_analiz, set):
                return set() 
            # ERROR
            elif result_analiz is None:
                return None
            
            return keyframes_first, keyframes_second 
        return await _get_kframes_videos()
