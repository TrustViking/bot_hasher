
from typing import Union, Set, Tuple, List, Optional, Dict
from time import strftime
from os.path import isfile, join, abspath, dirname
from os import makedirs, remove
from sys import platform, path, argv

from PIL import Image
from typing import Tuple, List
import numpy as np
from sqlalchemy.engine.result import Row
# from perception.hashers import PHash, DHash, WaveletHash, MarrHildreth, BlockMean

from bot_env.bot_init import ConfigInitializer
from bot_env.decorators import safe_await_execute, safe_execute
from bot_env.mod_log import LogBot
from data_base.table_db import DiffTable
from data_base.base_db import MethodDB
from .delogo import Delogo 
from .kframes import Kframes
from .kfhasher import HasherKFrames




class Comparison(ConfigInitializer):
    
    countInstance=0
    
    def __init__(self,
                config_path: str,
                logger: LogBot,
                method_db: MethodDB, 
                 ):
        super().__init__()
        Comparison.countInstance += 1
        self.countInstance = Comparison.countInstance
        self.cls_name = self.__class__.__name__
        # Logger
        self.logger = logger
        # MethodDB
        self.method_db = method_db
        self.abspath = dirname(abspath(__file__))
        self.entry_point = abspath(argv[0]) # точка входа самого первого скрипта
        # config
        self.config_path = config_path
        self.config = self.read_config(self.config_path)
        self.folder_video = self.config.get('folder_video') 
        self.folder_kframes = self.config.get('folder_kframes') 
        self.path_save_keyframe = join(path[0], self.folder_kframes)
        # Delogo
        self.delogo = Delogo(self.logger)
        # Kframes
        self.kframe = Kframes(self.logger, self.method_db)
        # Hasher
        self.hasher = HasherKFrames(self.logger)
        #
        self.name_table = DiffTable.name_table
        self.kwargs = {}
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
            'abspath',
            'entry_point',
            'config_path',
            'config',
            'folder_video',
            'folder_kframes',
            'path_save_keyframe',
            'delogo',
            'kframe',
            'hasher',
            'kwargs',
            'name_table',

        ]

        for attr in attributes_to_print:
            # "Attribute not found" будет выведено, если атрибут не существует
            value = getattr(self, attr, "Attribute not found")  
            msg += f"{attr}: {value}\n"

        print(msg)
        self.logger.log_info(msg)


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


    # удаляем файл с диска
    def delete_files(self, paths: list[str]):
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _delete_files():
            """
            Удаляем файлы

            Аргументы:
            - paths: список строк, каждая из которых является путем к файлу, 
            который необходимо удалить.
            """
            _ = [remove(path) for path in paths  if isfile(path)]
        return _delete_files()
    

    # фильтруем словарь всех методов на уникальность 
    # {xhash: {(i, j)=(d, xhash, threshold)}}
    def filter_diction_hash(self, diction: dict) -> Dict[Tuple[int, int], Tuple[int, str, int]]:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _filter_diction_hash():
            all_diction={}

            for x_hash_, diction_ij_dhash in diction.items():
                for (i, j), (d, xhash, threshold) in diction_ij_dhash.items():
                    if (i, j) not in all_diction:
                        all_diction[(i, j)]=(d, xhash, threshold)
                    else:
                        d_in_dict, xhash_in_dict, threshold_in_dict = all_diction[(i, j)]
                        if d < d_in_dict:
                            all_diction[(i, j)]=(d, xhash, threshold)

            return all_diction # {(i, j)=(d, xhash, threshold)}
        return _filter_diction_hash()


    # записываем в БД для уникальных видео
    async def save_unic(self, diction: dict, **kwargs):
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def  _save_notunic():
            
            dictions={'in_work':'diff', 'result_diff':'not_similar', 'num_similar_pair':'not_similar'}
            dictions.update(diction) # добавляем статистику анализа в БД
            
            table = kwargs.get('table')
            date_msg = kwargs.get('date_msg')
            chat_id = kwargs.get('chat_id')

            if not await self.method_db.update_table_date_chatid([table], date_msg, chat_id, dictions):
                print(f'\nERROR [{__name__}|{self.cls_name}] отметить в таблице [{table}] сравнение файлов не получилось\n')
        
        return await _save_notunic()

    # записываем в БД есть схожие кадры
    async def save_notunic(self, diction: dict, **kwargs):
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def  _save_notunic():
            
            # dictions={'in_work':'diff', 'result_diff':'not_similar', 'num_similar_pair':'not_similar'}
            # dictions.update(diction) # добавляем статистику анализа в БД
            
            table = kwargs.get('table')
            date_msg = kwargs.get('date_msg')
            chat_id = kwargs.get('chat_id')

            if not await self.method_db.update_table_date_chatid([table], date_msg, chat_id, diction):
                print(f'\nERROR [{__name__}|{self.cls_name}] отметить в таблице [{table}] сравнение файлов не получилось\n')
        
        return await _save_notunic()


    # сравниваем и получаем пары похожих хэшей всех методов 
    async def hash_comparison(self, kframes_first: List[np.ndarray], kframes_second: List[np.ndarray], **kwargs) \
        -> Optional[Union[List[Tuple[np.ndarray, np.ndarray, int, str, int]], Dict]]:
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def  _hash_comparison():
            
            #сравниваем хэши [(dhashes_first, dhashes_second, 'dhash')] 
            result_diff = self.hasher.diff_hash(kframes_first, kframes_second, **kwargs)
            
            if result_diff is None:
                print(f'\nERROR [{__name__}|{self.cls_name}] ERROR при сравнении хэшей ключевых кадров')
                return None
            
            elif isinstance(result_diff, Dict[str, int]):
                print(f'\n[{__name__}|{self.cls_name}] ни один метод не определил ПОХОЖИЕ КЛЮЧЕВЫЕ КАДРЫ')
                # записываем в БД для уникальных видео
                await self.save_unic(result_diff, **kwargs)
                return {}
            
            elif isinstance(result_diff, tuple):
                dict_similar_pairs, diction_db = result_diff

            # записываем в БД есть схожие кадры
            await self.save_notunic(diction_db, **kwargs)

            # фильтруем словарь всех методов на уникальность 
            # {(i, j)=(d, xhash, threshold)} <- {xhash: {# (i, j)=(d, xhash, threshold)}} 
            pairs = self.filter_diction_hash(dict_similar_pairs)
            
            # список похожих кадров и расстояние Хэмминга 
            similar_frames = []
            for (i, j), (d, xhash, threshold) in pairs.items():
                similar_frame=(kframes_first[i], kframes_second[j], d, xhash, threshold)
                similar_frames.append(similar_frame)
            return similar_frames
        return _hash_comparison()


    # создаем ключевые кадры, 
    # списки их хэшей, сравниваем хэши, 
    # анализируем матрицу расстояний Хэмминга 
    async def diff_video(self, **kwargs) -> Optional[Union[Set, Dict, List[Tuple[np.ndarray, np.ndarray, int, str, int]]]]:
        """
        Асинхронно выполняет сравнение видео на основе ключевых кадров и их хэшей.
        Применяет несколько методов для сравнения и анализирует матрицу расстояний Хэмминга.
        
        Возможные возвращаемые значения:
        - None: В случае, если произошла ошибка при получении ключевых кадров или при их сравнении.
        - set(): Возвращает пустое множество, если не удалось определить ключевые кадры в видео.
        - {}: Возвращает пустой словарь, если ни один метод сравнения не определил похожие ключевые кадры.
        - result_comparison: В случае успешного сравнения возвращает список кортежей. Каждый кортеж содержит два ключевых кадра, расстояние Хэмминга между ними, используемый метод хеширования и пороговое значение для сравнения.
        
        :return: одно из вышеуказанных значений
        """            
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def  _diff_video():
            # два списка ключевых кадров 
            kframes = await self.kframe.get_kframes_videos(**kwargs)
            # ERROR
            if kframes is None:
                return None
            # не определелили ключевые кадры в видео
            elif isinstance(kframes, set):
                return set()
            
            # сравниваем и получаем пары похожих хэшей всех методов 
            result_comparison = await self.hash_comparison(*kframes, **kwargs)
            # ERROR
            if result_comparison is None:
                return None
            # ни один метод не определил ПОХОЖИЕ КЛЮЧЕВЫЕ КАДРЫ    
            elif isinstance(result_comparison, dict):
                return {}
            
            return result_comparison # [(frame, frame, d, xhash, threshold)]
        return await _diff_video()


    # записываем на диск схожие кадры
    def save_frames(self, 
                    path2file: str, 
                    index: int, 
                    frame_1: np.ndarray, 
                    frame_2: np.ndarray, 
                    d: int,
                    xhash: str,
                    threshold: int) -> Optional[Tuple[str, str]]:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _save_frames():
            
            # полный путь к файлам
            name_1 = str('id'+str(index)+'_'+xhash+'_d'+str(int(d))+'_'+str(threshold)+'_1.png')
            name_2 = str('id'+str(index)+'_'+xhash+'_d'+str(int(d))+'_'+str(threshold)+'_2.png')
            full_name_file_1 = join(path2file, name_1) 
            full_name_file_2 = join(path2file, name_2) 

            # создание объекта PIL изображения на основе массива NumPy
            image_pil_1 = Image.fromarray(np.uint8(frame_1), 'RGB')
            image_pil_2 = Image.fromarray(np.uint8(frame_2), 'RGB')

            # сохранение в формате PNG
            image_pil_1.save(full_name_file_1)
            image_pil_2.save(full_name_file_2)
            
            return full_name_file_1, full_name_file_2
        return _save_frames()


    # сохраняем на диск похожие ключевые кадры
    # # [(frame, frame, d, xhash, threshold)]
    def save_similar_frames(self, 
                            date_message: str, 
                            path_save_keyframe: str, 
                            similar_frames: List[Tuple[np.ndarray, np.ndarray, int, str, int]]) -> Optional[Tuple[str, str]]:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _save_similar_frames():
            # date=str(row.date_message)
            path2file = join(path_save_keyframe, date_message.replace(' ', '_').replace(':', '_'))
            self.create_directory([path2file])

            list_index=[]
            for index, similar_frame in enumerate(similar_frames):
                path_saving=self.save_frames(path2file, index, *similar_frame)
                if not path_saving:
                    print(f'\nERROR [{__name__}|{self.cls_name}] записать на диск схожие кадры не получилось')
                    continue
                list_index.append(index)
            
            print(f'\n[{__name__}|{self.cls_name}] \n'
                  f'Общее количество отобранных схожих пар ключевых кадров: [{len(similar_frames)}]\n'
                  f'Общее количество записанных схожих пар ключевых кадров: [{list_index[-1]}]\n'
                  )
            return str(len(similar_frames)), path2file
        return _save_similar_frames()


    # формируем kwargs для diff_video и 
    # выполянем diff_video
    async def kwargs_diffvideo (self, row: Row) -> Optional[Union[Set, Dict, List[Tuple[np.ndarray, np.ndarray, int, str, int]]]]:
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def  _kwargs_diffvideo():
            
            ### основной метод сравнения видео
            # # [(frame, frame, d, xhash, threshold)]
            self.kwargs = {
            'table' : self.name_table,
            'path_first' : str(row.path_file_first),
            'path_second' : str(row.path_file_second),
            'date_msg' : str(row.date_message),
            'chat_id' : str(row.chat_id),
            'threshold_kframes' : float(row.threshold_kframes),
            'hash_factor' :  float(row.hash_factor) 
                        }
            return await self.diff_video(**self.kwargs)
        return await _kwargs_diffvideo()

    
    # анализируем результаты сравнения ключевых кадров
    async def analiz_diffvideo(self, row: Row) -> Optional[List[Tuple[np.ndarray, np.ndarray, int, str, int]]]:
        """
        Асинхронно анализирует результаты сравнения ключевых кадров двух видео.
        
        Возможные возвращаемые значения:
        - None: В случае ошибки, отсутствия похожих ключевых кадров или отсутствия ключевых кадров в одном из видео.
        - List[Tuple[np.ndarray, np.ndarray, int, str, int]]: В случае успешного сравнения возвращает список кортежей. Каждый кортеж содержит два ключевых кадра, расстояние Хэмминга между ними, используемый метод хеширования и пороговое значение для сравнения.
        
        :return: одно из вышеуказанных значений
        """        
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def  _analiz_diffvideo():
            ### основной метод сравнения видео
            # # [(frame, frame, d, xhash, threshold)]
            similar_frames = await self.kwargs_diffvideo(row)
            
            # ERROR
            if similar_frames is None:
                print(f'\nERROR[{__name__}|{self.cls_name}] ERROR при определении ключевых кадров и сравнении видео')
                return None
            
            # нет схожих кадров
            elif isinstance(similar_frames, dict):
                print(f'\n[{__name__}|{self.cls_name}] ПОХОЖИХ КЛЮЧЕВЫХ КАДРОВ НЕТ')
                return None
            
            # нет ключевых кадров
            elif isinstance(similar_frames, set):
                print(f'\n[{__name__}|{self.cls_name}] нет ключевых кадров в первом или втором видео')
                return None
            
            elif isinstance(similar_frames, list):
                return similar_frames
            
            else:
                print(f'\n[{__name__}|{self.cls_name}] не понятный similar_frames: {similar_frames}')
                return None

        return await _analiz_diffvideo()


    # отмечаем в таблице diff факт сравнения и записи ключевых кадров на диск
    async def save_db(self, num_similar_pair: str, path2file: str, **kwargs)-> Optional[str]:
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def  _save_db():

            diction = {
                    'in_work':'diff', 
                    'result_diff':'similar', 
                    'num_similar_pair': num_similar_pair,
                    'save_sim_img':'saved', 
                    'path_sim_img':path2file,
                    }
            
            table = kwargs.get('table')
            date_msg = kwargs.get('date_msg')
            chat_id = kwargs.get('chat_id')
            path_first = kwargs.get('path_first')
            path_second = kwargs.get('path_second')

            if not await self.method_db.update_table_date_chatid([table], date_msg, chat_id, diction):
                print(f'\nERROR [{__name__}|{self.cls_name}] отметить в таблице {table} сравнение файлов \n{path_first} \n'
                      f'{path_second} не получилось'
                      )
                return None
            return date_msg
        return await _save_db()



    #### точка входа сравнения файлов
    #### <class 'sqlalchemy.engine.row.Row'> 
    # удаляем лого, сравниваем видео, запись в БД
    async def files_comparison(self, row: Row) -> Optional[Tuple[str, str]] :
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def  _files_comparison():
            
            # удаляем лого, если в БД стоит задача
            if not self.delogo.delete_logo(row):
                print(f'\n[{__name__}|{self.cls_name}] ERROR DELETE LOGO\n')
            
            ### пары схожих ключевых кадров
            # основной метод сравнения видео
            similar_frames = await self.analiz_diffvideo(row)
            if similar_frames is None:
                return None

            # сохраняем на диск похожие ключевые кадры
            result_save = self.save_similar_frames(row.date_message, self.path_save_keyframe, similar_frames)
            if not result_save:
                return None
            
            # отмечаем в таблице diff факт сравнения и записи ключевых кадров на диск
            if not await self.save_db(*result_save, **self.kwargs):
                return None
            
            return result_save  
        return await _files_comparison()
