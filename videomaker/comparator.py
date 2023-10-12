
from typing import Coroutine, Callable, Any
from time import strftime
from os.path import isfile, join
from os import makedirs, remove
from sys import platform

from PIL import Image
from typing import Tuple, List
import numpy as np
from sqlalchemy.engine.result import Row
from perception.hashers import PHash, DHash, WaveletHash, MarrHildreth, BlockMean

from bot_env.mod_log import Logger
from data_base.base_db import BaseDB
from .delogo import Delogo
from .kframes import Kframes

class VidCompar:
    
    countInstance=0
    
    def __init__(self,
                 logger: Logger,
                 method_db: BaseDB, 
                 save_file_path: str,
                 path_save_keyframe: str,
                #  hash_factor=0.2, # множитель (0.3*len(hash)) для определения порога расстояния Хэминга 
                #  threshold_keyframes = 0.3, # порог для гистограммы ключевых кадров (0-1)
                #  logo_size=180,
                #  number_corner = '2',
                #  withoutlogo=False,
                 ):
        VidCompar.countInstance += 1
        self.countInstance = VidCompar.countInstance
        #
        self.Logger = logger
        self.Db = method_db
        self.save_file_path=save_file_path
        self.path_save_keyframe=path_save_keyframe
        # self.hash_factor=hash_factor
        # self.threshold_keyframes = threshold_keyframes 
        # self.logo_size=logo_size
        # self.number_corner = number_corner
        # self.withoutlogo=withoutlogo
        #
        # Длина хэшей разных методов
        self.hash_length=64
        self.hash_length_block=968
        self.hash_length_MH=576
        #
        self.Dl = Delogo(self.Logger)
        self.Kf = Kframes(self.Logger)
        self._print()

    # выводим № объекта
    def _print(self):
        print(f'\n[VidCompar] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'\n[VidCompar] countInstance: [{self.countInstance}]\n')
        msg = (f"Started at {strftime('%X')}\n"
              f'platform: [{platform}]\n'
              f'\nАргументы:\n'
              f'folder_logfile: {self.Logger.folder_logfile}\n'
              f'logfile: {self.Logger.logfile}\n'
              f'loglevel: {self.Logger.loglevel}\n'
              f'folder_db: {self.Db.folder_db}\n'
              f'name_db: {self.Db.name_db}\n'
              f'save_file_path: {self.save_file_path}\n'
              f'path_save_keyframe: {self.path_save_keyframe}\n'
            #   f'hash_factor: {self.hash_factor}\n'
            #   f'threshold_keyframes: {self.threshold_keyframes}\n'
            #   f'withoutlogo: {self.withoutlogo}\n'
            #   f'logo_size: {self.logo_size}\n'
            #   f'number_corner: {self.number_corner}\n'
              )
        print(msg)
        self.Logger.log_info(msg)

    # создаем директорию, если такой папки нет
    def create_directory(self, paths: list[str]):
        """
        Создает директорию для хранения video и ключевых кадров, 
        если она не существует

         Аргументы:
        - paths: список строк, каждая из которых является путем к директории, которую необходимо создать.
        """
        _ = [makedirs(path,  exist_ok=True) for path in paths]

    # асинхронная обертка для безопасного выполнения методов
    # async def safe_execute(self, coroutine: Callable[..., Coroutine[Any, Any, T]]) -> T:
    async def safe_await_execute(self, coroutine: Coroutine, name_func: str = None):
        try:
            return await coroutine
        except Exception as eR:
            print(f'\nERROR[VidCompar {name_func}] ERROR: {eR}') 
            self.Logger.log_info(f'\nERROR[VidCompar {name_func}] ERROR: {eR}') 
            return None

    # синхронная обертка для безопасного выполнения методов
    def safe_execute(self, func: Callable[..., Any], name_func: str = None):
        try:
            return func()
        except Exception as eR:
            print(f'\nERROR[VidCompar {name_func}] ERROR: {eR}')
            self.Logger.log_info(f'\nERROR[VidCompar {name_func}] ERROR: {eR}')
            return None

    # удаляем файл с диска
    def delete_files(self, paths: list[str]):
        """
        Удаляем файлы

         Аргументы:
        - paths: список строк, каждая из которых является путем к файлу, 
          который необходимо удалить.
        """
        _ = [self.safe_execute(remove(path), 'delete_files') for path in paths  if isfile(path)]

    #
    # хэши ключевых кадров PHash
    def phash_keyframes(self, keyframes: list):
        hasher = PHash()
        return [hasher.compute(frame, hash_format="hex") for frame in keyframes]

    # хэши ключевых кадров DHash
    def dhash_keyframes(self, keyframes: list):
        hasher = DHash()
        return [hasher.compute(frame, hash_format="hex") for frame in keyframes]

    # хэши ключевых кадров WaveletHash
    def wavelet_hash_keyframes(self, keyframes: list):
        hasher = WaveletHash()
        return [hasher.compute(frame, hash_format="hex") for frame in keyframes]

    # хэши ключевых кадров MarrHildreth
    def marrihildreth_hash_keyframes(self, keyframes: list):
        hasher = MarrHildreth()
        return [hasher.compute(frame, hash_format="hex") for frame in keyframes]

    # хэши ключевых кадров BlockMean
    def blockhash_keyframes(self, keyframes: list):
        hasher = BlockMean()
        return [hasher.compute(frame, hash_format="hex") for frame in keyframes]


    # Вычисление расстояния Хэмминга
    def hamming_distance(self, hash_first: str, hash_second: str):
        if len(hash_first)!= len(hash_second):
            print(f'\n[VidCompar hamming_distance] разная длина хэша {len(hash_first)} и {len(hash_second)}')
            return None
        return sum(el1 != el2 for el1, el2 in zip(hash_first, hash_second))

    # Преобразование hex в двоичную строку
    def hex_to_binary(self, hex_str: str, xhash: str):
        binary_str = bin(int(hex_str, 16))[2:]
        if xhash=='marrihildreth':
            return binary_str.zfill(self.hash_length_MH)  # Дополнение нулями до длины 576
        elif xhash=='blockhash':
            return binary_str.zfill(self.hash_length_block)  # Дополнение нулями до длины 968
        else:    
            return binary_str.zfill(self.hash_length)  # Дополнение нулями до длины 64

    # Создание матрицы расстояний на основе расстояния Хэмминга
    def matrix_distance_hashes(self, hashes_first: list, hashes_second: list, xhash: str):
        num_hashes_first = len(hashes_first)
        num_hashes_second = len(hashes_second)
        
        # Инициализация матрицы расстояний нулями
        matrix_distance = np.zeros((num_hashes_first, num_hashes_second))
        for i in range(num_hashes_first):
            for j in range(num_hashes_second):
                bin_hash1 = self.hex_to_binary(hashes_first[i], xhash)
                bin_hash2 = self.hex_to_binary(hashes_second[j], xhash)
                matrix_distance[i][j] = self.hamming_distance(bin_hash1, bin_hash2)
        print(f'\nМетод [{xhash}] Matrix_distance: \n{matrix_distance}')
        return matrix_distance    

    # общая статистика по матрице расстояний
    def stat_distance_matrix(self, 
                             matrix_distance: np.ndarray, 
                             xhash: str, 
                             threshold: int,
                             ):
        # количество расстояний в матрице меньше пороговового значения
        similar_distance = np.sum(matrix_distance < threshold)
        # минимальное расстояние в матрице
        min_distance = np.min(matrix_distance)
        # максимальное расстояние в матрице
        max_distance = np.max(matrix_distance)
        num_matrix=matrix_distance.size
        print(f'\nМетод: [{xhash}]\n'
              f'Количество похожих ключевых кадров: *[{similar_distance}]\n'
              f'Порог расстояний между ключевыми кадрами:  {threshold}\n'
              f'Минимальное значение в матрице: {min_distance}\n'
              f'Максимальное значение в матрице: {max_distance}\n'
              f'Размерность матрицы: {matrix_distance.shape}\n'
              f'Общее количество элементов в матрице: {num_matrix}'
            )
        # xhashs=['phash', 'dhash', 'wavelet', 'marrihildreth', 'blockhash']
        xhash_threshold = xhash+'_threshold'
        xhash_min_distance = xhash+'_min_distance'
        xhash_max_distance = xhash+'_max_distance'
        xhash_matrix_shape = xhash+'_matrix_shape'
        diction = {xhash:xhash, 
                   xhash_threshold:int(threshold),
                   xhash_min_distance:int(min_distance),
                   xhash_max_distance:int(max_distance),
                   xhash_matrix_shape:int(num_matrix),
                    }
        return diction

    # анализируем матрицу расстояний ключевых кадров
    def analyze_distance_matrix(self, matrix_distance: np.ndarray, xhash: str, hash_factor: float):
        # словарь для хранения пар похожих кадров
        similar_pairs = {}  
        
        if xhash=='marrihildreth':
            threshold=int(hash_factor*self.hash_length_MH)  # длина 576
        elif xhash=='blockhash':
            threshold=int(hash_factor*self.hash_length_block)  # длина 968
        else:    
            threshold=int(hash_factor*self.hash_length)  # длина 64
        
        # Проходим по всем элементам матрицы
        num_rows, num_cols = matrix_distance.shape
        for i in range(num_rows):
            for j in range(num_cols):
                d = matrix_distance[i][j]
                # Если расстояние меньше порога, добавляем в список похожих пар
                if d < threshold:
                    similar_pairs[(i, j)]=(d, xhash, threshold)
        # общая статистика по матрице расстояний
        diction = self.stat_distance_matrix(matrix_distance, xhash, threshold)
        return similar_pairs, diction # ((i, j)=(d, xhash, threshold), diction)

    # список хэшей ключевых кадров
    def hashes_keyframes(self, keyframes_first: list, keyframes_second: list) ->List[Tuple]:
        # xhashs=['phash', 'dhash', 'wavelet', 'marrihildreth', 'blockhash']
        hashes_kframes=[]
        dhashes_first = self.dhash_keyframes(keyframes_first)
        dhashes_second = self.dhash_keyframes(keyframes_second)
        hashes_kframes.append((dhashes_first, dhashes_second, 'dhash'))
        #
        phashes_first=self.phash_keyframes(keyframes_first)
        phashes_second=self.phash_keyframes(keyframes_second)
        hashes_kframes.append((phashes_first, phashes_second, 'phash'))
        #
        whashes_first=self.wavelet_hash_keyframes(keyframes_first)
        whashes_second=self.wavelet_hash_keyframes(keyframes_second)
        hashes_kframes.append((whashes_first, whashes_second, 'wavelet'))
        #
        mhashes_first=self.marrihildreth_hash_keyframes(keyframes_first)
        mhashes_second=self.marrihildreth_hash_keyframes(keyframes_second)
        hashes_kframes.append((mhashes_first, mhashes_second, 'marrihildreth'))
        #
        bhashes_first=self.blockhash_keyframes(keyframes_first)
        bhashes_second=self.blockhash_keyframes(keyframes_second)
        hashes_kframes.append((bhashes_first, bhashes_second, 'blockhash'))
        return hashes_kframes # [([dhashes_first], [dhashes_second], xhashs)]

    # сравниваем хэши [(dhashes_first, dhashes_second, 'dhash')] 
    def diff_hash(self, hashes_kframes: list):
        xhash_similar_pairs={} # словарь для списков кортежей похожих кадров
        dictions={} # словарь для статистики результатов анализа матрицы расстояний
        for hashes_first, hashes_second, xhash in hashes_kframes:
            # матрица расстояний между хэшами
            matrix_distance = self.safe_execute(self.matrix_distance_hashes(hashes_first, hashes_second, xhash), 'VidCompar diff_hash matrix_distance_hashes')
            # анализ матрицы расстояний между хэшами
            # словарь {(i, j)=(d, xhash, threshold)} схожих ключевых кадров
            dict_similar_pairs, diction = self.safe_execute(self.analyze_distance_matrix(matrix_distance, xhash), 'VidCompar diff_hash analyze_distance_matrix')
            dictions.update(diction)
            if isinstance(dict_similar_pairs, dict) and not dict_similar_pairs:
                print(f'Метод [{xhash}] не определил ПОХОЖИХ КЛЮЧЕВЫХ КАДРОВ')
                continue
            xhash_similar_pairs[xhash]=dict_similar_pairs
        return xhash_similar_pairs, dictions # {xhash: {# (i, j)=(d, xhash, threshold)}}, {diction for DB}

    # фильтруем словарь всех методов на уникальность 
    # {xhash: {(i, j)=(d, xhash, threshold)}}
    def filter_diction_hash(self, diction: dict):
        all_diction={}
        # print(f'\n[VidCompar filter_diction_hash] \n'
        #       f'Количество отобранных элементов в diction: [{len(diction.items())}]')
        for x_hash_, diction_ij_dhash in diction.items():
            for (i, j), (d, xhash, threshold) in diction_ij_dhash.items():
                if (i, j) not in all_diction:
                    all_diction[(i, j)]=(d, xhash, threshold)
                else:
                    d_in_dict, xhash_in_dict, threshold_in_dict = all_diction[(i, j)]
                    if d < d_in_dict:
                        all_diction[(i, j)]=(d, xhash, threshold)
        # print(f'\n[VidCompar filter_diction_hash] \n'
        #       f'Количество отобранных схожих пар ключевых кадров: [{len(all_diction.items())}]')
        return all_diction # {(i, j)=(d, xhash, threshold)}

    # делаем два списка ключевых кадров
    async def get_kframes_two_path(self, path_first: str, path_second: str, threshold_kframes: float, date_msg: str, chat_id: str):
        
        #  первый список ключевых кадров
        keyframes_first = self.Kf.get_keyframes(path_first, threshold_kframes)
        
        # ошибка при определении ключевых кадров первого видео
        if keyframes_first is None:
            print(f'\nERROR[VidCompar get_kframes_two_path] ERROR в первом видео при определении ключевых кадров')
            # отметка в БД - ERROR
            diction={'in_work':'ERROR', 'num_kframe_1':'ERROR', 'result_kframe':'ERROR'}
            if not await self.Db.update_table_date_chatid(['diff'], date_msg, chat_id, diction):
                print(f'\nERROR [VidCompar get_kframes_two_path] отметить в diff сравнение файлов не получилось\n')
            return None
        
        # не определелили ключевые кадры в первом видео
        elif isinstance(keyframes_first, list) and not keyframes_first:
            print(f'\n[VidCompar get_kframes_two_path] нет ключевых кадров в первом видео')
            # отметка в БД - not_kframe
            # diction = {'in_work':'diff', 'num_kframe_1':str(0), 'num_kframe_2':'?', 'result_kframe':'not_kframe'}
            diction = {'in_work':'diff', 'num_kframe_1':str(0), 'num_kframe_2':'?', 'result_kframe':'not_kframe', 'result_diff':'not_similar'}
            if not await self.Db.update_table_date_chatid(['diff'], date_msg, chat_id, diction):
                print(f'\nERROR [VidCompar get_kframes_two_path] отметить в diff сравнение файлов не получилось\n')
            return []
        
        # есть ключевые кадры в первом видео
        elif keyframes_first: 
            print(f'\nПороговое значение определения ключевых кадров: [{threshold_kframes}]\n'                  
                  f'В первом видео [{len(keyframes_first)}] ключевых кадров')
            # отметка в БД
            diction={'num_kframe_1':str(len(keyframes_first))}
            if not await self.Db.update_table_date_chatid(['diff'], date_msg, chat_id, diction):
                print(f'\nERROR [VidCompar get_kframes_two_path] отметить в diff сравнение файлов не получилось\n')
        
        #  второй список ключевых кадров
        keyframes_second = self.Kf.get_keyframes(path_second, threshold_kframes)
        # ошибка при определении ключевых кадров второго видео
        if keyframes_second is None:
            print(f'\nERROR[VidCompar get_kframes_two_path] ERROR во втором видео при определении ключевых кадров')
            # отметка в БД - ERROR
            diction={'in_work':'ERROR', 'num_kframe_2':'ERROR', 'result_kframe':'ERROR'}
            if not await self.Db.update_table_date_chatid(['diff'], date_msg, chat_id, diction):
                print(f'\nERROR [VidCompar get_kframes_two_path] отметить в diff сравнение файлов не получилось\n')
                return None
        # не определелили ключевые кадры во втором видео
        elif isinstance(keyframes_second, list) and not keyframes_second:
            print(f'\n[VidCompar get_kframes_two_path] нет ключевых кадров во втором видео')
            # отметка в БД - not_kframe
            diction = {'in_work':'diff', 'num_kframe_2':str(0), 'result_kframe':'not_kframe', 'result_diff':'not_similar'}
            if not await self.Db.update_table_date_chatid(['diff'], date_msg, chat_id, diction):
                print(f'\nERROR [VidCompar get_kframes_two_path] отметить в diff сравнение файлов не получилось\n')
            return []
        # определелили ключевые кадры во втором видео
        elif keyframes_second:
            print(f'\nПороговое значение определения ключевых кадров: [{threshold_kframes}]\n'                  
                  f'Во втором видео [{len(keyframes_second)}] ключевых кадров')
            # отметка в БД
            diction={'num_kframe_2':str(len(keyframes_second)), 
                     'result_kframe':str(len(keyframes_first)+len(keyframes_second)),
                     }
            if not await self.Db.update_table_date_chatid(['diff'], date_msg, chat_id, diction):
                print(f'\nERROR [VidCompar get_kframes_two_path] отметить в diff сравнение файлов не получилось\n')
        
        return keyframes_first,keyframes_second 




    # создаем ключевые кадры, 
    # списки их хэшей, сравниваем хэши, 
    # анализируем матрицу расстояний Хэмминга 
    async def diff_video(self, row: Row):
        path_first = str(row.path_file_first)
        path_second = str(row.path_file_second)
        date_msg = str(row.date_message)
        chat_id = str(row.chat_id)
        threshold_kframes = float(row.threshold_kframes)

        # делаем два списка ключевых кадров
        kframes = await self.safe_await_execute(self.get_kframes_two_path(path_first, path_second, threshold_kframes, date_msg, chat_id), 'get_kframes_two_path')
        if isinstance(kframes, list):
            return []
        elif not kframes:
            return None
        elif isinstance(kframes, tuple):
            keyframes_first, keyframes_second = kframes

        # список кортежей хэшей ключевых кадров 
        # [(dhashes_first, dhashes_second, 'dhash')]
        hashes_kframes = self.safe_execute(self.hashes_keyframes(keyframes_first, keyframes_second), 'VidCompar diff_video hashes_keyframes')
        if not hashes_kframes:
            print(f'\nERROR [VidCompar diff_video] ERROR нет хэшей ключевых кадров')
            return None
            #
        # сравниваем и получаем пары похожих хэшей всех методов 
        # {xhash: {# (i, j)=(d, xhash, threshold)}}, {diction for DB}
        dict_similar_pairs, diction_stat_DB = self.diff_hash(hashes_kframes)
        if isinstance(dict_similar_pairs, dict) and not dict_similar_pairs:
            print(f'\n[VidCompar diff_video] ни один метод не определил ПОХОЖИЕ КЛЮЧЕВЫЕ КАДРЫ')
            # отметка в БД
            diction={'in_work':'diff', 'result_diff':'not_similar', 'num_similar_pair':'not_similar'}
            diction.update(diction_stat_DB) # добавляем статистику анализа в БД
            if not await self.Db.update_table_date_chatid(['diff'], date_msg, row.chat_id, diction):
                print(f'\nERROR [VidCompar diff_video] отметить в diff сравнение файлов не получилось\n')
            return set()
        # фильтруем словарь всех методов на уникальность 
        # {(i, j)=(d, xhash, threshold)} <- {xhash: {# (i, j)=(d, xhash, threshold)}} 
        similar_pairs = self.filter_diction_hash(dict_similar_pairs)
        # список похожих фото и расстояние Хэмминга 
        similar_frames = []
        for (i, j), (d, xhash, threshold) in similar_pairs.items():
            similar_frame=(keyframes_first[i], keyframes_second[j], d, xhash, threshold)
            similar_frames.append(similar_frame)
        return similar_frames # [(frame, frame, d, xhash, threshold)]

    # записываем на диск схожие кадры
    def save_similar_frames(self, path2file: str, 
                                  index: int, 
                                  frame_1: np.ndarray, 
                                  frame_2: np.ndarray, 
                                  d: int,
                                  xhash: str,
                                  threshold: int):
        # полный путь к файлам
        name_1 = str('id'+str(index)+'_'+xhash+'_d'+str(int(d))+'_'+str(threshold)+'_1.png')
        name_2 = str('id'+str(index)+'_'+xhash+'_d'+str(int(d))+'_'+str(threshold)+'_2.png')
        # name_2 = str('id'+str(index)+'_'+xhash+'_d'+str(int(d))+'_2.png')
        full_name_file_1 = join(path2file, name_1) 
        full_name_file_2 = join(path2file, name_2) 
        # сохранение в формате PNG
        image_pil_1 = Image.fromarray(np.uint8(frame_1), 'RGB')
        image_pil_2 = Image.fromarray(np.uint8(frame_2), 'RGB')
        self.safe_execute(image_pil_1.save(full_name_file_1), 'comparator_files')
        self.safe_execute(image_pil_2.save(full_name_file_2), 'comparator_files')
        return full_name_file_1, full_name_file_2
    
    # удаляем лого с переданных на сравнение видеофайлов
    def delete_logo(self, paths: list[str], logo_size: int, corner: str):
        new_paths = [self.safe_execute(self.Dl.del_logo(path, logo_size, corner), 'self.Dl.del_logo') for path in paths]
        if not new_paths:
                print(f'\nERROR[VidCompar delete_logo] ERROR лого не удалили c таких файлов: {paths}')
                return None
        return new_paths


    #### точка входа сравнения файлов
    #### <class 'sqlalchemy.engine.row.Row'> 
    async def compar_vid_hash(self, row: Row):
        date=str(row.date_message)
        path_first=str(row.path_file_first)
        path_second=str(row.path_file_second)
        # удаляем лого если withoutlogo=yes
        if str(row.withoutlogo)=='yes':
            path_first, path_second = self.safe_execute(self.delete_logo([path_first, path_second]), 'delete_logo')
        else: print(f'\n[VidCompar compar_vid_hash] Лого не будем удалять\n')
        
        ### основной метод сравнения видео
        # # [(frame, frame, d, xhash, threshold)]
        similar_frames = await self.safe_await_execute(self.diff_video(row), 'diff_video')
        #
        # ошибка в процессе выполнения
        if similar_frames is None:
            print(f'\nERROR[VidCompar compar_vid_hash] ERROR при определении ключевых кадров и сравнении видео')
            return None
        
        # нет схожих кадров
        elif isinstance(similar_frames, set) and not similar_frames:
            print(f'\n[VidCompar compar_vid_hash] ПОХОЖИХ КЛЮЧЕВЫХ КАДРОВ НЕТ')
            return set()
        
        # нет ключевых кадров
        elif isinstance(similar_frames, list) and not similar_frames:
            print(f'\n[VidCompar compar_vid_hash] нет ключевых кадров в первом или втором видео')
            return []
        
        
        # сохраняем на диск похожие ключевые кадры
        # # [(frame, frame, d, xhash, threshold)]
        path2file = join(self.path_save_keyframe, date.replace(' ', '_').replace(':', '_'))
        self.create_directory([path2file])
        for index, similar_frame in enumerate(similar_frames):
            path_saving=self.save_similar_frames(path2file, index, *similar_frame)
            if not path_saving:
                print(f'\nERROR [VidCompar compar_vid_hash] записать на диск схожие кадры не получилось')
                return None
        
        print(f'\n[VidCompar compar_vid_hash] \n'
              f'Количество отобранных схожих пар ключевых кадров: [{len(similar_frames)}]')
        # отмечаем в таблице diff факт сравнения и записи ключевых кадров на диск
        diction = {
                   'in_work':'diff', 
                   'result_diff':'similar', 
                   'num_similar_pair': str(len(similar_frames)),
                   'save_sim_img':'saved', 
                   'path_sim_img':path2file,
                   }
        if not await self.Db.update_table_date_chatid(['diff'], date, row.chat_id, diction):
            print(f'\nERROR [VidCompar compar_vid_hash] отметить в таблице сравнение файлов \n{path_first} \n'
                    f'{path_second} не получилось')
        
        return path2file  

