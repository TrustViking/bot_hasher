

from typing import Union, Set, Tuple, List, Optional, Any, Dict
from os import makedirs
from time import strftime
from sys import platform
from numpy.typing import NDArray
import numpy as np
from sqlalchemy.engine.result import Row
from perception.hashers import PHash, DHash, WaveletHash, MarrHildreth, BlockMean


from bot_env.mod_log import Logger 
from bot_env.decorators import safe_execute, safe_await_execute
from data_base.base_db import MethodDB
from data_base.table_db import DiffTable


class HasherKFrames: 
    
    countInstance=0
    # Длина хэшей разных методов
    HASH_LENGTH = 64
    HASH_LENGTH_BLOCK = 968
    HASH_LENGTH_MH = 576
    
    def __init__(self,
                logger: Logger,
                # method_db: MethodDB, 
                 ):
        HasherKFrames.countInstance += 1
        self.countInstance = HasherKFrames.countInstance
        self.cls_name = self.__class__.__name__
        self.logger = logger
        # self.method_db = method_db
        self.xhashs=['dhash', 'phash', 'wavelet', 'marri', 'blockhash']
        #
        self._print()

    # выводим № объекта
    def _print(self):
        print(f'\n[{__name__}|{self.cls_name}] countInstance: [{self.countInstance}]')
        self.logger.log_info(f'\n[Kframes] countInstance: [{self.countInstance}]\n')
        msg = (f"Started at {strftime('%X')}\n"
              f'platform: [{platform}]\n'
              f'\nАргументы:\n'
              f'logger: {self.logger}\n'
              )
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


    # хэши ключевых кадров PHash
    def phash_kframes(self, keyframes: List[np.ndarray]) -> List[str]:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _phash_kframes():
            phasher = PHash()
            return [phasher.compute(frame, hash_format="hex") for frame in keyframes]
        return _phash_kframes()


    # хэши ключевых кадров DHash
    def dhash_kframes(self, keyframes: List[np.ndarray]) -> List[str]:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _dhash_kframes():
            dhash = DHash()
            return [dhash.compute(frame, hash_format="hex") for frame in keyframes]
        return _dhash_kframes()


    # хэши ключевых кадров WaveletHash
    def wavelet_hash_kframes(self, keyframes: List[np.ndarray]) -> List[str]:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _wavelet_hash_kframes():
            wavelet = WaveletHash()
            return [wavelet.compute(frame, hash_format="hex") for frame in keyframes]
        return _wavelet_hash_kframes()


    # хэши ключевых кадров MarrHildreth
    def marri_hash_kframes(self, keyframes: List[np.ndarray]) -> List[str]:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _marri_hash_kframes():
            marri = MarrHildreth()
            return [marri.compute(frame, hash_format="hex") for frame in keyframes]
        return _marri_hash_kframes()


    # хэши ключевых кадров BlockMean
    def blockhash_kframes(self, keyframes: List[np.ndarray]) -> List[str]:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _blockhash_kframes():
            block = BlockMean()
            return [block.compute(frame, hash_format="hex") for frame in keyframes]
        return _blockhash_kframes()


    # Вычисление расстояния Хэмминга
    def hamming_distance(self, hash_first: str, hash_second: str) -> int:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _hamming_distance():
            if len(hash_first)!= len(hash_second):
                print(f'\n[{__name__}|{self.cls_name}] разная длина хэша {len(hash_first)} и {len(hash_second)}')
                return None
            return sum(el1 != el2 for el1, el2 in zip(hash_first, hash_second))
        return _hamming_distance()


    # Преобразование hex в двоичную строку
    def hex_to_binary(self, hex_str: str, xhash: str):
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _hex_to_binary():
            binary_str = bin(int(hex_str, 16))[2:]
            # Дополнение нулями до длины 576
            if xhash=='marri':
                return binary_str.zfill(HasherKFrames.HASH_LENGTH_MH)  
            # Дополнение нулями до длины 968
            elif xhash=='blockhash':
                return binary_str.zfill(HasherKFrames.HASH_LENGTH_BLOCK)  
            # Дополнение нулями до длины 64
            else:    
                return binary_str.zfill(HasherKFrames.HASH_LENGTH)  
        return _hex_to_binary()


    # список хэшей ключевых кадров всех методов
    def hashes_kframes(self, list_kframes: List[List[np.ndarray]], name_hashes: List[str]) -> List[Tuple]:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _hashes_kframes():
            # self.xhashs=['dhash', 'phash', 'wavelet', 'marri', 'blockhash']
            xhashes=[]
            dhashes = [self.dhash_kframes(keyframes) for keyframes in list_kframes]
            xhashes.append(dhashes)
            phashes=[self.phash_kframes(keyframes) for keyframes in list_kframes]
            xhashes.append(phashes)
            whashes=[self.wavelet_hash_kframes(keyframes) for keyframes in list_kframes]
            xhashes.append(whashes)
            mhashes=[self.marri_hash_kframes(keyframes) for keyframes in list_kframes]
            xhashes.append(mhashes)
            bhashes=[self.blockhash_kframes(keyframes) for keyframes in list_kframes]
            xhashes.append(bhashes)
            if len(xhashes)!=len(name_hashes):
                print(f'[{__name__}|{self.cls_name}] ERROR len(xhashes)!=len(name_hashes)\n'
                      f'len(xhashes): {len(xhashes)}\n'
                      f'len(name_hashes): {len(name_hashes)}\n')
                return None
            thashes=[]
            # xhashes = [ [[hashes], [hashes]], [[hashes], [hashes]] ]
            for hash, name in zip(xhashes, name_hashes):
                hash.append(name)
                thash = tuple(hash)
                thashes.append(thash)
            
            return thashes # [([dhashes_first], [dhashes_second], xhashs)]
        return _hashes_kframes()


    # общая статистика по матрице расстояний
    def stat_distance_matrix(self, matrix_distance: NDArray[np.int16], xhash: str, threshold: int) -> Dict[str, int]:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _stat_distance_matrix():
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
            # xhashs=['phash', 'dhash', 'wavelet', 'marri', 'blockhash']
            xhash_threshold = xhash+'_threshold'
            xhash_min_distance = xhash+'_min_distance'
            xhash_max_distance = xhash+'_max_distance'
            xhash_matrix_shape = xhash+'_matrix_shape'
            
            diction = {xhash:xhash, 
                        xhash_threshold:threshold,
                        xhash_min_distance:int(min_distance),
                        xhash_max_distance:int(max_distance),
                        xhash_matrix_shape:num_matrix,
                        }
            return diction
        return _stat_distance_matrix()


    # анализируем матрицу расстояний ключевых кадров
    def analyze_distance_matrix(self, matrix_distance:  NDArray[np.int16], xhash: str, hash_factor: float) \
        ->  Union[Dict[str, int], Tuple[Dict[Tuple[int, int], Tuple[int, str, int]], Dict[str, int]]]:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _analyze_distance_matrix():
            
            if xhash=='marri':
                threshold=int(hash_factor*HasherKFrames.HASH_LENGTH_MH)  # длина 576
            elif xhash=='blockhash':
                threshold=int(hash_factor*HasherKFrames.HASH_LENGTH_BLOCK)  # длина 968
            else:    
                threshold=int(hash_factor*HasherKFrames.HASH_LENGTH)  # длина 64

            # Проходим по всем элементам матрицы
            num_rows, num_cols = matrix_distance.shape
            # словарь для хранения пар похожих кадров
            similar_pairs = {}  
            for i in range(num_rows):
                for j in range(num_cols):
                    # расстояние между ключевыми кадрами
                    d = matrix_distance[i][j]
                    # Если расстояние меньше порога, 
                    # добавляем в список похожих пар
                    if d < threshold:
                        similar_pairs[(i, j)]=(d, xhash, threshold)
            
            # общая статистика по матрице расстояний
            diction = self.stat_distance_matrix(matrix_distance, xhash, threshold)

            if not similar_pairs:
                print(f'\n[{__name__}|{self.cls_name}] Метод [{xhash}] при пороге {threshold} не определил ПОХОЖИХ КЛЮЧЕВЫХ КАДРОВ')
                return diction
            
            return similar_pairs, diction # ((i, j)=(d, xhash, threshold), diction)
        return _analyze_distance_matrix()


    # Создание матрицы расстояний на основе расстояния Хэмминга
    def matrix_distance_hashes(self, hashes_first: List[str], hashes_second: List[str], xhash: str) -> NDArray[np.int16]:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _matrix_distance_hashes():
            
            num_hashes_first = len(hashes_first)
            num_hashes_second = len(hashes_second)
            
            # Инициализация матрицы расстояний нулями
            matrix_distance = np.zeros((num_hashes_first, num_hashes_second), dtype=np.int16)
            for i in range(num_hashes_first):
                for j in range(num_hashes_second):
                    bin_hash1 = self.hex_to_binary(hashes_first[i], xhash)
                    bin_hash2 = self.hex_to_binary(hashes_second[j], xhash)
                    matrix_distance[i][j] = self.hamming_distance(bin_hash1, bin_hash2)
            print(f'\nМетод [{xhash}] Matrix_distance: \n{matrix_distance}')
            return matrix_distance    
        return _matrix_distance_hashes()


    ### Точка входа сравниваем хэши [(dhashes_first, dhashes_second, 'dhash')] 
    def diff_hash(self, keyframes_first: List[np.ndarray], keyframes_second: List[np.ndarray], **kwarg) \
        -> Optional[Union[Dict[str, int], Tuple[Dict[str, Dict[Tuple[int, int], Tuple[int, str, int]]], Dict[str, int]]]]:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _diff_hash():

            # список кортежей хэшей ключевых кадров 
            # [(dhashes_first, dhashes_second, 'dhash')]
            hashes_kframes = self.hashes_kframes(keyframes_first, keyframes_second)
            if not hashes_kframes:
                print(f'\nERROR [{__name__}|{self.cls_name}] ERROR при определении хэшей ключевых кадров')
                return None


            xhash_similar_pairs={} # словарь для списков кортежей похожих кадров
            dictions_DB={} # словарь для статистики результатов анализа матрицы расстояний
            for hashes_first, hashes_second, xhash in hashes_kframes:
                
                # матрица расстояний между хэшами одного метода 
                matrix_distance = self.matrix_distance_hashes(hashes_first, hashes_second, xhash)
                
                # анализ матрицы расстояний между хэшами
                # словарь {(i, j)=(d, xhash, threshold)} схожих ключевых кадров
                hash_factor = kwarg('hash_factor')
                result_analiz = self.analyze_distance_matrix(matrix_distance, xhash, hash_factor)
                
                if isinstance(result_analiz, Dict[str, int]):
                    print(f'\n[{__name__}|{self.cls_name}] Метод [{xhash}] не определил ПОХОЖИХ КЛЮЧЕВЫХ КАДРОВ')
                    return result_analiz
                
                elif isinstance(result_analiz, tuple):
                    dict_similar_pairs, diction =  result_analiz
                    xhash_similar_pairs[xhash]=dict_similar_pairs
                    dictions_DB.update(diction)
            
            return xhash_similar_pairs, dictions_DB # {xhash: {# (i, j)=(d, xhash, threshold)}}, Dict[str, int]
        return _diff_hash()




