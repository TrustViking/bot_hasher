import os, sys, cv2 
# import perception
from types import NoneType
from PIL import Image
from typing import Dict, Tuple, List
from moviepy.editor import VideoFileClip, AudioFileClip
import numpy as np
from sqlalchemy.engine.result import Row

from bot_env.mod_log import Logger
from data_base.base_db import BaseDB
# from perception import tools, hashers
from perception.hashers import PHash, DHash, WaveletHash, MarrHildreth, BlockMean


class VidCompar:
    def __init__(self,
                 save_file_path,
                 path_save_keyframe,
                 log_file='vidcompar_log.txt', 
                 hash_length_factor=0.3, # множитель (0.3*len(hash)) для определения порога расстояния Хэминга 
                 threshold_keyframes = 0.3, # порог для гистограммы ключевых кадров (0-1)
                 logo_size=180,
                 withoutlogo=False,
                 ):
        self.Logger = Logger(log_file=log_file)
        self.Db = BaseDB(logger=self.Logger)
        # Порог схожести для определения уникальности
        self.hash_length_factor=hash_length_factor
        self.threshold_keyframes = threshold_keyframes 
        self.hash_length=64
        self.hash_length_block=968
        self.hash_length_MH=576
        self.withoutlogo=withoutlogo
        self.square_size=logo_size
        # self.save_file_path=os.path.join(sys.path[0], 'diff_video')
        # self.path_save_keyframe=os.path.join(sys.path[0], 'diff_video', 'keyframes')
        self.save_file_path=save_file_path
        self.path_save_keyframe=path_save_keyframe
        # self.threshold = None  


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

    # синхронная обертка для безопасного выполнения методов
    def safe_execute(self, func, name_func: str = None):
        try:
            # print(f'\n***HasherVid safe_execute: выполняем обертку ****')
            return func
        except Exception as eR:
            print(f'\nERROR[VidCompar {name_func}] ERROR: {eR}') 
            self.Logger.log_info(f'\nERROR[VidCompar {name_func}] ERROR: {eR}') 
            return None

    # убираем лого
    def  del_logo (self, full_path: str):
        print(f'\n[VidCompar del_logo] Удаляем лого... \n{full_path}\n')
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
        print(f'\n[VidCompar del_logo] \nvideo: {video} \n'
              f'fps: {fps} \nfourcc: {fourcc} \n'
              f'frame_size: {frame_size} \n'
              f'frame_count: {frame_count}')        
        
        # удаляем фраг с лого
        if video and fps and fourcc and frame_size and os.path.isfile(full_path):
            os.remove(full_path)
        else: print(f'\nERROR [VidCompar del_logo] video: {video} \nfps: {fps} \nfourcc: {fourcc} \nframe_size: {frame_size}')

        # Координаты верхнего левого угла квадрата маски
        x1, y1 = width-self.square_size, 0
        # Координаты нижнего правого угла квадрата маски
        x2, y2 = width, self.square_size
        full_path_noaudio = full_path+'_noaudio.mp4'
        # Определение выходного файла без лого, но и без звука
        out_file = cv2.VideoWriter(full_path_noaudio, fourcc, fps, frame_size)
        # чтение буфера video
        count_frame = 0
        try:
            while video.isOpened():
                # type(frame): <class 'numpy.ndarray'>
                # frame.shape: (1080, 1920, 3)
                ret, frame = video.read()
                if not ret: break
                count_frame+=1
                if count_frame==int(frame_count/2):
                    print(f'\n[VidCompar del_logo] отработали 50% кадров')

                # Создание маски для области логотипа
                mask = np.zeros_like(frame[:, :, 0])
                mask[y1:y2, x1:x2] = 255
                # Применение инпейнтинга для удаления логотипа
                result = cv2.inpaint(frame, mask, inpaintRadius=15, flags=cv2.INPAINT_TELEA)
                out_file.write(result)
        except Exception as eR:
            print(f"\nERROR [VidCompar del_logo] ERROR инпейнтинг для удаления логотипа: {eR}")
            self.Logger.log_info(f"\nERROR [VidCompar del_logo] ERROR инпейнтинг для удаления логотипа: {eR}")
            return None
        finally:
                if video: video.release()
                if out_file: out_file.release()
        
        # склеиваем видео без лого и звука
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
                    print(f'\nERROR [VidCompar del_logo] нет файла full_path: {full_path} \nfull_path_audio: {full_path_audio}')
        print(f'\n[VidCompar del_logo] logo удалили: \n{full_path}\n')
        return full_path

    # извлечение ключевых кадров
    def get_keyframes(self, video_path: str) -> List[np.ndarray]:
        # список ключевых кадров
        keyframes = []
        # hist_keyframes = [] # список сравнения гистограммы кадров видео
        # cap = cv2.VideoCapture(video_path)
        cap = self.safe_execute(cv2.VideoCapture(video_path), 'get_keyframes cv2.VideoCapture')
        ret, prev_frame = cap.read()
        # записываем первый кадр как ключевой
        rgb_frame = cv2.cvtColor(prev_frame, cv2.COLOR_BGR2RGB)
        keyframes.append(rgb_frame)
        # prev_hist - <class 'numpy.ndarray'>
        prev_hist = self.safe_execute(cv2.calcHist([prev_frame], [0, 1, 2], None, [8, 8, 8], [0, 256, 0, 256, 0, 256]), 'get_keyframes calcHist prev_hist')
        # print(f'\n[VidCompar get_keyframes] prev_hist: {prev_hist} \ntype: {type(prev_hist)}')

        while True:
            ret, frame = cap.read()
            if not ret:
                # записываем последний кадр как ключевой
                rgb_frame = cv2.cvtColor(prev_frame, cv2.COLOR_BGR2RGB)
                keyframes.append(rgb_frame)
                break
            # гистограмма текущего кадра
            curr_hist = self.safe_execute(cv2.calcHist([frame], [0, 1, 2], None, [8, 8, 8], [0, 256, 0, 256, 0, 256]), 'get_keyframes calcHist curr_hist')
            # if not curr_hist: continue
            # сравниваем предыдущую и текущую гистограммы
            hist_diff = self.safe_execute(cv2.compareHist(prev_hist, curr_hist, cv2.HISTCMP_BHATTACHARYYA), 'get_keyframes compareHist')
            # hist_keyframes.append(hist_diff)
            # сравнение с пороговым значением ключевого кадра
            if hist_diff > self.threshold_keyframes:
                rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                keyframes.append(rgb_frame)
            # текущий отработанный кадр становится предыдущим
            prev_hist = curr_hist
            prev_frame=frame

        cap.release()
        # print(f'\n[VidCompar get_keyframes] length keyframes: {len(keyframes)}')
        # max_value = max(hist_keyframes)
        if not len(keyframes):
            print(f'\n[VidCompar get_keyframes] \nПри пороге {self.threshold_keyframes} нет ключевых кадров в файле: \n{video_path}. \n'
                  f'Надо уменьшить порог определения ключевых кадров или это видео не имеет их вообще')
            return []
        # print(f'\n[VidCompar get_keyframes] \n'
        #       f'Пороговое значение определения ключевых кадров: [{self.threshold_keyframes}]\n' 
        #       f'При пороге [{self.threshold_keyframes}] количество ключевых кадров: [{len(keyframes)}]\n')
        return keyframes
    #
    # хэши ключевых кадров PHash
    def phash_keyframes(self, keyframes: list):
        hasher = PHash()
        # print(f'\n[VidCompar phash_keyframes]  num key_frame: {len(keyframes)} ')
        return [hasher.compute(frame, hash_format="hex") for frame in keyframes]

    # хэши ключевых кадров DHash
    def dhash_keyframes(self, keyframes: list):
        hasher = DHash()
        # print(f'\n[VidCompar phash_keyframes]  num key_frame: {len(keyframes)} ')
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
    def block_hash_keyframes(self, keyframes: list):
        hasher = BlockMean()
        return [hasher.compute(frame, hash_format="hex") for frame in keyframes]


    # Вычисление расстояния Хэмминга
    def hamming_distance(self, hash_first: str, hash_second: str):
        if len(hash_first)!= len(hash_second):
            print(f'\n[VidCompar hamming_distance] разная длина хэша {len(hash_first)} и {len(hash_second)}')
            return None
        # print(f'\n[VidCompar hamming_distance] \nlen(hash_first): {len(hash_first)} \nhash_first: {hash_first}')
        # print(f'\n[VidCompar hamming_distance] \nlen(hash_second): {len(hash_second)} \nhash_second: {hash_second}')
        return sum(el1 != el2 for el1, el2 in zip(hash_first, hash_second))

    # Преобразование hex в двоичную строку
    def hex_to_binary(self, hex_str: str, xhash: str):
        binary_str = bin(int(hex_str, 16))[2:]
        if xhash=='marrihildreth':
            return binary_str.zfill(self.hash_length_MH)  # Дополнение нулями до длины 576
        elif xhash=='block_hash':
            return binary_str.zfill(self.hash_length_block)  # Дополнение нулями до длины 968
        else:    
            return binary_str.zfill(self.hash_length)  # Дополнение нулями до длины 64

    # Создание матрицы расстояний на основе расстояния Хэмминга
    def matrix_distance_hashes(self, hashes_first: list, hashes_second: list, xhash: str):
        num_hashes_first = len(hashes_first)
        # print(f'\n[VidCompar matrix_distance_hashes] num_hashes_first: {num_hashes_first}')
        num_hashes_second = len(hashes_second)
        # print(f'\n[VidCompar matrix_distance_hashes] num_hashes_second: {num_hashes_second}')
        
        # Инициализация матрицы расстояний нулями
        matrix_distance = np.zeros((num_hashes_first, num_hashes_second))
        for i in range(num_hashes_first):
            for j in range(num_hashes_second):
                bin_hash1 = self.hex_to_binary(hashes_first[i], xhash)
                bin_hash2 = self.hex_to_binary(hashes_second[j], xhash)
                matrix_distance[i][j] = self.hamming_distance(bin_hash1, bin_hash2)
        # print(f'\n[VidCompar matrix_distance_hashes] matrix_distance: \n{matrix_distance}')
        return matrix_distance    

    # анализируем матрицу расстояний ключевых кадров
    def analyze_distance_matrix(self, matrix_distance: np.ndarray, xhash: str):
        # словарь для хранения пар похожих кадров
        similar_pairs = {}  
        
        if xhash=='marrihildreth':
            threshold=int(self.hash_length_factor*self.hash_length_MH)  # длина 576
        elif xhash=='block_hash':
            threshold=int(self.hash_length_factor*self.hash_length_block)  # длина 968
        else:    
            threshold=int(self.hash_length_factor*self.hash_length)  # длина 64
        
        # Проходим по всем элементам матрицы
        num_rows, num_cols = matrix_distance.shape
        for i in range(num_rows):
            for j in range(num_cols):
                d = matrix_distance[i][j]
                # Если расстояние меньше порога, добавляем в список похожих пар
                if d < threshold:
                    similar_pairs[(i, j)]=(d, xhash)
        
        # общая статистика по матрице
        # количество расстояний в матрице меньше пороговового значения
        similar_distance = np.sum(matrix_distance < threshold)
        # минимальное расстояние в матрице
        min_distance = np.min(matrix_distance)
        # максимальное расстояние в матрице
        max_distance = np.max(matrix_distance)
        print(f'\n[VidCompar analyze_distance_matrix] \n'
              f'Метод: [{xhash}]\n'
              f'Порог расстояний между ключевыми кадрами:  {threshold}\n'
              f'Количество похожих ключевых кадров: {similar_distance}\n'
              f'Похожие ключевые кадры  первого видео: [{int(100 * similar_distance / num_rows)}%]\n'
              f'Похожие ключевые кадры  второго видео: [{int(100 * similar_distance / num_cols)}%]\n'
              f'Размерность матрицы: {matrix_distance.shape}\n'
              f'Общее количество элементов в матрице: {num_rows * num_cols}\n'
              f'Минимальное значение в матрице: {min_distance}\n'
              f'Максимальное значение в матрице: {max_distance}\n'
            )
        return similar_pairs # (i, j)=(d, xhash)

    # список хэшей ключевых кадров
    def hashes_keyframes(self, keyframes_first: list, keyframes_second: list) ->List[Tuple]:
        # xhashs=['phash', 'dhash', 'wavelet', 'marrihildreth', 'block_hash']
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
        bhashes_first=self.block_hash_keyframes(keyframes_first)
        bhashes_second=self.block_hash_keyframes(keyframes_second)
        hashes_kframes.append((bhashes_first, bhashes_second, 'block_hash'))
        return hashes_kframes # [([dhashes_first], [dhashes_second], xhashs)]

    # сравниваем хэши [(dhashes_first, dhashes_second, 'dhash')] 
    def diff_hash(self, hashes_kframes: list)-> Dict[str, Dict[Tuple[int, int], Tuple[int, str]]]:
        xhash_similar_pairs={} # словарь для списков кортежей похожих кадров
        for hashes_first, hashes_second, xhash in hashes_kframes:
            # матрица расстояний между хэшами
            matrix_distance = self.safe_execute(self.matrix_distance_hashes(hashes_first, hashes_second, xhash), 'VidCompar diff_hash matrix_distance_hashes')
            # анализ матрицы расстояний между хэшами
            # словарь {(i, j):(d, xhash)} схожих ключевых кадров
            dict_similar_pairs = self.safe_execute(self.analyze_distance_matrix(matrix_distance, xhash), 'VidCompar diff_hash analyze_distance_matrix')
            if isinstance(dict_similar_pairs, dict) and not dict_similar_pairs:
                print(f'[VidCompar diff_hash] метод [{xhash}] не определил ПОХОЖИХ КЛЮЧЕВЫХ КАДРОВ')
                continue
            xhash_similar_pairs[xhash]=dict_similar_pairs
        return xhash_similar_pairs # {xhash: {(i, j):(d, xhash)}}

    # фильтруем словарь всех методов на уникальность 
    # {xhash: {(i, j):(d, xhash)}}
    def filter_diction_hash(self, diction: dict):
        all_diction={}
        # print(f'\n[VidCompar filter_diction_hash] \n'
        #       f'Количество отобранных элементов в diction: [{len(diction.items())}]')
        for x_hash, diction_ij_dhash in diction.items():
            for (i, j), (d, original_xhash) in diction_ij_dhash.items():
                if (i, j) not in all_diction:
                    all_diction[(i, j)]=(d, original_xhash)
                else:
                    d_in_dict, xhash = all_diction[(i, j)]
                    if d < d_in_dict:
                        all_diction[(i, j)]=(d, xhash)
        # print(f'\n[VidCompar filter_diction_hash] \n'
        #       f'Количество отобранных схожих пар ключевых кадров: [{len(all_diction.items())}]')
        return all_diction # (i, j):(d, xhash)

    # создаем ключевые кадры, 
    # списки их хэшей, сравниваем хэши, 
    # анализируем матрицу расстояний Хэмминга 
    async def diff_video(self, row: Row):
        path_first = str(row.path_file_first)
        path_second = str(row.path_file_second)
        date_msg = str(row.date_message)
        
        #  первый список ключевых кадров
        keyframes_first = self.get_keyframes(path_first)
        if keyframes_first is None:
            print(f'\nERROR[VidCompar diff_video] ERROR в первом видео при определении ключевых кадров')
            # отметка в БД
            diction={'in_work':'ERROR', 'num_kframe_1':'ERROR', 'result_kframe':'ERROR'}
            if not await self.Db.update_table_date_chatid(['diff'], date_msg, row.chat_id, diction):
                print(f'\nERROR [VidCompar diff_video] отметить в diff сравнение файлов не получилось\n')
            return None
        elif isinstance(keyframes_first, list) and not keyframes_first:
            print(f'\n[VidCompar diff_video] нет ключевых кадров в первом видео')
            # отметка в БД
            diction={'in_work':'diff', 'num_kframe_1':str(0), 'num_kframe_2':'?', 'result_kframe':'not_kframe'}
            if not await self.Db.update_table_date_chatid(['diff'], date_msg, row.chat_id, diction):
                print(f'\nERROR [VidCompar diff_video] отметить в diff сравнение файлов не получилось\n')
            return []
        elif keyframes_first:
            print(f'\n[VidCompar diff_video] \n'
                  f'Пороговое значение определения ключевых кадров: [{self.threshold_keyframes}]\n'                  
                  f'В первом видео [{len(keyframes_first)}] ключевых кадров')
            # отметка в БД
            diction={'num_kframe_1':str(len(keyframes_first))}
            if not await self.Db.update_table_date_chatid(['diff'], date_msg, row.chat_id, diction):
                print(f'\nERROR [VidCompar diff_video] отметить в diff сравнение файлов не получилось\n')
        
        #  второй список ключевых кадров
        keyframes_second = self.get_keyframes(path_second)
        if keyframes_second is None:
            print(f'\nERROR[VidCompar diff_video] ERROR во втором видео при определении ключевых кадров')
            # отметка в БД
            diction={'in_work':'ERROR', 'num_kframe_2':'ERROR', 'result_kframe':'ERROR'}
            if not await self.Db.update_table_date_chatid(['diff'], date_msg, row.chat_id, diction):
                print(f'\nERROR [VidCompar diff_video] отметить в diff сравнение файлов не получилось\n')
                return None
        elif isinstance(keyframes_second, list) and not keyframes_second:
            print(f'\n[VidCompar diff_video] нет ключевых кадров во втором видео')
            # отметка в БД
            diction={'in_work':'diff', 'num_kframe_2':str(0), 'result_kframe':'not_kframe'}
            if not await self.Db.update_table_date_chatid(['diff'], date_msg, row.chat_id, diction):
                print(f'\nERROR [VidCompar diff_video] отметить в diff сравнение файлов не получилось\n')
            return []
        elif keyframes_second:
            print(f'\n[VidCompar diff_video] \n'
                  f'Пороговое значение определения ключевых кадров: [{self.threshold_keyframes}]\n'                  
                  f'Во втором видео [{len(keyframes_first)}] ключевых кадров')
            # отметка в БД
            diction={'num_kframe_2':str(len(keyframes_second)), 'result_kframe':str(len(keyframes_first)+len(keyframes_second))}
            if not await self.Db.update_table_date_chatid(['diff'], date_msg, row.chat_id, diction):
                print(f'\nERROR [VidCompar diff_video] отметить в diff сравнение файлов не получилось\n')

        # список кортежей хэшей ключевых кадров 
        # [(dhashes_first, dhashes_second, 'dhash')]
        hashes_kframes = self.safe_execute(self.hashes_keyframes(keyframes_first, keyframes_second), 'VidCompar diff_video hashes_keyframes')
        if not hashes_kframes:
            print(f'\nERROR [VidCompar diff_video] ERROR нет хэшей ключевых кадров')
            return None
            #
        # сравниваем и получаем пары похожих хэшей всех методов 
        # {xhash: {(i, j):(d, xhash)}}
        dict_similar_pairs = self.diff_hash(hashes_kframes)
        if isinstance(dict_similar_pairs, dict) and not dict_similar_pairs:
            print(f'\n[VidCompar diff_video] ни один метод не определил ПОХОЖИЕ КЛЮЧЕВЫЕ КАДРЫ')
            # отметка в БД
            diction={'in_work':'diff', 'result_diff':'not_similar'}
            if not await self.Db.update_table_date_chatid(['diff'], date_msg, row.chat_id, diction):
                print(f'\nERROR [VidCompar diff_video] отметить в diff сравнение файлов не получилось\n')
            return set()
        # фильтруем словарь всех методов на уникальность 
        # (i, j):(d, xhash) <- {xhash: {(i, j):(d, xhash)}} 
        similar_pairs = self.filter_diction_hash(dict_similar_pairs)
        # список похожих фото и расстояние Хэмминга 
        similar_frames = []
        for (i, j), (d, xhash) in similar_pairs.items():
            similar_frame=(keyframes_first[i], keyframes_second[j], d, xhash)
            similar_frames.append(similar_frame)
        return similar_frames # [(frame, frame, d, xhash)]


    # точка входа сравнения файлов
    # <class 'sqlalchemy.engine.row.Row'> 
    async def compar_vid_hash(self, row: Row):
        date=str(row.date_message)
        path_first=str(row.path_file_first)
        path_second=str(row.path_file_second)
        
        # удаляем лого если self.withoutlogo=True
        if self.withoutlogo:
            path_first = self.del_logo(path_first)
            if not path_first:
                print(f'\n[VidCompar compar_vid_hash] лого не удалили c первого видео')
                return None
            path_second = self.del_logo(path_second)
            if not path_second:
                print(f'\n[VidCompar compar_vid_hash] лого не удалили со второго видео')
                return None
        else: print(f'\n[VidCompar compar_vid_hash] Лого не будем удалять\n')
        
        ## сравниваем видео
        # [(frame, frame, d, xhash)]
        similar_frames = await self.diff_video(row)
        #
        # нет схожих кадров
        if isinstance(similar_frames, set) and not similar_frames:
            print(f'\n[VidCompar compar_vid_hash] ПОХОЖИХ КЛЮЧЕВЫХ КАДРОВ НЕТ')
            diction = {'in_work':'diff', 'result_diff':'not_similar', 'num_similar_pair':'not_similar'}
            if not await self.Db.update_table_date_chatid(['diff'], date, row.chat_id, diction):
                print(f'\nERROR [VidCompar compar_vid_hash] отметить в таблице сравнение файлов \n{path_first} \n'
                      f'{path_second} не получилось')
            return set()
        
        # нет ключевых кадров
        elif isinstance(similar_frames, list) and not similar_frames:
            print(f'\n[VidCompar compar_vid_hash] нет ключевых кадров в первом или втором видео')
            diction = {'in_work':'diff', 'result_kframe':'not_kframe', 'result_diff':'not_similar'}
            if not await self.Db.update_table_date_chatid(['diff'], date, row.chat_id, diction):
                print(f'\nERROR [VidCompar compar_vid_hash] отметить в таблице сравнение файлов \n{path_first} \n'
                      f'{path_second} не получилось')
            return []
        
        # ошибка в процессе выполнения
        elif similar_frames is None:
            print(f'\nERROR[VidCompar compar_vid_hash] ERROR при определении ключевых кадров и сравнении видео')
            return None
        
        # отмечаем в таблице diff факт сравнения видео
        print(f'\n[VidCompar compar_vid_hash] \n'
              f'Количество отобранных схожих пар ключевых кадров: [{len(similar_frames)}]')
        diction = {'in_work':'diff', 'result_diff':'similar', 'num_similar_pair': str(len(similar_frames))}
        if not await self.Db.update_table_date_chatid(['diff'], date, row.chat_id, diction):
            print(f'\nERROR [VidCompar compar_vid_hash] отметить в таблице сравнение файлов \n{path_first} \n'
                    f'{path_second} не получилось')
        
        # сохраняем на диск похожие ключевые кадры
        # [(frame, frame, d, xhash)]
        path2file = os.path.join(self.path_save_keyframe, date.replace(' ', '_').replace(':', '_'))
        self._create_save_directory(path2file)
        for index, similar_frame in enumerate(similar_frames):
            #
            frame_1, frame_2, d, xhash = similar_frame
            image_pil_1 = Image.fromarray(np.uint8(frame_1), 'RGB')
            image_pil_2 = Image.fromarray(np.uint8(frame_2), 'RGB')
            name_1 = str('kframe_'+xhash+'_id'+str(index)+'_d'+str(int(d))+'_1.png')
            name_2 = str('kframe_'+xhash+'_id'+str(index)+'_d'+str(int(d))+'_2.png')
            full_name_file_1 = os.path.join(path2file, name_1) 
            full_name_file_2 = os.path.join(path2file, name_2) 
            # сохранение в формате PNG
            self.safe_execute(image_pil_1.save(full_name_file_1), 'comparator_files')
            self.safe_execute(image_pil_2.save(full_name_file_2), 'comparator_files')
            # отмечаем в таблице diff факт записи видео
        
        diction = {'path_sim_img':path2file}
        if not await self.Db.update_table_date_chatid(['diff'], date, row.chat_id, diction):
            print(f'\nERROR [VidCompar compar_vid_hash] отметить в таблице сравнение файлов \n{path_first} \n'
                    f'{path_second} не получилось')
        
        return path2file  

