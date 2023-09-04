import os, cv2 
# import perception

from moviepy.editor import VideoFileClip, AudioFileClip
import numpy as np
from bot_env.mod_log import Logger
# from perception import tools, hashers
from perception.hashers import PHash, VideoHasher


class VidCompar:
    def __init__(self,
                 log_file='vidcompar_log.txt', 
                 threshold=10,
                 threshold_keyframes = 0.3, # порог для гистограммы ключевых кадров (0-1)
                 logo_size=180,
                 ):
        self.Logger = Logger(log_file=log_file)
        # Порог схожести для определения уникальности
        self.threshold = threshold  
        self.threshold_keyframes = threshold_keyframes 
        self.hash_length=64
        self.square_size=logo_size

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
    def get_keyframes(self, video_path):
        # выделяем ключевые кадры
        keyframes = []
        # hist_keyframes = [] # список сравнения гистограммы кадров видео
        cap = cv2.VideoCapture(video_path)
        ret, prev_frame = cap.read()
        # prev_hist - <class 'numpy.ndarray'>
        prev_hist = cv2.calcHist([prev_frame], [0, 1, 2], None, [8, 8, 8], [0, 256, 0, 256, 0, 256])
        # print(f'\n[VidCompar get_keyframes] prev_hist: {prev_hist} \ntype: {type(prev_hist)}')

        while True:
            ret, frame = cap.read()
            if not ret:
                break
            # текущая гистограмма 
            curr_hist = cv2.calcHist([frame], [0, 1, 2], None, [8, 8, 8], [0, 256, 0, 256, 0, 256])
            # if not curr_hist: continue
            # сравниваем предыдущую и текущую гистограммы
            hist_diff = cv2.compareHist(prev_hist, curr_hist, cv2.HISTCMP_BHATTACHARYYA)
            # hist_keyframes.append(hist_diff)
            # Пороговое значение, можно настроить
            if hist_diff > self.threshold_keyframes:
                rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                keyframes.append(rgb_frame)
            prev_hist = curr_hist
        cap.release()
        # print(f'\n[VidCompar get_keyframes] length keyframes: {len(keyframes)}')
        # max_value = max(hist_keyframes)
        if not len(keyframes):
            print(f'\n[VidCompar get_keyframes] Ключевых кадров не обнаружено. \n'
                  f'Надо уменьшить порог определения ключевых кадров или это видео не имеет их вообще')
        print(f'\n[VidCompar get_keyframes] \n'
              f'Пороговое значение определения ключевых кадров: [{self.threshold_keyframes}]\n' 
              f'При таком пороге определено количество ключевых кадров: [{len(keyframes)}]\n')
        return keyframes
    #
    # хэши ключевых кадров
    def phash_keyframes(self, keyframes: list):
        hasher = PHash()
        # print(f'\n[VidCompar phash_keyframes]  num key_frame: {len(keyframes)} ')
        return [hasher.compute(frame, hash_format="hex") for frame in keyframes]

    # Вычисление расстояния Хэмминга
    def hamming_distance(self, hash_first: str, hash_second: str):
        if len(hash_first)!= len(hash_second) or len(hash_first)!= self.hash_length or len(hash_second)!= self.hash_length :
            print(f'\n[VidCompar hamming_distance] разная длина хэша {len(hash_first)} и {len(hash_second)}')
            return None
        return sum(el1 != el2 for el1, el2 in zip(hash_first, hash_second))

    # Преобразование hex в двоичную строку
    def hex_to_binary(self, hex_str: str):
        binary_str = bin(int(hex_str, 16))[2:]
        return binary_str.zfill(self.hash_length)  # Дополнение нулями до длины 64

    # Создание матрицы расстояний на основе расстояния Хэмминга
    def matrix_distance_hashes(self, hashes_first: list, hashes_second: list):
        num_hashes_first = len(hashes_first)
        # print(f'\n[VidCompar matrix_distance_hashes] num_hashes_first: {num_hashes_first}')
        num_hashes_second = len(hashes_second)
        # print(f'\n[VidCompar matrix_distance_hashes] num_hashes_second: {num_hashes_second}')
        
        # Инициализация матрицы расстояний нулями
        matrix_distance = np.zeros((num_hashes_first, num_hashes_second))
        for i in range(num_hashes_first):
            for j in range(num_hashes_second):
                bin_hash1 = self.hex_to_binary(hashes_first[i])
                bin_hash2 = self.hex_to_binary(hashes_second[j])
                matrix_distance[i][j] = self.hamming_distance(bin_hash1, bin_hash2)
        print(f'\n[VidCompar matrix_distance_hashes] matrix_distance: \n{matrix_distance}')
        return matrix_distance    

    # анализируем матрицу расстояний ключевых кадров
    def analyze_distance_matrix(self, matrix_distance: np.ndarray):
        # Получаем размеры матрицы расстояний
        print(f'\n[VidCompar analyze_distance_matrix] matrix_distance.shape: {matrix_distance.shape}')
        num_rows, num_cols = matrix_distance.shape
        similar_pairs = []  # Список для хранения пар похожих кадров
        above_threshold_count = 0  # Счетчик для расстояний выше порога
        total_elements = num_rows * num_cols  # Общее количество элементов в матрице

        # Проходим по всем элементам матрицы
        for i in range(num_rows):
            for j in range(num_cols):
                # Если расстояние меньше порога, добавляем в список похожих пар
                if matrix_distance[i][j] < self.threshold:
                    similar_pairs.append((i, j, matrix_distance[i][j]))
                else:
                    above_threshold_count += 1  # Увеличиваем счетчик для расстояний выше порога

        # Вычисляем расстояние в матрице меньше пороговового значения
        similar_distance = np.sum(matrix_distance < self.threshold)
        # Вычисляем среднее расстояние в матрице
        # mean_distance = np.mean(matrix_distance)
        # Вычисляем минимальное расстояние в матрице
        min_distance = np.min(matrix_distance)
        # Вычисляем максимальное расстояние в матрице
        max_distance = np.max(matrix_distance)
        # Количество расстояний меньше среднего
        # below_mean_count = np.sum(matrix_distance < mean_distance)
        # Количество расстояний выше среднего
        # above_mean_count = total_elements - below_mean_count

        print(f'\n[VidCompar analyze_distance_matrix] \n'
              f'Порог расстояний между ключевыми кадрами:  {self.threshold}\n'
              f'Количество похожих ключевых кадров: {similar_distance}\n'
              f'Похожие ключевые кадры  первого видео: [{int(100 * similar_distance / num_rows)}%]\n'
              f'Похожие ключевые кадры  второго видео: [{int(100 * similar_distance / num_cols)}%]\n'
              f'\nОбщее количество элементов в матрице: {total_elements}\n'
              f'Минимальное значение в матрице: {min_distance}\n'
              f'Максимальное значение в матрице: {max_distance}\n'
            #   f'Количество расстояний выше среднего: {above_mean_count} [{int(100 * above_mean_count / total_elements)}%]\n'
            )
        return similar_pairs

    # анализируем матрицу расстояний ключевых кадров
    def is_video_unique(self, path_first: str, path_second: str,):
        # # удаляем лого
        # path_first = self.del_logo(path_first)
        # if not path_first:
        #     print(f'\n[VidCompar is_video_unique] лого не удалили c первого видео')
        #     return None
        # path_second = self.del_logo(path_second)
        # if not path_second:
        #     print(f'\n[VidCompar is_video_unique] лого не удалили со второго видео')
        #     return None

        # список ключевых кадров
        keyframes_first = self.get_keyframes(path_first)
        if not keyframes_first:
            print(f'\n[VidCompar is_video_unique] нет ключевых кадров в первом видео')
            return None
        keyframes_second = self.get_keyframes(path_second)
        # список хэшей ключевых кадров
        hashes_first=self.phash_keyframes(keyframes_first)
        hashes_second=self.phash_keyframes(keyframes_second)
        # матрица расстояний между хэшами
        matrix_distance = self.matrix_distance_hashes(hashes_first, hashes_second)
        # список кортежей (i, j, matrix_distance[i][j])
        similar_pairs = self.analyze_distance_matrix(matrix_distance)
        similar_frames = []
        for k in similar_pairs:
            # print(f'\n[VidCompar is_video_unique] похожие кадры: {k}')
            # список кортежей (i, j, matrix_distance[i][j])
            i, j, d = k
            similar_frame=(keyframes_first[i], keyframes_second[j])
            similar_frames.append(similar_frame)
            # similar_frames.append(keyframes_first[i])
            # similar_frames.append(keyframes_second[j])
        return similar_pairs, similar_frames



        


        # """
        # [prev_frame]: это изображение, для которого вычисляется гистограмма. 
        # Изображение передается в виде списка, потому что calcHist может работать 
        # с несколькими изображениями одновременно.
        # [0, 1, 2]: это каналы изображения, для которых вычисляется гистограмма. 
        # В данном случае это каналы B, G и R (0, 1 и 2 соответственно).
        # None: маска для изображения. В данном случае маска не используется, 
        # поэтому передается None.
        # [8, 8, 8]: это количество "бинов" (или интервалов) для каждого из каналов 
        # B, G и R. В данном случае гистограмма для каждого канала будет иметь 8 бинов.
        # [0, 256, 0, 256, 0, 256]: это диапазоны значений для каждого канала. 
        # Для каналов R, G и B диапазон значений — от 0 до 255, 
        # поэтому указаны интервалы [0, 256].
        # Выходной результат — это 3D гистограмма, представляющая распределение цветов 
        # в изображении. Эта гистограмма затем используется для сравнения с 
        # гистограммой следующего кадра и определения, насколько они различны.
        # """
