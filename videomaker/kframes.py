

from typing import Callable, Any, List, Dict, Tuple
from cv2 import VideoCapture, VideoWriter 
from cv2 import compareHist, calcHist, cvtColor 
from cv2 import COLOR_BGR2RGB, HISTCMP_BHATTACHARYYA
from os.path import isfile
from os import makedirs, remove
from time import strftime
from sys import platform
from moviepy.editor import VideoFileClip, AudioFileClip
import numpy as np

from bot_env.mod_log import Logger

class Kframes:
    
    countInstance=0
    
    def __init__(self,
                 logger: Logger,
                 ):
        Kframes.countInstance += 1
        self.countInstance = Kframes.countInstance
        #
        self.Logger = logger
        #
        self._print()

    # выводим № объекта
    def _print(self):
        print(f'\n[Kframes] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'\n[Kframes] countInstance: [{self.countInstance}]\n')
        msg = (f"Started at {strftime('%X')}\n"
              f'platform: [{platform}]\n'
              f'\nАргументы:\n'
              f'folder_logfile: {self.Logger.folder_logfile}\n'
              f'logfile: {self.Logger.logfile}\n'
              f'loglevel: {self.Logger.loglevel}\n'
              )
        print(msg)
        self.Logger.log_info(msg)

    # синхронная обертка для безопасного выполнения методов
    def safe_execute(self, func: Callable[..., Any], name_func: str = None):
        try:
            return func()
        except Exception as eR:
            print(f'\nERROR[Kframes {name_func}] ERROR: {eR}')
            self.Logger.log_info(f'\nERROR[Kframes {name_func}] ERROR: {eR}')
            return None
    
    # создаем директорию, если такой папки нет
    def create_directory(self, paths: list[str]):
        """
        Создает директорию для хранения video и ключевых кадров, 
        если она не существует

         Аргументы:
        - paths: список строк, каждая из которых является путем к директории, которую необходимо создать.
        """
        _ = [self.safe_execute(makedirs(path,  exist_ok=True), 'create_directory') for path in paths]


    ### извлечение ключевых кадров
    def get_keyframes(self, video_path: str, threshold_keyframes: float) -> List[np.ndarray]:
        # список ключевых кадров
        keyframes = []
        cap = self.safe_execute(VideoCapture(video_path), 'get_keyframes VideoCapture')
        ret, prev_frame = cap.read()
        prev_hist = self.safe_execute(calcHist([prev_frame], 
                                               [0, 1, 2], 
                                               None, 
                                               [8, 8, 8], 
                                               [0, 256, 0, 256, 0, 256]), 
                                               'get_keyframes calcHist prev_hist')
        while True:
            ret, frame = cap.read()
            if not ret:
                break
            # гистограмма текущего кадра
            curr_hist = self.safe_execute(calcHist([frame], 
                                                   [0, 1, 2], 
                                                   None, 
                                                   [8, 8, 8], 
                                                   [0, 256, 0, 256, 0, 256]), 
                                                   'get_keyframes calcHist curr_hist')
            # сравниваем предыдущую и текущую гистограммы
            hist_diff = self.safe_execute(compareHist(prev_hist, curr_hist, HISTCMP_BHATTACHARYYA), 'get_keyframes compareHist')
            # сравнение с пороговым значением ключевого кадра
            if hist_diff > threshold_keyframes:
                rgb_frame = cvtColor(frame, COLOR_BGR2RGB)
                keyframes.append(rgb_frame)
            # текущий отработанный кадр становится предыдущим
            prev_hist = curr_hist

        cap.release()

        if not len(keyframes):
            print(f'\n[Delogo get_keyframes] \n'
                  f'При пороге {threshold_keyframes} '
                  f'нет ключевых кадров в файле: \n{video_path}. \n'
                  f'Надо уменьшить порог определения ключевых кадров или '
                  f'это видео не имеет их вообще')
            return []
        return keyframes
    
