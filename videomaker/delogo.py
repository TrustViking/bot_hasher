

from typing import Callable, Any
from cv2 import VideoCapture, VideoWriter 
from cv2 import inpaint 
from cv2 import INPAINT_TELEA, CAP_PROP_FPS, CAP_PROP_FRAME_COUNT, CAP_PROP_FRAME_WIDTH, CAP_PROP_FRAME_HEIGHT
from os.path import isfile
from os import makedirs, remove
from time import strftime
from sys import platform
from moviepy.editor import VideoFileClip, AudioFileClip
import numpy as np

from bot_env.mod_log import Logger

class Delogo:
    
    countInstance=0
    
    def __init__(self,
                 logger: Logger,
                 ):
        Delogo.countInstance += 1
        self.countInstance = Delogo.countInstance
        #
        self.Logger = logger
        #
        self._print()

    # выводим № объекта
    def _print(self):
        print(f'\n[Delogo] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'\n[Delogo] countInstance: [{self.countInstance}]\n')
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
            print(f'\nERROR[Delogo {name_func}] ERROR: {eR}')
            self.Logger.log_info(f'\nERROR[Delogo {name_func}] ERROR: {eR}')
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

    # Отделение аудио и сохранение во временный файл
    def  separating_audio (self, full_path_to_video: str):
        # print(f'\n[Delogo del_logo] Удаляем лого с файла: {full_path_to_video}\n')
        # Отделение аудио и сохранение во временный файл
        try:
            with VideoFileClip(full_path_to_video) as video_clip:
                audio_clip = video_clip.audio
                full_path_audio = full_path_to_video + '.ogg'
                audio_clip.write_audiofile(full_path_audio)
        except Exception as eR:
            print(f'\nERROR[Delogo separating_audio] ERROR: {eR}')
            self.Logger.log_info(f'\nERROR[Delogo separating_audio] ERROR: {eR}')
            return None
        return full_path_audio

    # Чтение 'video' с диска в буфер 
    def  video_to_buffer (self, full_path_to_video: str):
        video = VideoCapture(full_path_to_video)
        # данные видеоряда
        fps = int(video.get(CAP_PROP_FPS))
        fourcc = VideoWriter.fourcc(*'mp4v')
        frame_count = int(video.get(CAP_PROP_FRAME_COUNT))
        frame_size = (int(video.get(CAP_PROP_FRAME_WIDTH)), int(video.get(CAP_PROP_FRAME_HEIGHT)))
        width, height = frame_size
        msg = (f'\n[Delogo video_to_buffer] \n'
                f'\nvideo: {video} \n'
                f'fps: {fps} \n'
                f'fourcc: {fourcc} \n'
                f'frame_size: {frame_size} \n'
                f'width: {width} \n'
                f'height: {height} \n'
                f'frame_count: {frame_count}')  
        print(msg)      
        return {'buffer':video, 'fps':fps, 'fourcc':fourcc, 'frame_count':frame_count, 
                'frame_size':frame_size, 'width':width, 'height':height}
    
    # удаляем файл с диска
    def delete_files(self, paths: list[str]):
        """
        Удаляем файлы

         Аргументы:
        - paths: список строк, каждая из которых является путем к файлу, 
          который необходимо удалить.
        """
        _ = [self.safe_execute(remove(path), 'delete_files') for path in paths  if isfile(path)]

    # определяем координаты маски
    def mask_coordinates(self, number_corner: str, width: int, height: int, logo_size: int):
        if number_corner=='1':
            print(f'\n[Delogo mask_coordinates] Удаляем лого из левого верхнего угла \n'
                  f'\nNumber_corner: [{number_corner}')
            # Координаты верхнего левого угла квадрата маски
            x1, y1 = 0, 0
            # Координаты нижнего правого угла квадрата маски
            x2, y2 = logo_size, logo_size
        elif number_corner=='2':
            print(f'\n[Delogo mask_coordinates] Удаляем лого из правого верхнего угла \n'
                  f'\nNumber_corner: [{number_corner}')
            # Координаты верхнего левого угла квадрата маски
            x1, y1 = width-logo_size, 0
            # Координаты нижнего правого угла квадрата маски
            x2, y2 = width, logo_size
        elif number_corner=='3':
            print(f'\n[Delogo mask_coordinates] Удаляем лого из правого нижнего угла \n'
                  f'\nNumber_corner: [{number_corner}')
            # Координаты верхнего левого угла квадрата маски
            x1, y1 = width-logo_size, height-logo_size
            # Координаты нижнего правого угла квадрата маски
            x2, y2 = width, height
        elif number_corner=='4':
            print(f'\n[Delogo mask_coordinates] Удаляем лого из левого нижнего угла \n'
                  f'\nNumber_corner: [{number_corner}')
            # Координаты верхнего левого угла квадрата маски
            x1, y1 = 0, height-logo_size
            # Координаты нижнего правого угла квадрата маски
            x2, y2 = logo_size, height
        else: 
            print(f'\nERROR [Delogo mask_coordinates] не правильно определили number_corner: [{number_corner}]')
            return None
        return {'x1':x1, 'y1':y1, 'x2':x2, 'y2':y2}
    
    # удаляем и восстанавливаем маску на видео
    def remove_restore_mask(self, buffer: dict, 
                            coords: dict, 
                            out_file: VideoWriter):
        count_frame = 0
        buffer = buffer.get('buffer')
        frame_count = buffer.get('frame_count')
        try:
            while buffer.isOpened():
                # type(frame): <class 'numpy.ndarray'>
                # frame.shape: (1080, 1920, 3)
                ret, frame = buffer.read()
                if not ret: break
                count_frame+=1
                if count_frame==int(0.1*frame_count):
                    print(f'\n[Delogo remove_restore_mask] отработали 10% кадров')
                if count_frame==int(0.3*frame_count):
                    print(f'\n[Delogo remove_restore_mask] отработали 30% кадров')
                if count_frame==int(0.5*frame_count):
                    print(f'\n[Delogo remove_restore_mask] отработали 50% кадров')
                if count_frame==int(0.8*frame_count):
                    print(f'\n[Delogo remove_restore_mask] отработали 80% кадров')
                # Создание маски для области логотипа
                # mask[y1:y2, x1:x2] = 255
                mask = np.zeros_like(frame[:, :, 0])
                mask[coords.get('y1'):coords.get('y2'), 
                     coords.get('x1'):coords.get('x2')] = 255
                # Применение инпейнтинга для удаления логотипа
                result = inpaint(frame, mask, inpaintRadius=15, flags=INPAINT_TELEA)
                out_file.write(result)
        except Exception as eR:
            print(f"\nERROR [Delogo remove_restore_mask] ERROR инпейнтинг для удаления логотипа: {eR}")
            self.Logger.log_info(f"\nERROR [Delogo remove_restore_mask] ERROR инпейнтинг для удаления логотипа: {eR}")
            return None
        finally:
                if buffer: buffer.release()
                if out_file: out_file.release()
        return out_file

    # склеиваем видео без лого и звука
    def link_audio_video(self, 
                         full_path_audio: str, 
                         full_path_noaudio: str, 
                         full_path_to_video: str,
                         bitrate: str,
                         audio_bitrate: str):
        try:
            with AudioFileClip(full_path_audio) as audio_clip:
                # загружаем видео без лого в MoviePy
                with VideoFileClip(full_path_noaudio) as video_clip:
                    # Сохраняем видео без лого + аудио
                    video_clip = video_clip.set_audio(audio_clip)
                    video_clip.write_videofile(filename=full_path_to_video, 
                                               bitrate=bitrate, 
                                               audio_bitrate=audio_bitrate)
        except Exception as eR:
            print(f"\nERROR [Delogo remove_restore_mask] ERROR инпейнтинг для удаления логотипа: {eR}")
            self.Logger.log_info(f"\nERROR [Delogo remove_restore_mask] ERROR инпейнтинг для удаления логотипа: {eR}")
            return None
        return full_path_to_video

    ### ОСНОВНОЙ метод удаления лого
    def  del_logo (self, full_path_to_video: str, logo_size: int, corner: str = '2'):
        print(f'\n[Delogo del_logo] Удаляем лого с файла: {full_path_to_video}\n')
        # Отделение аудио и сохранение во временный файл
        full_path_audio = self.separating_audio(full_path_to_video)
        if not full_path_audio:
            print(f'\nERROR[Delogo del_logo] не отделили звук от видео full_path_audio: {full_path_audio}')
            self.Logger.log_info(f'\nERROR[Delogo del_logo] не отделили звук от видео full_path_audio: {full_path_audio}')
            return None
        
        # Чтение 'video' с диска в буфер
        # {'buffer':video, 'fps':fps, 'fourcc':fourcc, 'frame_count':frame_count, 
        # 'frame_size':frame_size, 'width':width, 'height':height} 
        diction_buffer = self.video_to_buffer(full_path_to_video)
        if isinstance(diction_buffer, dict) and diction_buffer:
            # удаляем с диска файл с лого 
            self.delete_files([full_path_to_video])
        else:
            print((f'\nERROR[Delogo del_logo] не прочитали video с диска в буфер. diction: {diction_buffer}')) 
            return None

        # определяем координаты маски
        coordinates =  self.mask_coordinates(corner, 
                                             diction_buffer.get('width'), 
                                             diction_buffer.get('height'), 
                                             logo_size)
        
        # Определение выходного файла без лого, но и без звука
        full_path_noaudio = full_path_to_video+'_noaudio.mp4'
        out_file = VideoWriter(full_path_noaudio, 
                               diction_buffer.get('fourcc'),
                               diction_buffer.get('fps'),
                               diction_buffer.get('frame_size'),
                               )
        out_file_mask = self.remove_restore_mask(diction_buffer, coordinates, out_file)
        if not out_file_mask:
            print((f'\nERROR[Delogo del_logo] не наложили маску. out_file_mask: {out_file_mask}')) 
            return None
        full_path_saving_video_nologo = self.link_audio_video(full_path_audio, 
                                                                full_path_noaudio, 
                                                                full_path_to_video,
                                                                bitrate="8000k",
                                                                audio_bitrate="384k")
        if not full_path_saving_video_nologo:
            print(f"\nERROR [Delogo del_logo] ERROR не записали новый файл без логотипа: full_path_saving_video_nologo: {full_path_saving_video_nologo}")
            return None
        
        # удаляем временные файлы
        self.delete_files([full_path_noaudio, full_path_audio])        
        print(f'\n[Delogo del_logo] logo удалили: \n{full_path_saving_video_nologo}\n')
        
        return full_path_saving_video_nologo


