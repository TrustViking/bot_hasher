


from typing import Union, Set, Tuple, List, Optional, Any, Dict
from cv2 import VideoCapture, VideoWriter 
from cv2 import inpaint 
from cv2 import INPAINT_TELEA, CAP_PROP_FPS, CAP_PROP_FRAME_COUNT, CAP_PROP_FRAME_WIDTH, CAP_PROP_FRAME_HEIGHT
from os.path import isfile
from os import makedirs, remove
from time import strftime
from sys import platform
from moviepy.editor import VideoFileClip, AudioFileClip
import numpy as np
from sqlalchemy.engine.result import Row

from bot_env.mod_log import LogBot 
from bot_env.decorators import safe_execute


class Delogo:
    
    countInstance=0
    
    def __init__(self,
                 logger: LogBot,
                 ):
        Delogo.countInstance += 1
        self.countInstance = Delogo.countInstance
        self.cls_name = self.__class__.__name__
        self.logger = logger
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


    # Отделение аудио и сохранение во временный файл
    def  separating_audio (self, full_path_to_video: str):
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _separating_audio():

            with VideoFileClip(full_path_to_video) as video_clip:
                audio_clip = video_clip.audio
                full_path_audio = full_path_to_video + '.ogg'
                audio_clip.write_audiofile(full_path_audio)
            
            return full_path_audio
        return _separating_audio()


    # Чтение 'video' с диска в буфер 
    def video_to_buffer (self, full_path_to_video: str):
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _video_to_buffer():
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
        return _video_to_buffer()
  

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


    # определяем координаты маски
    def mask_coordinates(self, number_corner: str, width: int, height: int, logo_size: int):
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _mask_coordinates():
            if number_corner=='1':
                print(f'\n[{__name__}|{self.cls_name}] Удаляем лого из левого верхнего угла \n'
                    f'\nNumber_corner: [{number_corner}')
                # Координаты верхнего левого угла квадрата маски
                x1, y1 = 0, 0
                # Координаты нижнего правого угла квадрата маски
                x2, y2 = logo_size, logo_size
            elif number_corner=='2':
                print(f'\n[{__name__}|{self.cls_name}] Удаляем лого из правого верхнего угла \n'
                    f'\nNumber_corner: [{number_corner}')
                # Координаты верхнего левого угла квадрата маски
                x1, y1 = width-logo_size, 0
                # Координаты нижнего правого угла квадрата маски
                x2, y2 = width, logo_size
            elif number_corner=='3':
                print(f'\n[{__name__}|{self.cls_name}] Удаляем лого из правого нижнего угла \n'
                    f'\nNumber_corner: [{number_corner}')
                # Координаты верхнего левого угла квадрата маски
                x1, y1 = width-logo_size, height-logo_size
                # Координаты нижнего правого угла квадрата маски
                x2, y2 = width, height
            elif number_corner=='4':
                print(f'\n[{__name__}|{self.cls_name}] Удаляем лого из левого нижнего угла \n'
                    f'\nNumber_corner: [{number_corner}')
                # Координаты верхнего левого угла квадрата маски
                x1, y1 = 0, height-logo_size
                # Координаты нижнего правого угла квадрата маски
                x2, y2 = logo_size, height
            else: 
                print(f'\nERROR [{__name__}|{self.cls_name}] не правильно определили number_corner: [{number_corner}]')
                return None
            return {'x1':x1, 'y1':y1, 'x2':x2, 'y2':y2}
        return _mask_coordinates()

    
    # удаляем и восстанавливаем маску на видео
    def remove_restore_mask(self, buffer: dict, coords: dict, out_file: VideoWriter):
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _remove_restore_mask():
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
                        print(f'\n[{__name__}|{self.cls_name}] отработали 10% кадров')
                    if count_frame==int(0.3*frame_count):
                        print(f'\n[{__name__}|{self.cls_name}] отработали 30% кадров')
                    if count_frame==int(0.5*frame_count):
                        print(f'\n[{__name__}|{self.cls_name}] отработали 50% кадров')
                    if count_frame==int(0.8*frame_count):
                        print(f'\n[{__name__}|{self.cls_name}] отработали 80% кадров')
                    # Создание маски для области логотипа
                    # mask[y1:y2, x1:x2] = 255
                    mask = np.zeros_like(frame[:, :, 0])
                    mask[coords.get('y1'):coords.get('y2'), 
                        coords.get('x1'):coords.get('x2')] = 255
                    # Применение инпейнтинга для удаления логотипа
                    result = inpaint(frame, mask, inpaintRadius=15, flags=INPAINT_TELEA)
                    out_file.write(result)
            except Exception as eR:
                print(f"\nERROR [{__name__}|{self.cls_name}] ERROR инпейнтинг для удаления логотипа: {eR}")
                self.logger.log_info(f"\nERROR [{__name__}|{self.cls_name}] ERROR инпейнтинг для удаления логотипа: {eR}")
                return None
            finally:
                    if buffer: 
                        buffer.release()
                    if out_file: 
                        out_file.release()
            return out_file
        return _remove_restore_mask()


    # склеиваем видео без лого и звука
    def link_audio_video(self, 
                         full_path_audio: str, 
                         full_path_noaudio: str, 
                         full_path_to_video: str,
                         bitrate: str,
                         audio_bitrate: str) -> str:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _link_audio_video():
            with AudioFileClip(full_path_audio) as audio_clip:
                # загружаем видео без лого в MoviePy
                with VideoFileClip(full_path_noaudio) as video_clip:
                    # Сохраняем видео без лого + аудио
                    video_clip = video_clip.set_audio(audio_clip)
                    video_clip.write_videofile(filename=full_path_to_video, 
                                                bitrate=bitrate, 
                                                audio_bitrate=audio_bitrate)
            return full_path_to_video
        return _link_audio_video()



    ### ОСНОВНОЙ метод удаления лого
    def  delogo (self, full_path_to_video: str, logo_size: int, corner: str = '2') -> str:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _del_logo():
            
            print(f'\n[{__name__}|{self.cls_name}] Удаляем лого с файла: {full_path_to_video}\n')
            # Отделение аудио и сохранение во временный файл
            full_path_audio = self.separating_audio(full_path_to_video)
            if not full_path_audio:
                print(f'\nERROR[{__name__}|{self.cls_name}] не отделили звук от видео full_path_audio: {full_path_audio}')
                self.logger.log_info(f'\nERROR[{__name__}|{self.cls_name}] не отделили звук от видео full_path_audio: {full_path_audio}')
                return None
            
            # Чтение 'video' с диска в буфер
            # {'buffer':video, 'fps':fps, 'fourcc':fourcc, 'frame_count':frame_count, 
            # 'frame_size':frame_size, 'width':width, 'height':height} 
            diction_buffer = self.video_to_buffer(full_path_to_video)
            if isinstance(diction_buffer, dict) and diction_buffer:
                # удаляем с диска файл с лого 
                self.delete_files([full_path_to_video])
            else:
                print((f'\nERROR[{__name__}|{self.cls_name}] не прочитали video с диска в буфер. diction: {diction_buffer}')) 
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
                print((f'\nERROR[{__name__}|{self.cls_name}] не наложили маску. out_file_mask: {out_file_mask}')) 
                return None
            full_path_saving_video_nologo = self.link_audio_video(full_path_audio, 
                                                                    full_path_noaudio, 
                                                                    full_path_to_video,
                                                                    bitrate="8000k",
                                                                    audio_bitrate="384k")
            if not full_path_saving_video_nologo:
                print(f"\nERROR [{__name__}|{self.cls_name}] ERROR не записали новый файл без логотипа: full_path_saving_video_nologo: {full_path_saving_video_nologo}")
                return None
            
            # удаляем временные файлы
            self.delete_files([full_path_noaudio, full_path_audio])        
            print(f'\n[{__name__}|{self.cls_name}] logo удалили: \n{full_path_saving_video_nologo}\n')
            
            return full_path_saving_video_nologo
        return _del_logo()


    ### Обработка результатов ОСНОВНОГО метод удаления лого
    def delete_logo(self, row: Row)-> Optional[Tuple[str, str]]:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def  _delete_logo():
            
            path_first=str(row.path_file_first)
            path_second=str(row.path_file_second)

            if str(row.withoutlogo)!='no' and str(row.withoutlogo)!='yes':
                print(f'\n[{__name__}|{self.cls_name}] ERROR row.withoutlogo: {str(row.withoutlogo)}\n')
                return None
            
            elif str(row.withoutlogo)=='no':
                print(f'\n[{__name__}|{self.cls_name}] Лого не будем удалять\n')
                return path_first, path_second 

            elif str(row.withoutlogo)=='yes':
                paths = [path_first, path_second]
                logo_size = str(row.withoutlogo)
                corner = str(row.withoutlogo)
                # удаляем лого
                new_paths = [self.delogo(path, logo_size, corner) for path in paths]
                if not new_paths:
                    print(f'\nERROR[{__name__}|{self.cls_name}] ERROR лого не удалили c таких файлов: {paths}')
                    return None
                return tuple(new_paths)
        return _delete_logo()

