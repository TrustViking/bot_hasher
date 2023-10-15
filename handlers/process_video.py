

from time import strftime
from sys import platform
from os import getenv, makedirs
from os.path import join
from pyrogram import Client
from aiogram import Bot
from aiogram.types.message import Message

from bot_env.mod_log import Logger
from bot_env.decorators import safe_await_execute



class ProcessVideo():
    """
    Скачивание с сервера telegram видео и его обработка:

    Аргументы:
    - logger: Logger
    """
    countInstance=0
    #
    def __init__(self, 
                logger: Logger,
                    ): 

        ProcessVideo.countInstance+=1
        self.countInstance=ProcessVideo.countInstance
        self.cls_name = self.__class__.__name__
        # Logger
        self.logger = logger
        # словарь - строка таблицы
        self.diction={}
        self._print()

    # выводим № объекта
    def _print(self):
        msg = (
            f"\nStarted at {strftime('%X')}\n"
            f'[{__name__}|{self.cls_name}] countInstance: [{self.countInstance}]\n'
            f'platform: [{platform}]\n'
            )

        print(msg)
        self.logger.log_info(msg)


    # создаем директорию, если такой папки нет
    def create_directory(self, paths: list[str]):
        """
        Создает директорию для хранения video и ключевых кадров, 
        если она не существует

         Аргументы:
        - paths: список строк, каждая из которых является путем к директории, 
                 которую необходимо создать.
        """
        _ = [makedirs(path,  exist_ok=True) for path in paths]


    # прогресс скачивания
    def progress_bar(self, current, total):
        print(f"\rПрогресс скачивания: {current * 100 / total:.1f}%", end="", flush=True)


    # словарь метаданных видео
    async def diction_metadata(self, message: Message):
            @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
            async def _diction_metadata():
                metadata = {}
                metadata['userid']= str(message.from_user.id)
                metadata['video']= message.video
                metadata['width']= str(message.video.width)
                metadata['height']= str(message.video.height)
                metadata['duration']= str(message.video.duration)
                metadata['file_id']= str(message.video.file_id)
                metadata['mime']= str(message.video.mime_type).split('/')[1]
                metadata['file_size']= int(message.video.file_size)
                metadata['file_size_format']= f"{metadata.get('file_size'):,}".replace(",", " ")
                return metadata
            return await  _diction_metadata()


    # проверка формата видео
    async def check_mime(self, metadata: dict, bot: Bot):
            @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
            async def _check_mime():
                mime = metadata.get('mime')
                userid = metadata.get('userid')
                if  not mime or mime != 'mp4':
                    msg = (f'ERROR Вы ввели видео формата [{mime}] \n'
                           f'Введите корректный формат видео - [mp4]. \n')
                    print(f'\n[{__name__}|{self.cls_name}] msg: {msg} \n')
                    await bot.send_message(userid, msg)
                    return None 
                else: 
                    return mime
            return await  _check_mime()


    # проверка размера видео
    async def check_size_video(self, metadata: dict, size_limit: int, bot: Bot):
            @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
            async def _check_size_video():
                file_size = metadata.get('file_size')
                userid = metadata.get('userid')
                if  file_size > size_limit:
                    msg = (f'ERROR Вы ввели видео размером: [{file_size}] байт \n'
                        f'Введите видео меньшего размера  \n')
                    print(f'\n[{__name__}|{self.cls_name}] msg: {msg} \n')
                    await bot.send_message(userid, msg)
                    return None 
                else: 
                    return file_size
            return await  _check_size_video()


    # уведомление пользователя о метаданных видео
    async def info_video(self, number_video: str, metadata: dict, bot: Bot):
            @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
            async def _info_video():
                width = metadata.get('width')
                height = metadata.get('height')
                duration = metadata.get('duration')
                file_size_format = metadata.get('file_size_format')
                mime = metadata.get('mime')
                userid = metadata.get('userid')
                
                msg =  (f'\n{number_video} видео:\n'
                        f'width: {width} px\n'
                        f'height: {height} px\n'
                        f'duration: {duration} sec\n'
                        f'file_size: {file_size_format} bytes\n'
                        f'mime_type: {mime}\n'
                        f'Начинаю скачивать...'
                        )
                
                print(f'\n[{__name__}|{self.cls_name}] msg: {msg} \n')
                await bot.send_message(userid, msg)
            return await  _info_video()


    # скачивание видео в файловую систему
    async def dnld_video(self, full_path: str, client: Client, metadata: dict, bot: Bot):
            @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
            async def _dnld_video():
                userid = metadata.get('userid')
                video = metadata.get('video')
                print(f'\n[{__name__}|{self.cls_name}] metadata.get("video"): {video}')
                # async with Client('F16', api_id=self.api_id, api_hash=self.api_hash, takeout=True) as app:
                
                async with client as app:
                    full_saving_path = await app.download_media(message=video, 
                                                                file_name=full_path, 
                                                                progress=self.progress_bar)
                if not full_saving_path:
                    msg = f"\nВидео не скачано.\nСсылка: {full_saving_path}"
                    print(msg)
                    await bot.send_message(userid, msg)  
                    return None
                
                msg = f"\nВидео скачано.\nСсылка: {full_saving_path}"
                print(msg)
                await bot.send_message(userid, msg)
                return full_saving_path 
            return await  _dnld_video()


    # обработка видео (проверка типа и размера, скачивание и информирование пользователя)
    async def process_video(self, message: Message, number_video: str, bot: Bot, client: Client, size_limit: int, path_save_vid: str):
            @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
            async def _process_video():
            
                metadata = await self.diction_metadata(message)
                if (
                    not metadata or
                    not await self.check_mime(metadata, bot) or 
                    not await self.check_size_video(metadata, size_limit, bot)
                    ):
                    return None
                # отправляем пользователю метаданные видео
                await self.info_video(number_video, metadata, bot)
                
                # полный путь для скачивания видео 
                full_path = join(path_save_vid, str(message.video.file_id) + '.mp4')
                
                # скачиваем видео Pyrogram
                full_saving_path = await self.dnld_video(full_path, client, metadata, bot)
                if not full_saving_path:
                    return None
                return full_saving_path
            return await  _process_video()



