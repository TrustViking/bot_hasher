
import os, sys
from pyrogram import Client
from PIL import Image
import numpy as np
import hashlib
from time import time
from aiogram import types
from aiogram.types.message import ContentType
from io import BytesIO
import magic
from aiogram.dispatcher.filters import Command
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher import FSMContext
from bot_env.mod_log import Logger
from bot_env.create_obj4bot import bot, dp, token
from data_base.base_db import BaseDB
from videomaker.comparator import VidCompar

# класс машины состояний
class Form(StatesGroup):
    first_video = State()
    second_video = State()

class Handlers4bot:
    """
    Создаем для telegram-bot хэндлеры клиента:

    Аргументы:
    - logger: Logger
    """
    countInstance=0
    #
    def __init__(self, 
                 logger: Logger,
                 folder_video= 'diff_video',
                 folder_kframes = 'diff_kframes',
                 size_limit = 524288000,
                    ): 

        Handlers4bot.countInstance+=1
        self.countInstance=Handlers4bot.countInstance
        self.countHandlers=0
        # api pyrogram
        self.api_id = os.getenv('TELEGRAM_API_ID')
        self.api_hash = os.getenv('TELEGRAM_API_HASH')
        # api BOT
        self.bot=bot
        self.dp=dp
        self.token=token
        #
        self.path_save_vid=os.path.join(sys.path[0], folder_video)
        self.path_save_keyframe=os.path.join(sys.path[0], folder_kframes)
        self.size_limit = size_limit # 500Mb
        #
        self.Logger = logger
        self.Db=BaseDB(logger=self.Logger)
        self.diction={}
        #
        self._new_client()
        self._create_save_directory()
        #
        #
    # New Client
    def _new_client(self):
        print(f'[_new_client] Client# {self.countInstance}')
        self.Logger.log_info(f'[_new_client] Client# {self.countInstance}')

    # New handlers
    def _new_handlers(self, name_handler=None):
        self.countHandlers+=1
        print(f'\n\n[_new_handlers] Handler {name_handler} # {self.countHandlers}')
        self.Logger.log_info(f'[_new_handlers] Handler# {self.countHandlers}')
    #
    def _create_save_directory(self):
        """
        Создает директорию для хранения video, если она не существует
        """
        if not os.path.exists(self.path_save_vid):
            os.makedirs(self.path_save_vid)
        #
        if not os.path.exists(self.path_save_keyframe):
            os.makedirs(self.path_save_keyframe)

    # обертка для безопасного выполнения методов
    # async def safe_execute(self, coroutine: Callable[..., Coroutine[Any, Any, T]]) -> T:
    async def safe_await_execute(self, coroutine, name_func: str = None):
        try:
            # print(f'\n***Handlers4bot safe_execute: выполняем обертку ****')
            return await coroutine
        except Exception as eR:
            print(f'\nERROR[Handlers4bot {name_func}] ERROR: {eR}') 
            self.Logger.log_info(f'\nERROR[Handlers4bot {name_func}] ERROR: {eR}') 
            return None


    # обрабатывает команду пользователя - /start
    async def command_start(self, message: types.Message, state: FSMContext): 
        # await Form.first_video.set()
        msg = (f'Будем сравнивать два видеофайла на уникальность \n'
               f'Пришлите в этот чат *первое*\ видео формата *[mp4]*\ \n'
               f'Размером не более *[500 Mб]*\  \n'
               f'Длительностью не более *[10 минут]*\ \n')
        await self.bot.send_message(message.from_user.id, msg, parse_mode='MarkdownV2')  
        # Устанавливаем пользователю состояние "ждем первое видео"
        await state.set_state(Form.first_video)    
    
    # обработчик любого сообщения, кроме  - /start
    async def any2start(self, message: types.Message, state: FSMContext):
        await self.bot.send_message(message.from_user.id, text=f'Прислали: {message.content_type}\n')
        msg = (f'Наберите команду [/start] для начала')
        await self.bot.send_message(message.from_user.id, msg)
        await state.finish()    

    # Функция, которая будет вызываться для отображения прогресса
    def progress_bar(self, current, total):
        print(f"\rПрогресс скачивания: {current * 100 / total:.1f}%", end="", flush=True)
    
    ## обработчик первого видео
    async def process_first_video(self, message: types.Message, state: FSMContext):
        self._new_handlers('process_first_video')
        self._create_save_directory()
        #
        file_id=str(message.video.file_id)
        file_size=str(message.video.file_size)
        number = int(file_size)
        file_size_format = f"{number:,}".replace(",", " ")
        mime = str(message.video.mime_type).split('/')[1]
        print(f'\n[Handlers4bot process_first_video] \nfile_id: {file_id} \nfile_size: {file_size} \nmime: {mime}')
        #
        if  mime != 'mp4':
            msg = (f'ERROR Вы ввели видео формата [{mime}] \n'
                   f'Введите корректный формат видео - [mp4]. \n')
            print(f'\n[Handlers4bot process_first_video] msg: {msg} \n')
            await self.bot.send_message(message.from_user.id, msg)  
        #
        if  int(file_size) > self.size_limit:
            msg = (f'ERROR Вы ввели видео размером: [{int(file_size)}] байт \n'
                   f'Введите видео меньшего размера  \n')
            print(f'\n[Handlers4bot process_first_video] msg: {msg} \n')
            await self.bot.send_message(message.from_user.id, msg)  
        #
        msg = (f'Первое видео:\n'
               f'width: {str(message.video.width)} px\n'
               f'height: {str(message.video.height)} px\n'
               f'duration: {str(message.video.duration)} sec\n'
               f'file_size: {file_size_format} b\n'
               f'mime_type: {mime}\n'
               f'Начинаю скачивать видео...'
               )
        await self.bot.send_message(message.from_user.id, msg)
        
        # скачивание видео
        directory = os.path.join(self.path_save_vid, str(message.video.file_id) + '.mp4')
        async with Client('F16', api_id=self.api_id, api_hash=self.api_hash, takeout=True) as app:
            full_saving_path = await app.download_media(message.video, directory, progress=self.progress_bar)
            # print(f'\n[Handlers4bot process_first_video] full_saving_path: {full_saving_path} \n')
        
        await bot.send_message(message.chat.id, "\nПервое видео скачано!")  
        # 
        msg = (f'\nТеперь отправьте второе видео.\n')
        await self.bot.send_message(message.from_user.id, msg)  
        # формируем строку таблицы task 
        self.diction={
            'date_message' : str(message.date),
            'chat_id' : str(message.chat.id),
            'username' : str(message.from_user.username),
            'video_id_first' : file_id,
            'width_first': str(message.video.width),
            'height_first': str(message.video.height),
            'duration_first': str(message.video.duration),
            'mime_type_first': mime,
            'file_size_first': file_size,
            'path_file_first': directory,
                        }
        # Устанавливаем пользователю состояние "ждем второе видео"
        await state.set_state(Form.second_video)    

    ## обработчик второго видео
    async def process_second_video(self, message: types.Message, state: FSMContext):
        self._new_handlers('process_first_video')
        self._create_save_directory()
        #
        file_id=str(message.video.file_id)
        file_size=str(message.video.file_size)
        number = int(file_size)
        file_size_format = f"{number:,}".replace(",", " ")
        mime = str(message.video.mime_type).split('/')[1]
        #
        if  mime != 'mp4' or int(message.video.file_size)>self.size_limit:
            msg = (f'ERROR Введите корректное видео исходя из вышеуказанных требований. \n'
                  f'Вы ввели видео формата [{mime}] и размером: [{str(message.video.file_size)}] байт')
            print(f'\n[Handlers4bot process_first_video] msg: {msg} \n')
            await self.bot.send_message(message.from_user.id, msg)  
        #
        #
        msg = (f'Второе видео:\n'
               f'width: {str(message.video.width)} px\n'
               f'height: {str(message.video.height)} px\n'
               f'duration: {str(message.video.duration)} sec\n'
               f'mime_type: {mime}\n'
               f'file_size: {file_size_format} b\n'
               f'Начинаю скачивать второе видео...'
               )
        await self.bot.send_message(message.from_user.id, msg)
        
        # скачивание видео
        directory = os.path.join(self.path_save_vid, str(message.video.file_id) + '.mp4')
        async with Client('F16', api_id=self.api_id, api_hash=self.api_hash) as app:
            full_saving_path = await app.download_media(message.video, directory, progress=self.progress_bar)
            print(f'\n[Handlers4bot process_second_video] full_saving_path: {full_saving_path} \n')
        
        await bot.send_message(message.chat.id, "\nВторое видео скачано! \nРезультаты анализа пришлю сюда")  
        # формируем вторую часть строки таблицы task
        self.diction['time_task']=int(time())
        self.diction['video_id_second']=file_id 
        self.diction['width_second']=str(message.video.width) 
        self.diction['height_second']=str(message.video.height) 
        self.diction['duration_second']=str(message.video.duration) 
        self.diction['mime_type_second']=mime 
        self.diction['file_size_second']=file_size 
        self.diction['path_file_second']=directory 
        self.diction['dnld']='dnlded'
        self.diction['in_work']='not_diff'
        #
        self.diction['num_kframe_1']='?_kframe'
        self.diction['num_kframe_2']='?_kframe'
        self.diction['result_kframe']='?_kframe'
        self.diction['result_diff']='?_similar'
        self.diction['num_similar_pair']='?_similar'
        self.diction['save_sim_img']='not_save'
        self.diction['path_sim_img']='not_path'
        self.diction['sender_user']='not_sender'
        # записываем словарь значений в таблицу task 
        await self.Db.insert_data('diff', self.diction)
        # выводим таблицу task
        await self.Db.print_data('diff')
        # снимаем состояние 
        await state.finish()

    # регистрация хэндлеров
    async def register_handlers_client(self):
        # обрабатываем нажатие кнопки СТАРТ 
        self.dp.register_message_handler(self.command_start, Command('start'))

        # обрабатываем первое видео 
        self.dp.register_message_handler(self.process_first_video, content_types=ContentType.VIDEO, state=Form.first_video)
        
        # обрабатываем второе видео 
        self.dp.register_message_handler(self.process_second_video, content_types=ContentType.VIDEO, state=Form.second_video)
        
        # любые сообщения и на старт
        self.dp.register_message_handler(self.any2start, content_types=ContentType.ANY, state='*')


    # отправка пользователю пары схожих ключевых кадров
    # async def send_user_image(self, path_folder: str, nfile: str, chat_id: str):
    #     #
    #     # with open(path, 'rb') as photo:
    #     #     await self.bot.send_photo(chat_id, photo)
    #     full_path = os.path.join(path_folder, nfile)
    #     return await self.safe_await_execute(self.bot.send_photo(chat_id=chat_id, 
    #                               photo=full_path, 
    #                               caption=nfile[:-4],
    #                               ), 'send_user_image')
        



