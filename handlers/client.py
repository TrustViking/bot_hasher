

from typing import Coroutine
from time import time, strftime
from sys import platform, argv, path
from os import getenv, makedirs
from os.path import join, dirname, exists, basename, abspath, isfile
from psutil import virtual_memory
from pyrogram import Client
from aiogram import Bot, Dispatcher, Router,  F
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types.message import Message
from aiogram.types.callback_query import CallbackQuery
from aiogram.filters import Command, CommandStart
from aiogram.types import ReplyKeyboardRemove

#
from bot_env.bot_init import LogInitializer, BotInitializer, ConfigInitializer
from bot_env.decorators import safe_await_execute, safe_execute
from bot_env.mod_log import Logger
from data_base.table_db import DiffTable
from data_base.base_db import MethodDB
from keyboards.client_kb import KeyBoardClient

# класс машины состояний
class Form(StatesGroup):
    first_video = State()
    second_video = State()
    hash_factor = State()
    threshold_keyframes = State()
    #
    withoutlogo = State()
    number_corner = State()
    logo_size = State()

class HandlersBot(ConfigInitializer):
    """
    Создаем для telegram-bot хэндлеры клиента:

    Аргументы:
    - logger: Logger
    """
    countInstance=0
    #
    def __init__(self, 
                config_path: str,
                logger: Logger,
                bot: Bot, 
                dp: Dispatcher,
                router:  Router,
                method_db: MethodDB,

                    ): 

        HandlersBot.countInstance+=1
        self.countInstance=HandlersBot.countInstance
        self.countHandlers=0
        self.cls_name = self.__class__.__name__
        # self.abspath = dirname(abspath(__file__))
        #
        # config
        self.config = self.read_config(config_path)
        self.folder_video = self.config.get('folder_video') 
        self.folder_kframes = self.config.get('folder_kframes') 
        self.size_limit = self.config.get('size_limit_video') # 500Mb
        # Logger
        self.logger = logger
        # Bot, Dispatcher, Router
        self.bot = bot
        self.dp = dp
        self.router = router
        # MethodDB
        self.method_db=method_db
        #
        # KeyBoardClient
        self.kb = KeyBoardClient(self.logger)
        # self.value_callback = [v for k, v in self.kb.name_button.items()]
        #
        # api pyrogram
        self.api_id = getenv('TELEGRAM_API_ID')
        self.api_hash = getenv('TELEGRAM_API_HASH')
        # словарь - данные строки таблицы
        self.diction={}
        #
        self.path_save_vid=join(path[0], self.folder_video)
        self.path_save_keyframe=join(path[0], self.folder_kframes)
        self.create_directory([self.path_save_vid, self.path_save_keyframe])
        self._new_client()
        #
        #
    # выводим № объекта
    def _print(self):
        msg = (
            f"\nStarted at {strftime('%X')}\n"
            f'[{__name__}|{self.cls_name}] countInstance: [{self.countInstance}]\n'
            f'platform: [{platform}]\n'
            f'\nAttributes:\n'
            )

        attributes_to_print = [
            'cls_name',
            'abspath',
            'config_path',
            'folder_video',
            'folder_kframes',
            'size_limit',
            'logger',
            'bot',
            'dp',
            'router',
            'name_table',
            'method_db',
            'kb',
            'path_save_vid',
            'path_save_keyframe',

        ]

        for attr in attributes_to_print:
            # "Attribute not found" будет выведено, если атрибут не существует
            value = getattr(self, attr, "Attribute not found")  
            msg += f"{attr}: {value}\n"

        print(msg)
        self.logger.log_info(msg)


    # New Client
    def _new_client(self):
        print(f'\n[{__name__}|{self.cls_name}] Client# {self.countInstance}')
        self.logger.log_info(f'[_new_client] Client# {self.countInstance}')

    # New handlers
    def _new_handlers(self, name_handler=None):
        self.countHandlers+=1
        print(f'\n[{__name__}|{self.cls_name}] Handler {name_handler} # {self.countHandlers}')
        self.logger.log_info(f'[_new_handlers] Handler# {self.countHandlers}')
    #
    # создаем директорию, если такой папки нет
    def create_directory(self, paths: list[str]):
        """
        Создает директорию для хранения video и ключевых кадров, 
        если она не существует

         Аргументы:
        - paths: список строк, каждая из которых является путем к директории, которую необходимо создать.
        """
        _ = [makedirs(path,  exist_ok=True) for path in paths]


    # обрабатывает команду пользователя - /start
    async def command_start(self, message: Message, state: FSMContext):
        @safe_await_execute (logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _command_start():
            # Устанавливаем пользователю состояние "ждем первое видео"
            await state.set_state(Form.first_video)
            #  тестовое сообщение и убираем предыдущую клавиатуру   
            await self.bot.send_message(message.from_user.id, text=f'Прислали: {message.content_type}\n', reply_markup=ReplyKeyboardRemove())

            msg = (f'Будем сравнивать два видеофайла на уникальность \n'
                f'Пришлите в этот чат *первое*\ видео формата *[mp4]*\ \n'
                f'Размером не более *[500 Mб]*\  \n'
                f'Длительностью не более *[10 минут]*\ \n')
            await self.bot.send_message(message.from_user.id, msg, parse_mode='MarkdownV2')  
        return await _command_start()

    # обработчик любого сообщения, кроме  - /start
    async def any2start(self, message: Message, state: FSMContext):
        @safe_await_execute (logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _any2start():
            await self.bot.send_message(message.from_user.id, text=f'Прислали: {message.content_type}\n')
            msg = (f'Наберите команду [/start] для начала')
            await self.bot.send_message(message.from_user.id, msg)
            await state.finish()    
        return await _any2start()

    # Функция, которая будет вызываться для отображения прогресса
    def progress_bar(self, current, total):
        print(f"\rПрогресс скачивания: {current * 100 / total:.1f}%", end="", flush=True)
    
    ## обработчик первого видео
    async def process_first_video(self, message: Message, state: FSMContext):
        @safe_await_execute (logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _process_first_video():
            self._new_handlers('process_first_video')
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
                f'file_size: {file_size_format} bytes\n'
                f'mime_type: {mime}\n'
                f'Начинаю скачивать видео...'
                )
            await self.bot.send_message(message.from_user.id, msg)
            
            # первое видео
            directory = join(self.path_save_vid, str(message.video.file_id) + '.mp4')
            #
            async with Client('F16', api_id=self.api_id, api_hash=self.api_hash, takeout=True) as app:
                full_saving_path = await app.download_media(message.video, directory, progress=self.progress_bar)
                # print(f'\n[Handlers4bot process_first_video] full_saving_path: {full_saving_path} \n')
            await bot.send_message(message.chat.id, f"\nПервое видео скачано.\nСсылка: {full_saving_path}")  
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
    async def process_second_video(self, message: Message, state: FSMContext):
        self._new_handlers('process_first_video')
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
        
        # второе видео
        directory = join(self.path_save_vid, str(message.video.file_id) + '.mp4')
        #
        async with Client('F16', api_id=self.api_id, api_hash=self.api_hash) as app:
            full_saving_path = await app.download_media(message.video, directory, progress=self.progress_bar)
            # print(f'\n[Handlers4bot process_second_video] full_saving_path: {full_saving_path} \n')
        # await bot.send_message(message.chat.id, f"\nВторое видео скачано.\nСсылка: {full_saving_path} \nРезультаты анализа пришлю сюда")  
        msg = (f'\nВторое видео скачано.\nСсылка: {full_saving_path} \n'
               f'\nВыберите значение порога схожести кадров от 0.1 до 0.9')
        await bot.send_message(message.chat.id, msg, 
                               reply_markup=self.kb.kb_hash_factor(5).as_markup(resize_keyboard=True))  
        
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
        # await self.db.insert_data('diff', self.diction)
        # # выводим таблицу task
        # await self.db.print_data('diff')
        # снимаем состояние 
        # await state.finish()
        # Устанавливаем пользователю состояние 'hash_factor'
        await state.set_state(Form.hash_factor)    

    ## обработчик hash_factor
    async def process_hash_factor(self, callback: CallbackQuery, state: FSMContext):
        #
        data = callback.data
        print(f'\n[Handlers4bot process_hash_factor] callback.data: {data}')
        self.diction['hash_factor']=data
        msg = (f'\nВыбрали значение порога схожести кадров [{data}]\n'
               f'\nВыберите значение порога определения ключевых кадров')
        await bot.send_message(callback.from_user.id, msg, 
                        reply_markup=self.kb.kb_threshold_keyframes(5).as_markup())  

        # Устанавливаем пользователю состояние 'hash_factor'
        await state.set_state(Form.threshold_keyframes)    

    ## обработчик threshold_keyframes
    async def process_threshold_keyframes(self, callback: CallbackQuery, state: FSMContext):
        #
        data = callback.data
        print(f'\n[Handlers4bot process_threshold_keyframes] callback.data: {data}')
        self.diction['threshold_kframes']=data
        msg = (f'\nВыбрали значение порога определения ключевых кадров [{data}]\n')
        await bot.send_message(callback.from_user.id, msg, 
                        reply_markup=self.kb.kb_withoutlogo(2).as_markup())  
        
        # Устанавливаем пользователю состояние 'withoutlogo'
        await state.set_state(Form.withoutlogo)    

    ## обработчик withoutlogo_yes
    async def process_withoutlogo_yes(self, callback: CallbackQuery, state: FSMContext):
        #
        data = callback.data
        print(f'\n[Handlers4bot process_withoutlogo_yes] callback.data: {data}')
        self.diction['withoutlogo']=data
        msg = (f'\nВыбрали удалять лого [{data}]\n')
        await bot.send_message(callback.from_user.id, msg, 
                        reply_markup=self.kb.kb_number_corner(4).as_markup())  
        # Устанавливаем пользователю состояние 'number_corner'
        await state.set_state(Form.number_corner)    
   
    ## обработчик withoutlogo_no
    async def process_withoutlogo_no(self, callback: CallbackQuery, state: FSMContext):
        #
        data = callback.data
        print(f'\n[Handlers4bot process_withoutlogo_no] callback.data: {data}')
        self.diction['withoutlogo']=data
        msg = (f'\nВыбрали не удалять лого [{data}]\n')
        await bot.send_message(callback.from_user.id, msg)
        #
        # записываем строку (словарь значений) в БД
        if not await self.db.insert_data('diff', self.diction):
            msg = (f'\nERROR не смогли записать введенные данные в БД.\n'
                   f'\nНадо ввести данные заново. \nВведите любой символ\n')
            res = await bot.send_message(callback.from_user.id, msg)
            print(res)
        self.diction = {}
        # выводим таблицу task
        await self.db.print_tables('diff')
        # снимаем состояние 
        await state.finish()


    ## обработчик number_corner
    async def process_number_corner(self, callback: CallbackQuery, state: FSMContext):
        #
        data = callback.data
        print(f'\n[Handlers4bot process_number_corner] callback.data: {data}')
        self.diction['number_corner']=data
        msg = (f'\nВыбрали удалять лого в углу №{data}\n'
               f'\nПришлите в чат размер маски удаления лого в пикселях (рекомендуется 180) не более 300')
        await bot.send_message(callback.from_user.id, msg)
        # Устанавливаем пользователю состояние 'logo_size'
        await state.set_state(Form.logo_size)    

    ## обработчик logo_size
    async def process_logo_size(self, callback: CallbackQuery, state: FSMContext):
        #
        data = callback.data
        try:
            if 0<int(data)<300:
                data = int(data)
        except Exception as eR:
            msg = (f'\nЭто не число от 0 до 300\n'
                    f'\nВы ввели: {data} \n')
        
        print(f'\n[Handlers4bot process_logo_size] callback.data: {data}')
        self.diction['logo_size']=data
        msg = (f'\nРазмер маски удаления лого выбрали [{data}] пикселей\n'
               f'\nРезультат работы пришлю сюда\n')
        await bot.send_message(callback.from_user.id, msg)  
        #
        # записываем строку (словарь значений) в БД
        if not await self.db.insert_data('diff', self.diction):
            msg = (f'\nERROR не смогли записать введенные данные в БД.\n'
                   f'\nНадо ввести данные заново. \nВведите любой символ\n')
            res = await bot.send_message(callback.from_user.id, msg)
            print(res)
        self.diction = {}
        # выводим таблицу task
        await self.db.print_tables('diff')
        # снимаем состояние 
        await state.finish()


    # регистрация хэндлеров
    async def register_handlers_client(self):
        # обрабатываем нажатие кнопки СТАРТ 
        # self.dp.message.register(self.command_start, Command('start'))
        self.dp.message.register(self.command_start, CommandStart(ignore_mention=True))

        # обрабатываем первое видео 
        self.dp.message.register(self.process_first_video, F.video, state=Form.first_video)
        
        # обрабатываем второе видео 
        self.dp.message.register(self.process_second_video, F.video, state=Form.second_video)

        # обрабатываем hash_factor 
        self.dp.callback_query.register(self.process_hash_factor, F.data.in_(self.value_callback), state=Form.hash_factor)

        # обрабатываем threshold_keyframes 
        self.dp.callback_query.register(self.process_threshold_keyframes, F.data.in_(self.value_callback), state=Form.hash_factor)

        # обрабатываем process_withoutlogo_yes 
        self.dp.callback_query.register(self.process_withoutlogo_yes, F.data=='yes', state=Form.withoutlogo)
        
        # обрабатываем process_withoutlogo_no 
        self.dp.callback_query.register(self.process_withoutlogo_no, F.data=='not', state=Form.withoutlogo)

        # обрабатываем process_withoutlogo_s 
        self.dp.callback_query.register(self.process_number_corner, F.data.in_(['1', '2', '3', '4']), state=Form.number_corner)
        # обрабатываем process_withoutlogo_yes 

        self.dp.callback_query.register(self.process_logo_size, F.data.content_type.text, state=Form.logo_size)
        # обрабатываем process_withoutlogo_yes 

        # любые сообщения и на старт
        self.dp.message.register(self.any2start)
        self.router.message.register(self.any2start, F.text)


