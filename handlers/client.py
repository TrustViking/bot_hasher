

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
from aiogram.filters import CommandStart, ExceptionTypeFilter, Command, CommandObject
from aiogram.types import ReplyKeyboardRemove
from aiogram.types.error_event import ErrorEvent
from aiogram.exceptions import AiogramError
#
from bot_env.bot_init import ConfigInitializer
from bot_env.decorators import safe_await_aiogram_exe, safe_await_execute, safe_execute
from bot_env.mod_log import Logger
from data_base.table_db import DiffTable
from data_base.base_db import MethodDB
from keyboards.client_kb import KeyBoardClient
from .diction_db import DictionDB 
from .process_video import ProcessVideo 

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

    # количество колонок кнопок
    COLUMN_HASH_FACTOR = 4 
    COLUMN_THRESHOLD_KFRAMES = 4
    COLUMN_WITHOUTLOGO = 2
    COLUMN_NUMBER_CORNER = 4
    COLUMN_LOGO_SIZE = 9


    countInstance = 0
    router = Router()
    name_data_hash_factor = [str(i / 10) for i in range(1, 10)]
    name_data_threshold_keyframes = [str(i / 10) for i in range(1, 6)]
    name_withoutlogo = ['Убираем лого', 'НЕ убираем лого']
    data_withoutlogo = ['yes', 'no']
    name_data_number_corner = ['1', '2', '4', '3']
    name_data_logo_size = [str(i) for i in range(100, 310, 10)]

    #
    def __init__(self, 
                config_path: str,
                logger: Logger,
                bot: Bot, 
                dp: Dispatcher,
                method_db: MethodDB,
                    ): 

        HandlersBot.countInstance+=1
        self.countInstance=HandlersBot.countInstance
        self.countHandlers=0
        self.cls_name = self.__class__.__name__
        #
        # config
        self.config = self.read_config(config_path)
        self.folder_video = self.config.get('folder_video') 
        self.folder_kframes = self.config.get('folder_kframes') 
        self.folder_pyrogram_session = self.config.get('folder_pyrogram_session') 
        self.name_pyrogram_session = self.config.get('name_pyrogram_session') 
        self.size_limit = self.config.get('size_limit_video') # 500Mb
        # Logger
        self.logger = logger
        # Bot, Dispatcher, Router
        self.bot = bot
        self.dp = dp
        # self.router = router
        # MethodDB
        self.method_db=method_db
        # DictionDB
        self.dictionDB = DictionDB(self.logger)
        # ProcessVideo
        self.video = ProcessVideo(self.logger)
        #
        self.path_save_vid=join(path[0], self.folder_video)
        self.path_save_keyframe=join(path[0], self.folder_kframes)
        self.path_pyrogram_session=join(path[0], self.folder_pyrogram_session)
        self.create_directory([self.path_save_vid, self.path_save_keyframe, self.path_pyrogram_session])
        #
        # api pyrogram
        self.api_id = getenv('TELEGRAM_API_ID')
        self.api_hash = getenv('TELEGRAM_API_HASH')
        self.client = Client(name=self.name_pyrogram_session, api_id=self.api_id, api_hash=self.api_hash, workdir=self.path_pyrogram_session, takeout=True, max_concurrent_transmissions=3)
        # словарь - строка таблицы
        self.diction={}
        #
        # KeyBoardClient
        self.kb = KeyBoardClient(self.logger)
        self.start_button = self.kb.start_button()
        #
        # создаем клавиатуру hash_factor от 0.1 до 0.9
        # self.name_data_hash_factor = [str(i / 10) for i in range(1, 10)]
        # self.column_hash_factor = 4 # количество колонок кнопок
        self.kb_hash_factor = self.kb.inline_kb(HandlersBot.COLUMN_HASH_FACTOR, HandlersBot.name_data_hash_factor, repit=True)
        #
        # создаем клавиатуру threshold_keyframes от 0.1 до 0.5
        # self.name_data_threshold_keyframes = [str(i / 10) for i in range(1, 6)]
        # self.column_threshold_keyframes = 4 # количество колонок кнопок
        self.kb_threshold_keyframes = self.kb.inline_kb(HandlersBot.COLUMN_THRESHOLD_KFRAMES, HandlersBot.name_data_threshold_keyframes, repit=True)
        #
        # создаем клавиатуру withoutlogo 
        # self.name_withoutlogo = ['Убираем лого', 'Оставляем лого']
        # self.data_withoutlogo = ['yes', 'no']
        # self.column_withoutlogo = 2 # количество колонок кнопок
        self.kb_withoutlogo = self.kb.inline_kb(HandlersBot.COLUMN_WITHOUTLOGO, HandlersBot.name_withoutlogo, HandlersBot.data_withoutlogo)
        #
        # создаем клавиатуру number_corner 
        # self.name_data_number_corner = ['1', '2', '4', '3']
        # self.column_number_corner = 4 # количество колонок кнопок
        self.kb_number_corner = self.kb.inline_kb(HandlersBot.COLUMN_NUMBER_CORNER, HandlersBot.name_data_number_corner, repit=True)
        #
        # создаем клавиатуру logo_size 
        # self.name_data_logo_size = [str(i) for i in range(100, 310, 10)]
        # self.column_logo_size = 9 # количество колонок кнопок
        self.kb_logo_size = self.kb.inline_kb(HandlersBot.COLUMN_LOGO_SIZE, HandlersBot.name_data_logo_size, repit=True)

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
            'diction_db',
            'process_vid',
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


    # записываем строку (словарь значений) в БД
    async def save_data_bd(self, name_table: str, diction: list[dict], userid: str):
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _save_data_bd():
            args = (name_table, diction)
            if not await self.method_db.insert_data(*args):
                msg = (f'\nERROR [{__name__}|{self.cls_name}] ERROR не смогли записать данные в БД.\n')
                res = await self.bot.send_message(userid, msg)
                print(f'\n[{__name__}|{self.cls_name}] res: {res}')
            # выводим таблицу 
            await self.method_db.print_tables(name_table)
            # очищаем строку-словарь БД
            self.diction = {}
            return args
        return await  _save_data_bd()




    # обрабатывает команду пользователя - /start
    @router.message(CommandStart(ignore_case=True))
    async def command_start(self, message: Message, state: FSMContext):
        @safe_await_aiogram_exe(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _command_start():

            #  тестовое сообщение и убираем предыдущую клавиатуру   
            msg = (
                    f'Будем сравнивать два видеофайла на уникальность \n'
                    f'Пришлите в этот чат *первое*\ видео формата *[mp4]*\ \n'
                    f'Размером не более *[500 Mб]*\  \n'
                    f'Длительностью не более *[10 минут]*\ \n'
                    )
            await self.bot.send_message(message.from_user.id, 
                                        msg, 
                                        parse_mode='MarkdownV2',
                                        reply_markup=ReplyKeyboardRemove(),
                                        )  
            
            # Устанавливаем пользователю состояние "ждем первое видео"
            await state.set_state(Form.first_video)
        return await _command_start()

    # обрабатывает команду - /cancel
    @router.message(Command(["cancel"], ignore_case=True))
    @router.message(F.text.lower() == "отмена")
    async def cmd_cancel(message: Message, state: FSMContext):
        await state.clear()
        await message.answer(
            text="Действие отменено",
            reply_markup=ReplyKeyboardRemove()
                                )
    
    ## обработчик Form.first_video
    @router.message(Form.first_video, F.video)
    async def process_first_video(self, message: Message, state: FSMContext):
        @safe_await_aiogram_exe(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _process_first_video():
            self._new_handlers('process_first_video')
            
            # обработка видео (проверка типа и размера, скачивание 
            # и информирование пользователя)
            full_saving_path = await self.video.process_video(message, 
                                                        'Первое', 
                                                        self.bot, 
                                                        self.client, 
                                                        self.size_limit, 
                                                        self.path_save_vid,
                                                        )
            if not full_saving_path:
                return None
            
            # записываем данные первого видео в словарь-строку для БД
            self.diction = {}
            self.diction = self.dictionDB.diction_base_data_first(message, full_saving_path)

            msg = (f'\nТеперь отправьте второе видео.\n')
            await self.bot.send_message(message.from_user.id, msg)
            
            # Устанавливаем пользователю состояние "ждем второе видео"
            await state.set_state(Form.second_video) 
        return  await _process_first_video()  


    # обработчик некорректного ввода Form.first_video
    @router.message(Form.first_video)
    async def uncor_first_video(self, message: Message):
        @safe_await_aiogram_exe(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _uncor_first_video():
            self._new_handlers('uncor_first_video')
            msg = (f'Прислали: {message.content_type}\n'
                    f'Пришлите в этот чат *первое*\ видео формата *[mp4]*\ \n'
                    f'Размером не более *[500 Mб]*\  \n'
                    f'Длительностью не более *[10 минут]*\ \n'
                    )
            await message.answer(
                    text=msg,
                    parse_mode='MarkdownV2',
                                    )
        return  await _uncor_first_video()  


    ## обработчик Form.second_video
    @router.message(Form.second_video, F.video)
    async def process_second_video(self, message: Message, state: FSMContext):
        @safe_await_aiogram_exe(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _process_second_video():
            self._new_handlers('process_second_video')
            
            # обработка видео (проверка типа и размера, скачивание 
            # и информирование пользователя)
            full_saving_path = await self.video.process_video(message, 
                                                        'Второе', 
                                                        self.bot, 
                                                        self.client, 
                                                        self.size_limit, 
                                                        self.path_save_vid,
                                                        )
            if not full_saving_path:
                return None
            
            # формируем вторую часть словарь-строку таблицы БД
            self.diction = self.dictionDB.diction_base_data_second(message, full_saving_path, self.diction)

            # выводим клавиатуру hash_factor от 0.1 до 0.9
            msg = (f'\nВторое видео скачано.\nСсылка: {full_saving_path} \n'
                   f'\nВыберите значение порога схожести кадров от 0.1 до 0.9\n'
                   f'В клавиатуре стоит resize_keyboard=True'
                   )
            await self.bot.send_message(message.chat.id, msg, 
                                reply_markup=self.kb_hash_factor.as_markup(resize_keyboard=True))  
            
            # Устанавливаем пользователю состояние 'ждем hash_factor'
            await state.set_state(Form.hash_factor)    
        return  await _process_second_video()  


    # обработчик некорректного ввода Form.second_video
    @router.message(Form.second_video)
    async def uncor_second_video(self, message: Message):
        @safe_await_aiogram_exe(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _uncor_second_video():
            self._new_handlers('uncor_second_video')
            msg = (f'Прислали: {message.content_type}\n'
                    f'Пришлите в этот чат *второе*\ видео формата *[mp4]*\ \n'
                    f'Размером не более *[500 Mб]*\  \n'
                    f'Длительностью не более *[10 минут]*\ \n'
                    )
            await message.answer(
                    text=msg,
                    parse_mode='MarkdownV2',
                                    )
        return  await _uncor_second_video()  


    ## обработчик Form.hash_factor
    @router.callback_query(Form.hash_factor, F.text.in_(name_data_hash_factor))
    async def callback_hash_factor(self, callback: CallbackQuery, state: FSMContext):
        @safe_await_aiogram_exe(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _callback_hash_factor():
            
            # дополняем словарь-строку таблицы БД
            data = callback.data
            print(f'\n[{__name__}|{self.cls_name}] callback.data: {data}')
            self.diction['hash_factor']=data
            
            # выводим клавиатуру threshold_keyframes от 0.1 до 0.5
            msg = (f'\nВыбрали значение порога схожести кадров [{data}]\n'
                   f'\nВыберите значение порога определения ключевых кадров\n'
                   f'В клавиатуре не стоит resize_keyboard=True'
                   )
            await self.bot.send_message(callback.from_user.id, msg, 
                            reply_markup=self.kb_threshold_keyframes.as_markup())  

            # Устанавливаем пользователю состояние 'ждем threshold_keyframes'
            await state.set_state(Form.threshold_keyframes) 
        return  await _callback_hash_factor()  


    # обработчик некорректного ввода Form.hash_factor
    @router.callback_query(Form.hash_factor)
    async def uncor_hash_factor(self, message: Message):
        @safe_await_aiogram_exe(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _uncor_hash_factor():
            self._new_handlers('uncor_hash_factor')
            msg = (f'Прислали: {message.content_type}\n'
                    f'Нажмите кнопку на *клавиатуре*\ \n'
                    f'Выберите значение *hash_factor*\ \n'
                    )
            await message.answer(
                    text=msg,
                    parse_mode='MarkdownV2',
                                    )
        return  await _uncor_hash_factor()  


    ## обработчик Form.threshold_keyframes
    @router.callback_query(Form.threshold_keyframes, F.text.in_(name_data_threshold_keyframes))
    async def callback_threshold_keyframes(self, callback: CallbackQuery, state: FSMContext):
        @safe_await_aiogram_exe(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _callback_threshold_keyframes():
            
            # дополняем словарь-строку таблицы БД
            data = callback.data
            print(f'\n[{__name__}|{self.cls_name}] callback.data: {data}')
            self.diction['threshold_kframes']=data
            
            # выводим клавиатуру withoutlogo 'yes', 'no'
            msg = (f'\nВыбрали значение порога определения ключевых кадров [{data}]\n')
            await self.bot.send_message(callback.from_user.id, msg, 
                            reply_markup=self.kb_withoutlogo.as_markup())  
            
            # Устанавливаем пользователю состояние 'withoutlogo'
            await state.set_state(Form.withoutlogo)    
        return  await _callback_threshold_keyframes()  


    # обработчик некорректного ввода Form.threshold_keyframes
    @router.callback_query(Form.threshold_keyframes)
    async def uncor_threshold_keyframes(self, message: Message):
        @safe_await_aiogram_exe(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _uncor_threshold_keyframes():
            self._new_handlers('uncor_threshold_keyframes')
            msg = (f'Прислали: {message.content_type}\n'
                    f'Нажмите кнопку на *клавиатуре*\ \n'
                    f'Выберите значение *threshold_keyframes*\ \n'
                    )
            await message.answer(
                    text=msg,
                    parse_mode='MarkdownV2',
                                    )
        return  await _uncor_threshold_keyframes()  



    ## обработчик Form.withoutlogo
    @router.callback_query(Form.withoutlogo, F.text.in_(data_withoutlogo))
    async def callback_withoutlogo(self, callback: CallbackQuery, state: FSMContext):
        @safe_await_aiogram_exe(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _callback_withoutlogo():
            
            # дополняем словарь-строку таблицы БД
            data = callback.data
            print(f'\n[{__name__}|{self.cls_name}] callback.data: {data}')
            self.diction['withoutlogo']=data

            if data=='yes':
                msg = (f'\nВыбрали удалять лого [{data}]\n')
                await self.bot.send_message(callback.from_user.id, msg, 
                                reply_markup=self.kb_number_corner.as_markup())  
                
                # Устанавливаем пользователю состояние 'number_corner'
                await state.set_state(Form.number_corner)  
            
            elif data=='no':
                msg = (f'\nВыбрали не удалять лого [{data}]\n'
                       f'\nБудем только сравнивать видео\n'
                       f'\nРезультаты работы пришлю сюда... \n'
                       )
                await self.bot.send_message(callback.from_user.id, msg)
                
                # записываем строку (словарь значений) в БД
                if not await self.save_data_bd('diff', [self.diction], callback.from_user.id):
                    return None
                # снимаем состояние 
                await state.clear()
        return  await _callback_withoutlogo()  


    # обработчик некорректного ввода Form.withoutlogo
    @router.callback_query(Form.withoutlogo)
    async def uncor_withoutlogo(self, message: Message):
        @safe_await_aiogram_exe(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _uncor_withoutlogo():
            self._new_handlers('uncor_withoutlogo')
            msg = (f'Прислали: {message.content_type}\n'
                    f'Нажмите кнопку на *клавиатуре*\ \n'
                    f'Выберите значение *withoutlogo*\ \n'
                    )
            await message.answer(
                    text=msg,
                    parse_mode='MarkdownV2',
                                    )
        return  await _uncor_withoutlogo()  


    ## обработчик Form.number_corner
    @router.callback_query(Form.number_corner, F.text.in_(name_data_number_corner))
    async def callback_number_corner(self, callback: CallbackQuery, state: FSMContext):
        @safe_await_aiogram_exe(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _callback_number_corner():
            
            # дополняем словарь-строку таблицы БД
            data = callback.data
            print(f'\n[{__name__}|{self.cls_name}] callback.data: {data}')
            self.diction['number_corner']=data
            
            msg = (f'\nВыбрали удалять лого в углу №{data}\n'
                   f'\nПришлите в чат размер маски удаления лого в пикселях\n' 
                   f'(рекомендуется 180px), но не менее 100 и не более 300 пикселей')
            await self.bot.send_message(callback.from_user.id, msg, 
                                        reply_markup=self.kb_logo_size.as_markup())
            
            # Устанавливаем пользователю состояние 'logo_size'
            await state.set_state(Form.logo_size)
        return  await _callback_number_corner()  


    # обработчик некорректного ввода Form.number_corner
    @router.callback_query(Form.number_corner)
    async def uncor_number_corner(self, message: Message):
        @safe_await_aiogram_exe(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _uncor_number_corner():
            self._new_handlers('uncor_number_corner')
            msg = (f'Прислали: {message.content_type}\n'
                    f'Нажмите кнопку на *клавиатуре*\ \n'
                    f'Выберите значение *number_corner*\ \n'
                    )
            await message.answer(
                    text=msg,
                    parse_mode='MarkdownV2',
                                    )
        return  await _uncor_number_corner()  


    ## обработчик Form.logo_size
    @router.callback_query(Form.logo_size, F.text.in_(name_data_logo_size))
    async def callback_logo_size(self, callback: CallbackQuery, state: FSMContext):
        @safe_await_aiogram_exe(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _callback_logo_size():
            
            # дополняем словарь-строку таблицы БД
            data = callback.data
            print(f'\n[{__name__}|{self.cls_name}] callback.data: {data}')
            self.diction['logo_size']=data
            
            msg = (f'\nРазмер маски удаления лого выбрали [{data}] пикселей\n'
                   f'\nРезультат работы пришлю сюда...\n')
            await self.bot.send_message(callback.from_user.id, msg, reply_markup=ReplyKeyboardRemove())  
            
            # записываем строку (словарь значений) в БД
            if not await self.save_data_bd('diff', [self.diction], callback.from_user.id):
                 return None
            
            # снимаем состояние 
            await state.clear()
        return  await _callback_logo_size()  


    # обработчик некорректного ввода Form.number_corner
    @router.callback_query(Form.logo_size)
    async def uncor_logo_size(self, message: Message):
        @safe_await_aiogram_exe(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _uncor_logo_size():
            self._new_handlers('uncor_logo_size')
            msg = (f'Прислали: {message.content_type}\n'
                    f'Нажмите кнопку на *клавиатуре*\ \n'
                    f'Выберите значение *logo_size*\ \n'
                    )
            await message.answer(text=msg, parse_mode='MarkdownV2')
        return  await _uncor_logo_size()  


    # Для исключений
    @router.error(ExceptionTypeFilter(AiogramError), F.update.message.as_("message"))
    async def handle_aiogram_error(self, event: ErrorEvent, message: Message):
        @safe_await_aiogram_exe(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _handle_aiogram_error():
            msg = (f"Oops, something went wrong with aiogram! ERROR: {event.exception}")
            await message.answer(msg)
            print(msg)
            self.logger.log_info(msg)
        return  await _handle_aiogram_error()  


    # Для всех остальных исключений
    @router.error()  
    async def error_handler(self, event: ErrorEvent):
        @safe_await_aiogram_exe(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _error_handler():
            msg =  (f"ERROR: {event.exception}")
            print(msg)
            self.logger.log_info(msg)
        return  await _error_handler()  


    # обработчик любого сообщения
    @router.message()  
    async def any2start(self, message: Message, state: FSMContext):
        @safe_await_aiogram_exe(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _any2start():
            await self.bot.send_message(message.from_user.id, text=f'Прислали: {message.content_type}\n', reply_markup=ReplyKeyboardRemove())
            msg = (
                   f'Наберите команду [/start] для начала')
            await self.bot.send_message(message.from_user.id, msg, reply_markup=self.start_button)
            # очищаем все состояния FSMContext
            await state.clear()    
        return await _any2start()


