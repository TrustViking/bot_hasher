
import re, os
from time import time
from aiogram import types
from aiogram.dispatcher.filters import Text, Regexp, Command
from aiogram.dispatcher.filters.state import State
from aiogram.dispatcher import FSMContext
# from aiogram.types import ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton 
from bot_env.mod_log import Logger
from bot_env.create_obj4bot import bot, dp
from bot_env.timestamp_parsing import ParseTime
from keyboards.client_kb import KeyBoardClient
from y2b4bot.you2b import You2b
from data_base.base_db import BaseDB

class Client2bot:
    """
    Создаем для telegram-bot хэндлеры клиента:

    Аргументы:
    - logger: Logger
    """
    countInstance=0
    #
    def __init__(self, logger: Logger):
        Client2bot.countInstance+=1
        self.countInstance=Client2bot.countInstance
        self.Logger = logger
        self.Db=BaseDB(logger=self.Logger)
        # Устанавливаем состояние ожидания подтверждения
        self.State = State()
        self.group = os.getenv('TELEGRAM_GROUP')
        # В разметке MarkdownV2 для Telegram некоторые символы являются специальными
        #  и должны быть экранированы обратным слешем (\). 
        self.special_chars = "_*[]()~>#+-=|{}.!" 
        # словарь для создания и записи строки в БД про ссылку youtube
        self.youtube_info=None
        # self.youtube_link=''
        self.video_id=''
        self.video_title=''
        self.video_duration=''
        self.video_duration_sec = ''
        self.datatime_duration=None 
        #
        self.duration_minuts=''
        self.username=''
        self.date_message=''
        self.user_id=''
        self.chat_id=''
        self.video_url = None
        self.channel_title = None # 
        self.default_audio_language = None
        #
        self.diction4db={} 
        # временные метки
        self.ParsTime= None
        self.timestamp=''
        self.timestamp_start=''
        self.timestamp_start_dt=None
        self.timestamp_end=''
        self.timestamp_end_dt=None
        self.segment_duration=''
        #
        self._new_client()
        
        #
    # New Client
    def _new_client(self):
        print(f'[_new_client] Client# {self.countInstance}')
        self.Logger.log_info(f'[_new_client] Client# {self.countInstance}')
    #
    # обработчик любого сообщения, кроме  - /start
    async def any2start(self, message: types.Message):
        kb = KeyBoardClient(logger=self.Logger, row_width=1)
        kb.start_button() # загружаем кнопку Старт в клавиатуру
        mes4user=(f"Для СТАРТА нажмите \U0001F447 на кнопку \U0001F680 ПУСК ")
        # присылает chat_id==user_id
        # await message.answer(text=message.chat.id) # присылает chat_id==user_id
        # for char in self.special_chars:
        #     mes4user = mes4user.replace(char, f"\\{char}")
        await bot.send_message(message.from_user.id, mes4user, reply_markup=kb.keyboard)
        #
    # обрабатываем нажатие кнопки СТАРТ 
    # предлагаем выбрать 'youtube' (#1) или 'свое видео' (#2) 
    async def call_start(self, callback: types.CallbackQuery):
        kb = KeyBoardClient(logger=self.Logger, row_width=1)
        kb.menu_1_level() # добавляем меню 1 уровня
        msg = (f'Будем уникализировать фрагмент видео из youtube или свое видео?\n'
               f'Нажмите на соответствующую кнопку')
        await callback.message.answer(text=msg, reply_markup=kb.keyboard)
        # убираем часики на кнопке, которую нажали
        await callback.answer() # 
    #
    # обрабатываем нажатие кнопки #1 'Видео с youtube'
    async def call_1_y2b(self, callback: types.CallbackQuery):
        msg = f'Пришлите в этот чат ссылку на видео с youtube, которое будем фрагментировать'
        await callback.message.answer(msg)
        # # убираем часики на кнопке, которую нажали
        await callback.answer() # 
    #    
    # обрабатываем нажатие кнопки #2 'Свое видео' 
    async def call_my_video(self, callback: types.CallbackQuery):
        msg = f'Тут чуть позже будет ветка обработки видео пользоваттеля'
        await callback.message.answer(msg) # 
        # убираем часики на кнопке, которую нажали
        await callback.answer() # 
    #
    # обрабатывает ссылку youtube
    async def youtube_link_handler(self, message: types.Message): 
        # Получаем ссылку на YouTube из сообщения
        # self.youtube_link = message.text
        #
        # self.youtube_info=None
        self.youtube_info=You2b(url=message.text, logger=self.Logger)
        self.video_duration=str(self.youtube_info.duration_iso8601)
        # заполняем поля
        # self.video_url=str(self.youtube_info.video_url)
        # self.video_id=str(self.youtube_info.video_id)
        self.channel_title=str(self.youtube_info.channel_title)
        self.video_title=str(self.youtube_info.video_title)
        # self.default_audio_language=str(self.youtube_info.default_audio_language)
        # self.username=str(message.from_user.username)
        # self.date_message=str(message.date)
        # self.user_id=str(message.from_user.id)
        # self.chat_id=str(message.chat.id)
        #
        parsTime=ParseTime(logger=self.Logger)
        self.video_duration_sec, self.datatime_duration=parsTime.format_iso8601_sec_dt(self.video_duration)
        self.duration_minuts=parsTime.format_sec2minuts(self.video_duration_sec)
        #
        # формируем строку таблицы task 
        self.diction4db={
            'date_message' : str(message.date),
            'chat_id' : str(message.chat.id),
            'user_id' : str(message.from_user.id),
            'username' : str(message.from_user.username),
            'url_video_y2b' : str(self.youtube_info.video_url),
            'video_id' : str(self.youtube_info.video_id),
            'channel_title' : self.channel_title,
            'video_title' : self.video_title,
            'video_duration' : self.video_duration,
            'video_duration_sec' : self.video_duration_sec,
            # 'datatime_duration' : self.datatime_duration,
            'duration_minuts' : self.duration_minuts,
            'default_audio_language' : str(self.youtube_info.default_audio_language),
                        }
        #
        # спрашиваем подтверждение ссылки youtube
        if self.youtube_info:
            # добавляем кнопки ОК & NO для youtube_link_handler
            kb = KeyBoardClient(logger=self.Logger, row_width=1)
            kb.button_OK_NO_youtube_link() 
            # отвечаем пользователю на введенную ссылку YouTube 
            mes4user=(f'Видео: {self.video_title} \n\n'
                      f'Канал: {self.channel_title} \n\n'
                      f'Продолжительность: {self.duration_minuts} \n')
            # отправляем клавиатуру подтверждения 
            await bot.send_message(message.from_user.id, mes4user, reply_markup=kb.keyboard)
            # Устанавливаем состояние ожидания подтверждения
            await self.State.set()
        else: 
            print(f'[youtube_link_handler] There are empty fields in table \ndiction4db: {str(self.diction4db)}')
            # добавляем кнопки ОК & NO для youtube_link_handler
            kb = KeyBoardClient(logger=self.Logger, row_width=1)
            kb.button_OK_NO_youtube_link_bad() 
            mes4user=(f'{self.date_message} Вы ввели ошибочную ссылку'
                      f'на видео с названием: {self.video_title} '
                      f'на канале {self.channel_title} \n'
                      f'Будете вводить другую ссылку?')
            await bot.send_message(message.from_user.id, mes4user, reply_markup=kb.keyboard)
            # Устанавливаем состояние ожидания подтверждения
            await self.State.set()
        #
    # обрабатываем нажатие кнопки ОК #3 youtube_link 
    async def youtube_link_OK(self, callback: types.CallbackQuery, state: FSMContext):
        mes4user=(f'Введите временные метки фрагмента из видео: \n'
                  f'{self.video_title} \n'
                  f'Формат временных меток начала и конца фрагмента: \n'
                  f'00:00:00-00:00:00, формат: [ЧЧ:ММ:СС-ЧЧ:ММ:СС]')
        await callback.message.answer(mes4user)
        # убираем часики на кнопке, которую нажали
        await callback.answer() # 
        # Сбрасываем состояние
        await state.finish()
    #
    # обрабатываем временные метки 00:00:00-00:00:00, 00:00:00 -> ЧЧ:ММ:СС 
    async def youtube_timestamp(self, message: types.Message):
        self.timestamp = message.text
        parsTime=ParseTime(logger=self.Logger)
        # self.timestamp_start, self.timestamp_start_dt, self.timestamp_end, self.timestamp_end_dt, self.segment_duration = parsTime.parse_time(timestamp=self.timestamp)
        (   self.timestamp_start, 
            self.timestamp_start_dt, 
            self.timestamp_end, 
            self.timestamp_end_dt, 
            self.segment_duration
        ) = parsTime.parse_time(self.timestamp)
        
        # проверяем метки на корректность
        if int(self.segment_duration) >= int(self.video_duration_sec):
            mes4user=(f'Длительность фрагмента больше самого видео!  \n'
                      f'Пришлите любое сообщение')
            await bot.send_message(message.from_user.id, mes4user) 
            return None
        # проверяем метки на корректность
        if self.timestamp_start_dt > self.datatime_duration or self.timestamp_end_dt>self.datatime_duration:
            mes4user=(f'Временные метки не корректны: \n'
                      f'Начало фрагмента: {self.timestamp_start} \n'
                      f'Окончание фрагмента: {self.timestamp_end} \n'
                      f'Длительность видео: {self.duration_minuts} \n'
                      f'Введите любые символы и начните заново')
            await bot.send_message(message.from_user.id, mes4user) 
            return None
        #
        # добавляем кнопки ОК & NO для подтверждения timestamp
        kb = KeyBoardClient(logger=self.Logger, row_width=1)
        kb.button_OK_NO_youtube_timestamp() 
        # отвечаем пользователю на введенные временные метки 
        # отправляем клавиатуру подтверждения ОК & NO
        mes4user=(f'Вы передали временные метки фрагмента: \n'
                  f'Начало: {self.timestamp_start} \n'
                  f'Окончание: {self.timestamp_end} \n'
                  f'Продолжительность фрагмента: {parsTime.format_sec2minuts(self.segment_duration)} \n\n'
                  f'Видео: {self.video_title} \n'
                  f'Канал: {self.channel_title} \n\n'
                  f'Нажмите \U0001F447 кнопку ОК, если подтверждаете')
        await bot.send_message(message.from_user.id, mes4user, reply_markup=kb.keyboard)
        # Устанавливаем состояние ожидания подтверждения
        await self.State.set()
    #
    # обрабатываем нажатие кнопки ОК #7  timestamp
    async def youtube_timestamp_OK(self, callback: types.CallbackQuery, state: FSMContext):
        #
        parsTime=ParseTime(logger=self.Logger)
        mes4user=(f'Вы подтвердили временные метки фрагмента: \n'
                  f'Начало: {self.timestamp_start} \n'
                  f'Окончание: {self.timestamp_end} \n'
                  f'Продолжительность фрагмента: {parsTime.format_sec2minuts(time_sec=self.segment_duration)} \n\n'
                  f'Видео: {self.video_title} \n'
                  f'Канал: {self.channel_title} \n\n'
                  f'Бот начал работать... \n'
                  f'Результат будет передан в этот чат. \n'
                  f'Ожидайте... \n')
        await callback.message.answer(mes4user)
        # убираем часики на кнопке, которую нажали
        await callback.answer() # 
        # Сбрасываем состояние
        await state.finish()
        #
        # дописываем в строку временные метки для БД
        self.diction4db['segment_duration']=self.segment_duration
        self.diction4db['time_task']=int(time())
        self.diction4db['timestamp_start']=self.timestamp_start
        # self.diction4db['timestamp_start_dt']=self.timestamp_start_dt
        self.diction4db['timestamp_end']=self.timestamp_end
        # self.diction4db['timestamp_end_dt']=self.timestamp_end_dt
        # задача не проверялась на закачку исходника
        self.diction4db['in_work_download']='not_download'
        self.diction4db['path_download']='not_path'
        self.diction4db['in_work_frag']='not_frag' 
        # self.diction4db['num_frag']=2 
        self.diction4db['name_frag']='not_name.frag' 
        self.diction4db['path_frag']='not_path' 
        self.diction4db['send']='not_send'
        self.diction4db['send2group_file_id']='not_id' 
        self.diction4db['resend']='not_resend'
        self.diction4db['resend_file_id']='not_id'
        #
        # создаем новую запись в таблице task 
        # db=BaseDB(logger=self.Logger)
        # записываем словарь значений в таблицу task 
        await self.Db.insert_data('task', self.diction4db)
        # выводим таблицу task
        await self.Db.print_data('task')
    #
    # обрабатываем нажатие кнопки #4 не та ссылка y2b -> start
    async def call_NO_link(self, callback: types.CallbackQuery, state: FSMContext):
        # self.Logger.log_info(f'[call_NO_link] choice_user: {callback.data} ')
        msg = f'Наберите любой символ'
        await callback.message.answer(msg)
        # убираем часики на кнопке, которую нажали
        await callback.answer() # 
        # Сбрасываем состояние
        await state.finish()
    #
    # обрабатываем итоговое видео в группе
    async def resend_video(self, message: types.Message):
        video_id = message.video.file_id if message.video else None
        # full_name_frag = os.path.basename(message.caption)
        # name_frag=full_name_frag[:-4]
        name_frag=os.path.basename(message.caption)[:-4] if message.caption else 'no_name.mp4'
        result = await self.Db.read_data_one('frag', 'name_frag', name_frag)
        row = result.fetchone()
        if not row:
            print(f'\n[Client2bot resend_video result.fetchone()] нет строки в result из frag')
            return None
        chat_id = row.chat_id  
        # отправляем видео пользователю
        try:
            resend = await bot.send_video(chat_id=chat_id, video=video_id)
        except Exception as eR:
            print(f'\nERROR[Client2bot resend_video bot.send_video] ERROR: {eR}')
            self.Logger.log_info(f'\nERROR[Client2bot resend_video bot.send_video] ERROR: {eR}')
            return None
        if not resend:
            print(f'\n[Client2bot resend_video bot.send_video] не переслали из группы файл пользователю ')
            return None
        # отмечаем в таблицах пересылку сообщения
        diction = {'resend': 'resended', 'resend_file_id': video_id}
        if not await self.Db.update_table_resend(['task', 'frag'], name_frag, diction):
            print(f'\n[Client2bot resend_video update_table_resend] не записали в таблицы [task, frag] отметки об отправке пользователю')
            return None
        #
        # удаляем из группы пересланое видео, чтобы не засорять группу
        chat_message_id=message.message_id
        print(f'\n[Client2bot resend_video bot.send_video] chat_message_id: {chat_message_id}')
        try:
            if not await bot.delete_message(chat_id=self.group, message_id=chat_message_id):
                await bot.send_message(chat_id=self.group,
                                       text=f'\n[Client2bot resend_video] Не получилось удалить крайнее пересланое видео id: {chat_message_id}')
        except Exception as eR:
            print(f'\nERROR[Client2bot resend_video] ERROR: {eR}')
            self.Logger.log_info(f'\nERROR[Client2bot resend_video] ERROR: {eR}')

    # регистрация хэндлеров
    async def register_handlers_client(self):
        # обрабатываем нажатие кнопки СТАРТ 
        # предлагаем выбрать 'youtube' (#1) или 'свое видео' (#2) 
        dp.register_callback_query_handler(self.call_start, Text(contains='/start', ignore_case=True))
        
        # обрабатываем нажатие кнопки #1 'Видео с youtube'
        dp.register_callback_query_handler(self.call_1_y2b, Text(contains='1', ignore_case=True))
        #
        # обрабатываем нажатие кнопки #2 'Свое видео' 
        dp.register_callback_query_handler(self.call_my_video, Text(contains='2', ignore_case=True))
        #
        # обрабатывает ссылку youtube
        regexp_pattern_link_youtube =  re.compile(r'(https?://)?(www\.)?(youtube\.com/(v|watch)\?.*|youtu\.be/.*|youtube\.com/attribution_link.*|youtube\.com/results\?.*)', re.IGNORECASE)
        dp.register_message_handler(self.youtube_link_handler, Regexp(regexp_pattern_link_youtube))
        #
        # обрабатываем нажатие кнопки ОК #3 youtube_link 
        dp.register_callback_query_handler(self.youtube_link_OK, Text(contains='3', ignore_case=True))
        
        # обрабатываем нажатие кнопки #4 не та ссылка y2b -> start
        dp.register_callback_query_handler(self.call_NO_link, Text(contains='4', ignore_case=True))

        # обрабатывает временные метки видео-фрагмента
        regexp_pattern_timestamp =  re.compile(r'^\d{2}:\d{2}:\d{2}-\d{2}:\d{2}:\d{2}$', re.IGNORECASE)
        dp.register_message_handler(self.youtube_timestamp, Regexp(regexp_pattern_timestamp))
        #
        # обрабатываем нажатие кнопки ОК #7  timestamp
        dp.register_callback_query_handler(self.youtube_timestamp_OK, Text(contains='7', ignore_case=True))
        #  
        # обрабатываем нажатие кнопки NО #8  timestamp
        dp.register_callback_query_handler(self.youtube_link_OK, Text(contains='8', ignore_case=True))

        # обрабатывает видео в группе
        # dp.register_message_handler(self.resend_video, MagicFilter())
        dp.register_message_handler(self.resend_video, content_types=types.ContentType.VIDEO)

        # ловит любые сообщения и вызывает кнопку старт
        dp.register_message_handler(self.any2start)


