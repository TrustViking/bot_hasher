from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from bot_env.mod_log import Logger


class KeyBoardClient:
    """
    Создаем клавиатуру клиента для telegram-bot:
    
    Аргументы:
    - 
    """
    countInstance=0

    def __init__(self, 
                 logger: Logger,
                 row_width=3,
                 ):
        KeyBoardClient.countInstance+=1
        self.countInstance=KeyBoardClient.countInstance
        self.row_width=row_width
        self.Logger = logger
        self._print()
        # # создаем клавиатуру
        self.keyboard = InlineKeyboardMarkup(row_width=self.row_width) 
        #
        # кнопки
        self.b_y2b = None
        self.b_OK_y2b = None
        self.b_NO_y2b = None
        self.b_OK_y2b_bad = None
        self.b_NO_y2b_bad = None
        self.b_OK_y2b_timestamp = None
        self.b_NO_y2b_timestamp = None
        self.user_video = None
        self.b_2020 = None
        self.b_2021 = None
        self.b_2022 = None
        self.b_2023 = None
        self.b_menu_3 = None
        # словарь наименования кнопок и значения, которые ловим хэндлером
        self.name_button = {}
        self._make_name_button()

    # создаем словарь наименований кнопок и значений, которые они отправляют
    # ловим хэндлером: dp.register_callback_query_handler
    def _make_name_button(self):
        self.name_button['/start']='/start'
        # menu # 1
        self.name_button['1']='Видео с youtube'
        self.name_button['2']='Свое видео'
        # menu OK/No link norm
        self.name_button['3']='OK'
        self.name_button['4']='NO'
        # menu OK/No link bad
        self.name_button['5']='OK'
        self.name_button['6']='NO'
        # menu OK/No timestamp
        self.name_button['7']='OK'
        self.name_button['8']='NO'
        #
        # self.name_button['9']='2022'
        # self.name_button['10']='2023'
        # self.name_button['20']='меню 3 уровня'
        print(f'[_make_name_button] создали клавиатуру № {self.countInstance}')
    #
    # выводим № объекта
    def _print(self):
        print(f'[KeyBoardClient] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'[KeyBoardClient] countInstance: [{self.countInstance}]')
#
    # создаем кнопку старта, отправляет команду '/start'
    # используем, когда пользователь набирает любые символы, кроме '/start'
    def start_button(self): 
        start_button = InlineKeyboardButton(text="Это кнопка \U0001F680 ПУСК", 
                                            callback_data='/start')
        self.keyboard.add(start_button)
    #
    # создаем кнопки меню первого уровня
    def menu_1_level(self):
        self.b_y2b = InlineKeyboardButton(text=self.name_button['1'], callback_data='1')
        self.user_video = InlineKeyboardButton(text=self.name_button['2'], callback_data='2')
        self.keyboard.row(self.b_y2b).row(self.user_video)

    # создаем кнопку ОК & NO для youtube_link_handler
    def button_OK_NO_youtube_link(self):
        self.b_OK_y2b = InlineKeyboardButton(text=self.name_button['3'], callback_data='3')
        self.b_NO_y2b = InlineKeyboardButton(text=self.name_button['4'], callback_data='4')
        self.keyboard.row(self.b_OK_y2b).row(self.b_NO_y2b)
    #
    # создаем кнопку ОК & NO для youtube_link_handler
    def button_OK_NO_youtube_link_bad(self):
        self.b_OK_y2b_bad = InlineKeyboardButton(text=self.name_button['5'], callback_data='3')
        self.b_NO_y2b_bad = InlineKeyboardButton(text=self.name_button['6'], callback_data='4')
        self.keyboard.row(self.b_OK_y2b_bad).row(self.b_NO_y2b_bad)
    #
    # создаем кнопку ОК & NO для youtube_timestamp
    def button_OK_NO_youtube_timestamp(self):
        self.b_OK_y2b_timestamp = InlineKeyboardButton(text=self.name_button['7'], callback_data='7')
        self.b_NO_y2b_timestamp = InlineKeyboardButton(text=self.name_button['8'], callback_data='8')
        self.keyboard.row(self.b_OK_y2b_timestamp).row(self.b_NO_y2b_timestamp)
    #
    # создаем кнопки меню второго уровня
    # def menu_2_level(self):
    #     self.b_2022 = InlineKeyboardButton(text=self.name_button['9'], callback_data='9')
    #     self.b_2023 = InlineKeyboardButton(text=self.name_button['10'], callback_data='10')
    #     self.keyboard.add(self.b_2020, self.b_2021, self.b_2022, self.b_2023)
    #
    # создаем кнопки меню третьего уровня
    # def menu_3_level(self):
    #     self.b_menu_3 = InlineKeyboardButton(text=self.name_button['20'], callback_data='20')
    #     self.keyboard.add(self.b_menu_3)

