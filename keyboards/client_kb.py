
from sys import platform, argv, path
from time import time, strftime
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
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
                 ):
        KeyBoardClient.countInstance+=1
        self.countInstance=KeyBoardClient.countInstance
        self.cls_name = self.__class__.__name__
        self.logger = logger
        self.builder = InlineKeyboardBuilder()
        #
        # словарь наименования кнопок и значения, которые ловим хэндлером
        self.name_button = None
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
    
    # создаем словарь наименований кнопок и значений, которые они отправляют
    # ловим хэндлером: dp.register_callback_query_handler
    def make_name_button(self, diction_buttons: dict = None, 
                                name_buttons: list = None, 
                                data_buttons: list = None):
        name_button={}
        if not diction_buttons and name_buttons and data_buttons and len(name_buttons)==len(data_buttons):
            for key, value in zip(name_buttons, data_buttons):
                name_button[key]=value
                # name_button['0.1']='0.1'
                # name_button['0.2']='0.2'
                # name_button['0.3']='0.3'
                # name_button['0.4']='0.4'
                # name_button['0.5']='0.5'
            print(f'[{__name__}|{self.cls_name}] создали клавиатуру № {self.countInstance}: {name_button} ')
            return name_button 
        if diction_buttons: 
            print(f'[{__name__}|{self.cls_name}] создали клавиатуру № {self.countInstance}: {diction_buttons} ')
            return diction_buttons


    # создаем клавиатуру hash_factor
    def kb_hash_factor(self, column: int):
        for key, value in self.name_button.items():
            self.builder.add(InlineKeyboardButton(text=key, callback_data=value))
        return self.builder.adjust(column)

    # создаем клавиатуру threshold_keyframes
    def kb_threshold_keyframes(self, column: int):
        for key, value in self.name_button.items():
            self.builder.add(InlineKeyboardButton(text=key, callback_data=value))
        return self.builder.adjust(column)

    # создаем клавиатуру withoutlogo
    def kb_withoutlogo(self, column: int):
        buttons = [InlineKeyboardButton(text='Убираем лого', callback_data='yes'), 
                   InlineKeyboardButton(text='Оставляем лого', callback_data='no')]
        self.builder.add(buttons)
        return self.builder.adjust(column)
    
    # создаем клавиатуру number_corner
    def kb_number_corner(self, column: int):
        for i in ['1', '2', '4', '3']:
            self.builder.add(InlineKeyboardButton(text=i, callback_data=i))
        return self.builder.adjust(column)

