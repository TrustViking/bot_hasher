
from sys import platform, argv, path
from time import time, strftime
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
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

    # создаем кнопку СТАРТ  
    def start_button(self):
        kb = [[KeyboardButton(text="start")]]
        keyboard = ReplyKeyboardMarkup(
                    keyboard=kb,
                    resize_keyboard=True,
                    input_field_placeholder="Нажмите для старта на кнопку",
                                        )
        return keyboard


    # создаем Inline клавиатуру 
    def inline_kb(self, column: int, 
                        buttons: dict = None, 
                        name_buttons: list = None, 
                        data_buttons: list = None,
                        repit: bool = False,
                        ):
        
        if isinstance(buttons, dict) and buttons:
            for key, value in buttons.items():
                self.builder.add(InlineKeyboardButton(text=key, callback_data=value))
        
        elif not repit and not buttons and name_buttons and data_buttons and len(name_buttons)==len(data_buttons):
            for key, value in zip(name_buttons, data_buttons):
                self.builder.add(InlineKeyboardButton(text=key, callback_data=value))
        
        elif repit and name_buttons and not data_buttons:
            for key, value in zip(name_buttons, name_buttons):
                self.builder.add(InlineKeyboardButton(text=key, callback_data=value))
        else: 
            print(f'\n[{__name__}|{self.cls_name}] conditions for keyboard creation are not met')
            return None
        
        return self.builder.adjust(column)



    # создаем клавиатуру number_corner
    def kb_number_corner(self, column: int):
        for i in ['1', '2', '4', '3']:
            self.builder.add(InlineKeyboardButton(text=i, callback_data=i))
        return self.builder.adjust(column)

