
from sys import platform
from time import strftime
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from bot_env.mod_log import LogBot


class KeyBoardClient:
    """
    Создаем клавиатуру клиента для telegram-bot:
    
    Аргументы:
    - 
    """
    countInstance=0

    def __init__(self, 
                 logger: LogBot,
                 ):
        KeyBoardClient.countInstance+=1
        self.countInstance=KeyBoardClient.countInstance
        self.cls_name = self.__class__.__name__
        self.logger = logger
        self.builder = InlineKeyboardBuilder()
        #
        # словарь наименования кнопок и значения, которые ловим хэндлером
        # self.name_button = None
        self._print()

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
            'logger',
            'builder',
        ]

        for attr in attributes_to_print:
            # "Attribute not found" будет выведено, если атрибут не существует
            value = getattr(self, attr, "Attribute not found")  
            msg += f"{attr}: {value}\n"

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
        print(f'\n[{__name__}|{self.cls_name}] start_button keyboard: {keyboard}')
        return keyboard


    # создаем Inline клавиатуру 
    def inline_kb(self, column: int, 
                        buttons: dict = None, 
                        name_buttons: list = None, 
                        data_buttons: list = None,
                        repeat: bool = False,
                        ):
        
        if isinstance(buttons, dict) and buttons:
            for key, value in buttons.items():
                self.builder.add(InlineKeyboardButton(text=key, callback_data=value))
        
        elif not repeat and not buttons and name_buttons and data_buttons and len(name_buttons)==len(data_buttons):
            for key, value in zip(name_buttons, data_buttons):
                self.builder.add(InlineKeyboardButton(text=key, callback_data=value))
        
        elif repeat and name_buttons and not data_buttons:
            for key, value in zip(name_buttons, name_buttons):
                self.builder.add(InlineKeyboardButton(text=key, callback_data=value))
        
        elif repeat and data_buttons and not name_buttons:
            for key, value in zip(data_buttons, data_buttons):
                self.builder.add(InlineKeyboardButton(text=key, callback_data=value))
        
        else: 
            print(f'\n[{__name__}|{self.cls_name}] conditions for keyboard creation are not met')
            return None
        
        if column >8:
            return self.builder.adjust(repeat=True)

        return self.builder.adjust(column)


    # создаем клавиатуру number_corner
    def kb_number_corner(self, column: int, buttons: list):
        # for i in ['1', '2', '4', '3']:
        for i in buttons:
            self.builder.add(InlineKeyboardButton(text=i, callback_data=i))
        return self.builder.adjust(column)

