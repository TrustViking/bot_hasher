
from typing import Union
from time import time, strftime
from sys import platform
from aiogram.types.message import Message

from bot_env.mod_log import LogBot
from bot_env.decorators import safe_await_execute


class DictionDB():
    """
    Создаем словарь для записи в таблицу БД:

    Аргументы:
    - logger: LogBot
    """
    countInstance=0
    #
    def __init__(self, logger: LogBot):
        DictionDB.countInstance+=1
        self.countInstance=DictionDB.countInstance
        self.cls_name = self.__class__.__name__
        # Logger
        self.logger = logger
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
        ]

        for attr in attributes_to_print:
            # "Attribute not found" будет выведено, если атрибут не существует
            value = getattr(self, attr, "Attribute not found")  
            msg += f"{attr}: {value}\n"

        print(msg)
        self.logger.log_info(msg)


    # добавляем в словарь-строку таблицы БД
    async def diction_add(self, key: str, val: Union[int, str, float], diction: dict):
        @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        async def _diction_add():
                diction[key] = val
                return diction
        return await  _diction_add()


    # формируем словарь-строку таблицы БД
    async def diction_base_data_first(self, message: Message, full_path: str):
            @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
            async def _diction_base_data():
                diction={
                    'date_message' : str(message.date),
                    'chat_id' : str(message.chat.id),
                    'username' : str(message.from_user.username),
                    'video_id_first' : str(message.video.file_id),
                    'width_first': str(message.video.width),
                    'height_first': str(message.video.height),
                    'duration_first': str(message.video.duration),
                    'mime_type_first': str(message.video.mime_type).split('/')[1],
                    'file_size_first': int(message.video.file_size),
                    'path_file_first': full_path,
                                }
                return diction
            return await  _diction_base_data()


    # формируем вторую часть словарь-строку таблицы БД
    async def diction_base_data_second(self, message: Message, full_path: str, diction: dict):
            @safe_await_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
            async def _diction_base_data():
                diction['time_task']=int(time())
                diction['video_id_second'] = str(message.video.file_id) 
                diction['width_second'] = str(message.video.width) 
                diction['height_second'] = str(message.video.height) 
                diction['duration_second'] = str(message.video.duration) 
                diction['mime_type_second'] = str(message.video.mime_type).split('/')[1] 
                diction['file_size_second'] = int(message.video.file_size) 
                diction['path_file_second'] = full_path 
                diction['dnld']='dnlded'
                diction['in_work']='not_diff'
                #
                # diction['num_kframe_1']='0'
                # diction['num_kframe_2']='0'
                # diction['result_kframe']='0'
                diction['num_kframe_1']='?_kframe'
                diction['num_kframe_2']='?_kframe'
                diction['result_kframe']='?_kframe'
                diction['hash_factor']=0.0
                diction['threshold_kframes']=0.0
                diction['withoutlogo']='0'
                diction['number_corner']='0'
                diction['logo_size']='0'

                diction['result_diff']='?_similar'
                diction['num_similar_pair']='?_similar'
                diction['save_sim_img']='not_save'
                diction['path_sim_img']='not_path'
                diction['sender_user']='not_sender'
                return diction
            return await  _diction_base_data()
