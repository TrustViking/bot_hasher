import os
from aiogram import Bot
from aiogram.dispatcher import Dispatcher
from aiogram.contrib.fsm_storage.memory import MemoryStorage

"""
Создаем для telegram-bot объекты:
Bot, Dispatcher  
Переменные:
- token: токен telegram-bot берем в $PATH (.bashrc)
- bot: объект Bot
- dp: объект Dispatcher
"""
#
# хранилище состояний пользователей Finite State Machine (FSM)
# может потребоваться настроить более надежное и постоянное хранилище, 
# такое как RedisStorage или MongoDBStorage.
storage_mem = MemoryStorage() 
token=os.getenv('TELEGRAM_TOKEN')
bot=Bot(token)
dp=Dispatcher(bot, storage=storage_mem)



