from os import getenv
from os.path import basename, join, isfile, dirname
from sys import platform, argv, path
from json import load 
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from .mod_log import Logger

"""
Создаем для telegram-bot объекты:
Bot, Dispatcher  
Переменные:
- token: токен telegram-bot берем в $PATH (.bashrc)
- bot: объект Bot
- dp: объект Dispatcher
- log_hasher: Logger
"""
#
# хранилище состояний пользователей Finite State Machine (FSM)
# может потребоваться настроить более надежное и постоянное хранилище, 
# такое как RedisStorage или MongoDBStorage.
token=getenv('TELEGRAM_TOKEN_HASHER')
bot=Bot(token)
dp=Dispatcher(storage=MemoryStorage())

# Чтение конфигурационного файла
config_path = join(dirname(__file__), 'config.json')
with open(config_path, 'r') as f:
    config = load(f)

# Инициализация объектов и переменных из конфигурационного файла
folder_logfile = config['folder_logfile']
logfile = config['logfile']
loglevel = config['loglevel']

log_hasher = Logger(folder_logfile, logfile, loglevel)


