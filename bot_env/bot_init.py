

from typing import Coroutine, List, Optional, Dict, Union
from os import getenv
from os.path import basename, join, isfile, abspath, dirname
from json import load, JSONDecodeError
from aiogram import Bot, Dispatcher, Router
from aiogram.fsm.storage.memory import MemoryStorage
from .mod_log import Logger
from .decorators import safe_execute



class ConfigInitializer:
    def __init__(self, logger: Logger = None):
        self.logger = logger if logger else Logger()

    def read_config(self, config_path: str) -> Optional[Dict[str, Union[int, str]]]:
        @safe_execute(self.logger, name_method="ReadConfig")
        def _read_config(config_path: str):
            if not isfile(config_path):
                print(f'\nERROR [ConfigInitializer  _read_config] not exist config_path: {config_path}')
                return None
            # config_path = join(dirname(__file__), 'config.json')
            # print(f'\n[ConfigInitializer  _read_config]  config_path: {config_path}')
            
            try:
                with open(config_path, 'r') as f:
                    return load(f)
            except JSONDecodeError as e:
                print(f"\nERROR[LogInitializer _read_config] Error decoding JSON config: {e}")
                return None
        return _read_config(config_path)


class LogInitializer(ConfigInitializer):
    def __init__(self):
        super().__init__(Logger())
        # self.initialize()

    def initialize(self, config_path: str):
        @safe_execute(self.logger, name_method="InitializeLogger")
        def _initialize():
            config = self.read_config(config_path)
            if config is None:
                print("\n[LogInitializer _read_config] Failed to read configuration.")
                return
            folder_logfile = config['folder_logfile']
            logfile = config['logfile']
            loglevel = config['loglevel']
            self.logger = Logger(folder_logfile, logfile, loglevel)
            return self.logger
        return _initialize()


class BotInitializer(LogInitializer):
    def __init__(self, logger: Logger):
        self.logger = logger
        self.bot = None
        self.dp = None
        self.router = None
        super().__init__()
        self.initialize_bot()

    def initialize_bot(self, config_path: str):
        @safe_execute(self.logger, name_method="InitializeBot")
        def _initialize():
            config = self.read_config(config_path)
            if config is None:
                print("Failed to read configuration.")
                return
            token = getenv('TELEGRAM_TOKEN_HASHER')
            self.bot = Bot(token)
            self.dp = Dispatcher(storage=MemoryStorage())
            self.router = Router()
        return _initialize()

