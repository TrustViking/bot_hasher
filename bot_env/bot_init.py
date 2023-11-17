

from typing import Coroutine, List, Optional, Dict, Union
from os import getenv
from os.path import basename, join, isfile, abspath, dirname
from json import load, JSONDecodeError
from aiogram import Bot, Dispatcher, Router
from aiogram.fsm.storage.memory import MemoryStorage
from .mod_log import LogBot
from .decorators import safe_execute



class ConfigInitializer:
    def __init__(self, logger: LogBot = None):
        self.logger = logger if logger else LogBot()

    def read_config(self, config_path: str) -> Optional[Dict[str, Union[int, str]]]:
        @safe_execute(self.logger, name_method="read_config")
        def _read_config(config_path: str):
            if not isfile(config_path):
                print(f'\nERROR [ConfigInitializer  read_config] not exist config_path: {config_path}')
                return None
            
            try:
                with open(config_path, 'r') as f:
                    return load(f)
            except JSONDecodeError as e:
                print(f"\nERROR [LogInitializer read_config] Error decoding JSON config: {e}")
                return None
        return _read_config(config_path)


class LogInitializer(ConfigInitializer):
    def __init__(self):
        super().__init__(LogBot())

    def initialize(self, config_path: str)-> Optional[LogBot]:
        @safe_execute(self.logger, name_method="InitializeLogger")
        def _initialize():
            config = self.read_config(config_path)
            if config is None:
                print("\n[LogInitializer initialize] Failed to read configuration.")
                return
            folder_logfile = config['folder_logfile']
            logfile = config['logfile']
            loglevel = config['loglevel']
            self.logger = LogBot(folder_logfile, logfile, loglevel)
            return self.logger
        return _initialize()


class BotInitializer(LogInitializer):
    def __init__(self, logger: LogBot):
        self.logger = logger
        self.bot = None
        self.dp = None
        # self.router = None
        super().__init__()

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
            # self.router = Router() # будем получать из client.py
        return _initialize()

    def get_bot(self) -> Bot:
            if self.bot is None:
                raise ValueError("Bot has not been initialized.")
            return self.bot

    def get_dp(self) -> Dispatcher:
        if self.dp is None:
            raise ValueError("Dispatcher has not been initialized.")
        return self.dp

    # def get_router(self) -> Router:
    #     if self.router is None:
    #         raise ValueError("Router has not been initialized.")
    #     return self.router
