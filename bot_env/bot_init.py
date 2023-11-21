

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
        self.cls_name = self.__class__.__name__


    def read_config(self, config_path: str) -> Optional[Dict[str, Union[int, str]]]:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def _read_config(config_path: str):
            
            if not isfile(config_path):
                msg = f'\nERROR [{__name__}|{self.cls_name}]] not exist config_path: {config_path}' 
                print(msg)
                self.logger.log_info(msg)
                return None
            
            try:
                with open(config_path, 'r') as f:
                    return load(f)
            except JSONDecodeError as e:
                msg = f"\nERROR [{__name__}|{self.cls_name}] Error decoding JSON config: {e}"
                print(msg)
                self.logger.log_info(msg)
                return None
        return _read_config(config_path)


class LogInitializer(ConfigInitializer):
    def __init__(self):
        super().__init__(LogBot())
        self.cls_name = self.__class__.__name__


    def initialize(self, config_path: str)-> Optional[LogBot]:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def _initialize():
            config = self.read_config(config_path)
            if config is None:
                msg = f"\n[{__name__}|{self.cls_name}] Failed to read configuration."
                print(msg)
                self.logger.log_info(msg)
                return
            
            folder_logfile = config['folder_logfile']
            logfile = config['logfile']
            loglevel = config['loglevel']
            
            self.logger = LogBot(folder_logfile, logfile, loglevel)
            return self.logger
        return _initialize()


class BotInitializer():
    def __init__(self, logger: LogBot):
        self.logger = logger
        self.cls_name = self.__class__.__name__
        self.bot = None
        self.dp = None

    def initialize_bot(self):
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def _initialize_bot():
            
            token = getenv('TELEGRAM_TOKEN_HASHER')
            if not token:
                raise ValueError(f"\n[{__name__}|{self.cls_name}] TELEGRAM_TOKEN_HASHER is not set.")
            
            self.bot = Bot(token)
            if not self.bot:
                raise ValueError(f"\n[{__name__}|{self.cls_name}] BOT (self.bot) is not set.")
            
            self.dp = Dispatcher(storage=MemoryStorage())
            if not self.dp:
                raise ValueError(f"\n[{__name__}|{self.cls_name}] Dispatcher (self.dp) is not set.")
        
        return _initialize_bot()


    def get_bot(self) -> Bot:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def _get_bot():
            if self.bot is None:
                raise ValueError(f"\n[{__name__}|{self.cls_name}] Bot has not been initialized.")
            return self.bot
        return _get_bot()


    def get_dp(self) -> Dispatcher:
        @safe_execute(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        def _get_dp():
            if self.dp is None:
                raise ValueError(f"\n[{__name__}|{self.cls_name}] Dispatcher has not been initialized.")
            return self.dp
        return _get_dp()
    


