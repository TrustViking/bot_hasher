
from typing import Coroutine, Callable, Union, Set, Tuple, List, Optional, Any, Dict
from logging import Logger, getLevelName, getLogger, Formatter, FileHandler 
from os.path import join, dirname, exists, abspath
from os import makedirs
from sys import platform, argv, path
from time import time, strftime

class LogBot:
    """
        Modul for making Logger for Bot_Hasher

        Конструктор класса Logger.

        Аргументы:
        - folder_logfile: Имя папки хранения файла логирования. По умолчанию [logs].
        - logfile: Имя файла логирования. По умолчанию [logger.md].
        - loglevel: Уровень логирования. По умолчанию [logging.DEBUG].

        Возможные уровни логирования:
        - DEBUG: Детальная отладочная информация.
        - INFO: Информационные сообщения.
        - WARNING: Предупреждения.
        - ERROR: Ошибки, которые не приводят к прекращению работы программы.
        - CRITICAL: Критические ошибки, которые приводят к прекращению работы программы.
    """

    CRITICAL = 50
    FATAL = CRITICAL
    ERROR = 40
    WARNING = 30
    WARN = WARNING
    INFO = 20
    DEBUG = 10
    NOTSET = 0
    
    countInstance=0

    
    def __init__(self,
                folder_logfile='logs', 
                logfile='logger.md', 
                loglevel="DEBUG",
                 ):
        LogBot.countInstance += 1
        self.countInstance = LogBot.countInstance
        
        self.cls_name = self.__class__.__name__

        self.folder_logfile=folder_logfile
        self.logfile=logfile
        self.loglevel=getLevelName(loglevel)
        #
        self.directory = join(path[0], self.folder_logfile)
        self.create_directory([self.directory])
        #
        self.path_to_logfile = join(self.directory, self.logfile)
        self.logger = self.setup_logger(self.loglevel, self.path_to_logfile)
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
            'countInstance',
            'cls_name',
            'folder_logfile',
            'logfile',
            'loglevel',
            'directory',
            'path_to_logfile',
        ]

        for attr in attributes_to_print:
            # "Attribute not found" будет выведено, если атрибут не существует
            value = getattr(self, attr, "Attribute not found")  
            msg += f"{attr}: {value}\n"

        print(msg)
        self.logger.info(msg)

    # создаем директорию, если такой папки нет
    def create_directory(self, paths: list[str]):
        """
        Создает директорию, если она не существует

         Аргументы:
        - paths: список строк, каждая из которых является путем к директории, 
                 которую необходимо создать.
        """
        _ = [makedirs(path,  exist_ok=True) for path in paths]


    def setup_logger(self, log_level: Union[int, str], path_logfile: str) -> Optional[Logger]:
        """
        Настраивает логгер.

        Возвращает:
        - logger: Объект логера.
        """
        
        nameToLevel = {
            'CRITICAL': LogBot.CRITICAL,
            'FATAL': LogBot.FATAL,
            'ERROR': LogBot.ERROR,
            'WARN': LogBot.WARNING,
            'WARNING': LogBot.WARNING,
            'INFO': LogBot.INFO,
            'DEBUG': LogBot.DEBUG,
            'NOTSET': LogBot.NOTSET,
        }

        if isinstance(log_level, int):
            loglevel = log_level
            # print(f'\n[{__name__}|{self.cls_name}] loglevel(int): {loglevel}')
        elif isinstance(log_level, str):
            if log_level not in nameToLevel:
                print(f'\n[{__name__}|{self.cls_name}] ERROR log_level: {log_level} not in nameToLevel')
                return None
            # print(f'\n[{__name__}|{self.cls_name}] loglevel(str): {log_level}')
            loglevel = nameToLevel.get(log_level.upper())
        else:
            print(f'\n[{__name__}|{self.cls_name}] ERROR log_level: {log_level} is not int or str')
            return None
        
        # создаем логгер
        # logger = getLogger(__name__) # ссылаться на имя текущего модуля, а не на имя класса
        logger = getLogger(self.cls_name)
        # устанавливаем уровень логгирования
        logger.setLevel(loglevel)
        
        # хэндлер
        file_handler = FileHandler(path_logfile)
        file_handler.setLevel(loglevel)
        formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        # добавляем хэндлер в логгер
        logger.addHandler(file_handler)
        #
        return logger

    def log_info(self, message: Any) -> None:
        """
        Записывает информационное сообщение в лог.

        Аргументы:
        - message: Сообщение для записи в лог.
        """
        self.logger.info(message)


