
from logging import getLevelName, getLogger, Formatter, FileHandler 
from os.path import join, dirname, exists, abspath
from os import makedirs
from sys import platform, argv, path
from time import time, strftime

class Logger:

    CRITICAL = 50
    FATAL = CRITICAL
    ERROR = 40
    WARNING = 30
    WARN = WARNING
    INFO = 20
    DEBUG = 10
    NOTSET = 0

    def __init__(self,
                folder_logfile='logs', 
                logfile='logger.md', 
                loglevel="DEBUG",
                 ):
        """
        Конструктор класса Logger.

        Аргументы:
        - logfile: Имя файла логирования. 
        - loglevel: Уровень логирования. По умолчанию logging.DEBUG.

        Возможные уровни логирования:
        - DEBUG: Детальная отладочная информация.
        - INFO: Информационные сообщения.
        - WARNING: Предупреждения.
        - ERROR: Ошибки, которые не приводят к прекращению работы программы.
        - CRITICAL: Критические ошибки, которые приводят к прекращению работы программы.
        """
        self.cls_name = self.__class__.__name__
        self.folder_logfile=folder_logfile
        self.logfile=logfile
        self.loglevel=getLevelName(loglevel)
        #
        self.directory = join(path[0], self.folder_logfile)
        self.create_directory([self.directory])
        #
        self.parh_to_logfile = join(self.directory, self.logfile)
        self.logger = self.setup_logger(self.loglevel, self.parh_to_logfile)
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
            'folder_logfile',
            'logfile',
            'loglevel',
            'directory',
            'parh_to_logfile',
        ]

        for attr in attributes_to_print:
            # "Attribute not found" будет выведено, если атрибут не существует
            value = getattr(self, attr, "Attribute not found")  
            msg += f"{attr}: {value}\n"

        print(msg)
        self.logger.log_info(msg)

    # создаем директорию, если такой папки нет
    def create_directory(self, paths: list[str]):
        """
        Создает директорию для хранения video и ключевых кадров, 
        если она не существует

         Аргументы:
        - paths: список строк, каждая из которых является путем к директории, которую необходимо создать.
        """
        _ = [makedirs(path,  exist_ok=True) for path in paths]


    def setup_logger(self, log_level: int or str, parh_logfile: str):
        """
        Настраивает логгер.

        Возвращает:
        - logger: Объект логгера.
        """
        
        nameToLevel = {
            'CRITICAL': Logger.CRITICAL,
            'FATAL': Logger.FATAL,
            'ERROR': Logger.ERROR,
            'WARN': Logger.WARNING,
            'WARNING': Logger.WARNING,
            'INFO': Logger.INFO,
            'DEBUG': Logger.DEBUG,
            'NOTSET': Logger.NOTSET,
        }

        levelToName = {
            Logger.CRITICAL: 'CRITICAL',
            Logger.ERROR: 'ERROR',
            Logger.WARNING: 'WARNING',
            Logger.INFO: 'INFO',
            Logger.DEBUG: 'DEBUG',
            Logger.NOTSET: 'NOTSET',
        }

        if isinstance(log_level, int):
            loglevel = log_level
            # print(f'\n[{__name__}|{self.cls_name}] loglevel(int): {loglevel}')
        elif isinstance(log_level, str):
            if log_level not in nameToLevel:
                print(f'\n[{__name__}|{self.cls_name}] ERROR log_level: {log_level} not in nameToLevel')
                return None
            # print(f'\n[{__name__}|{self.cls_name}] loglevel(str): {log_level}')
            loglevel = nameToLevel[log_level]
        else:
            print(f'\n[{__name__}|{self.cls_name}] ERROR log_level: {log_level} is not int or str')
            return None
        
        # хэндлер
        file_handler = FileHandler(parh_logfile)
        file_handler.setLevel(loglevel)
        formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        # создаем логгер
        logger = getLogger(__name__)
        # устанавливаем уровень логгирования
        logger.setLevel(loglevel)
        # добавляем хэндлер в логгер
        logger.addHandler(file_handler)
        #
        return logger

    def log_info(self, message: str):
        """
        Записывает информационное сообщение в лог.

        Аргументы:
        - message: Сообщение для записи в лог.
        """
        self.logger.info(message)


