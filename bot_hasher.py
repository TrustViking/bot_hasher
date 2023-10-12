#!/usr/bin/env python3 
#
from typing import Coroutine, Callable, Any
from time import sleep, strftime
from datetime import datetime as dt
from asyncio import run 
from pynvml import nvmlInit, nvmlDeviceGetCount, nvmlDeviceGetHandleByIndex, nvmlDeviceGetMemoryInfo, nvmlShutdown
from logging import getLevelName
from os.path import join, getmtime, basename
from os import makedirs, listdir, remove
from sys import platform, argv, path
from psutil import virtual_memory
from argparse import ArgumentParser
# from sqlalchemy.engine.result import Row
#
from videomaker.comparator import VidCompar
from bot_env.mod_log import Logger
from data_base.base_db import BaseDB
#
#
#
class HasherVid:
    """Modul for hasher video"""
    countInstance=0
    #
    def __init__(self,
                folder_logfile = 'logs',
                logfile='hashervid_log.md', 
                loglevel='DEBUG',
                folder_db = 'db_storage',
                name_db = 'db_file.db',
                folder_video= 'diff_video',
                folder_kframes = 'diff_kframes',
                pause_minut=1,
                # hash_factor=0.2, # множитель (0.n*len(hash)) для определения порога расстояния Хэминга  
                # threshold_keyframes=0.3, # порог (больше порог, меньше кадров) для гистограммы ключевых кадров (0-1)
                # withoutlogo=False,
                # logo_size=180,
                # number_corner = '2',
                 ):
        HasherVid.countInstance += 1
        self.countInstance = HasherVid.countInstance
        #
        # args Logger
        self.folder_logfile = folder_logfile
        self.logfile=logfile
        self.loglevel=loglevel
        # args BaseDB
        self.folder_db = folder_db
        self.name_db = name_db
        #
        self.folder_video=folder_video
        self.folder_kframes=folder_kframes
        #        
        self.pause_minut = pause_minut
        # self.hash_factor=hash_factor
        # self.threshold_keyframes=threshold_keyframes
        # self.withoutlogo=withoutlogo
        # self.logo_size=logo_size
        # self.number_corner = number_corner
        # Разбор аргументов командной строки
        self._arg_parser()
        #
        # Logger
        self.Logger = Logger(self.folder_logfile, self.logfile, self.loglevel)
        # BaseDB
        self.Db=BaseDB(self.Logger, self.folder_db, self.name_db)
        #
        # VidCompar
        self.Cmp=self._cmp_def(self.Logger, self.Db)
        #
        # создаем рабочие пути
        self.save_file_path=join(path[0], self.folder_video)
        self.path_save_keyframe=join(path[0], self.folder_kframes)
        self.create_directory([self.save_file_path, self.path_save_keyframe])
        self.days_del=2
        self.time_del = 24 * 60 * 60 * self.days_del #  
        self._print()
    #
    # создаем VidCompar
    def _cmp_def(self, logger: Logger, method_db: BaseDB):
        return VidCompar(
                        logger = logger,
                        method_db = method_db,
                        save_file_path=self.save_file_path,
                        path_save_keyframe=self.path_save_keyframe,
                        # hash_factor=self.hash_factor, 
                        # threshold_keyframes=self.threshold_keyframes,
                        # withoutlogo=self.withoutlogo,
                        # logo_size=self.logo_size,
                        # number_corner=self.number_corner,
                        )

    # выводим № объекта
    def _print(self):
        print(f'\n[HasherVid] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'\n[HasherVid] countInstance: [{self.countInstance}]\n')
        msg = (f"Started at {strftime('%X')}\n"
              f'platform: [{platform}]\n'
              f'\nАргументы:\n'
              f'folder_logfile: {self.folder_logfile}\n'
              f'logfile: {self.logfile}\n'
              f'loglevel: {self.loglevel}\n'
              f'folder_db: {self.folder_db}\n'
              f'name_db: {self.name_db}\n'
              f'folder_video: {self.folder_video}\n'
              f'folder_kframes: {self.folder_kframes}\n'
              f'pause_minut: {self.pause_minut}\n'
            #   f'hash_factor: {self.hash_factor}\n'
            #   f'threshold_keyframes: {self.threshold_keyframes}\n'
            #   f'withoutlogo: {self.withoutlogo}\n'
            #   f'logo_size: {self.logo_size}\n'
            #   f'number_corner: {self.number_corner}\n'
              )
        print(msg)
        self.Logger.log_info(msg)
#
    # добавление аргументов командной строки
    def _arg_added(self, parser: ArgumentParser):
        # Добавление аргументов
        parser.add_argument('-fd', '--folder_db', type=str, help='Папка для БД')
        parser.add_argument('-nd', '--name_db', type=str, help='Имя файла БД')
        #
        parser.add_argument('-fv', '--folder_video', type=str, help='Папка для видео')
        parser.add_argument('-fk', '--folder_kframes', type=str, help='Папка для схожих кадров')
        #
        parser.add_argument('-fl', '--folder_logfile', type=str, help='Папка для логов')
        parser.add_argument('-lf', '--logfile', type=str, help='Имя журнала логгирования')
        parser.add_argument('-ll', '--loglevel', type=str, help='Уровень логирования')
        # 
        parser.add_argument('-pm', '--pause_minut', type=int, help='Пауза в работе бота')
        # parser.add_argument('-hf', '--hash_factor', type=float, help='Множитель порога (0-1) (0.n*len(hash)) для определения порога расстояния Хэминга')
        # parser.add_argument('-tk', '--threshold_keyframes', type=float, help='Порог (0-1) ключевых кадров (больше порог, меньше кадров) для гистограммы ключевых кадров (0-1)')
        # parser.add_argument('-ls', '--logo_size', type=int, help='Cторона квадрата для удаления лого')
        # parser.add_argument('-nc', '--number_corner', type=int, help='Номер угла, где будет удален лого верхний левый - 1, правый нижний - 3')
        # parser.add_argument('--withoutlogo', action='store_true', help='Удаляем лого')

    # Разбор аргументов строки
    def _arg_parser(self):
        # Инициализация парсера аргументов
        parser = ArgumentParser()
        # добавление аргументов строки
        self._arg_added(parser)
        args = parser.parse_args()
        
        if args.folder_db: 
            self.folder_db=args.folder_db
        if args.name_db: 
            self.name_db=args.name_db

        if args.folder_video: 
            self.folder_video=args.folder_video
        if args.folder_kframes: 
            self.folder_kframes=args.folder_kframes
        #
        if args.folder_logfile: 
            self.folder_logfile=args.folder_logfile
        if args.logfile: 
            self.logfile=args.logfile
        if args.loglevel: 
            self.loglevel=getLevelName(args.loglevel.upper()) # (CRITICAL, ERROR, WARNING,INFO, DEBUG)
        #
        if args.pause_minut: 
            self.pause_minut=args.pause_minut
        # if args.hash_factor: 
        #     self.hash_factor=args.hash_factor
        # if args.threshold_keyframes: 
        #     self.threshold_keyframes=args.threshold_keyframes
        # if args.logo_size: 
        #     self.logo_size=args.logo_size
        # if args.number_corner: 
        #     self.number_corner=args.number_corner
        # if args.withoutlogo: 
        #     self.withoutlogo=True
    
    # создаем директорию, если такой папки нет
    def create_directory(self, paths: list[str]):
        """
        Создает директорию для хранения video и ключевых кадров, 
        если она не существует

         Аргументы:
        - paths: список строк, каждая из которых является путем к директории, которую необходимо создать.
        """
        _ = [makedirs(path,  exist_ok=True) for path in paths]

    # асинхронная обертка для безопасного выполнения методов
    # async def safe_execute(self, coroutine: Callable[..., Coroutine[Any, Any, T]]) -> T:
    async def safe_await_execute(self, coroutine: Coroutine, name_func: str = None):
        try:
            # print(f'\n***HasherVid safe_await_execute: выполняем обертку ****')
            return await coroutine
        except Exception as eR:
            print(f'\nERROR[HasherVid {name_func}] ERROR: {eR}') 
            self.Logger.log_info(f'\nERROR[HasherVid {name_func}] ERROR: {eR}') 
            return None

    # синхронная обертка для безопасного выполнения методов
    def safe_execute(self, func: Callable[..., Any], name_func: str = None):
        try:
            return func()
        except Exception as eR:
            print(f'\nERROR[HasherVid {name_func}] ERROR: {eR}')
            self.Logger.log_info(f'\nERROR[HasherVid {name_func}] ERROR: {eR}')
            return None
    
    # из 'diff' скачанные видео, но не сравнивали 
    async def rows4diff (self, 
                        name_table: str, 
                        one_column_name: str, 
                        one_params_status: str,
                        two_column_name: str,
                        two_params_status: str):
        # отбираем в таблице 'frag' скачанные, но не фрагментированные 
        try:
            async_results = await self.Db.read_data_two( 
                            name_table = name_table,  
                            one_column_name = one_column_name, 
                            one_params_status = one_params_status,
                            two_column_name = two_column_name, 
                            two_params_status = two_params_status,
                                                        )
        except Exception as eR:
            print(f"\n[HasherVid rows4diff] Не удалось прочитать таблицу diff: {eR}")
            self.Logger.log_info(f"\n[HasherVid rows4diff] Не удалось прочитать таблицу diff: {eR}")
            return None
        rows = async_results.fetchall()
        if not rows: 
            return None
        return rows
            #
    # удаляет все файлы, которые старше одного дня 
    async def delete_old_files(self):
        # множество имен файлов, которые находятся на диске
        set_nfile_dir = set(listdir(self.save_file_path))
        # текущее время
        current_time = dt.now().timestamp()
        for name_file in set_nfile_dir:
            full_path = join(self.save_file_path, name_file)
            # время последнего изменения файла
            file_mod_time = getmtime(full_path)
            # если файл старше self.time_del
            if current_time - file_mod_time > self.time_del:
                try:
                    remove(full_path)
                    print(f"\n[Hashervid delete_old_files] Файл {full_path} успешно удалён.") 
                    self.Logger.log_info(f"[hashervid delete_old_files] Файл {full_path} успешно удалён.") 
                except Exception as eR:
                    print(f"\n[Hashervid delete_old_files] Не удалось удалить файл {full_path}: {eR}")
                    self.Logger.log_info(f'\nERROR [Hashervid delete_old_files] ERROR: {eR}')

    # логирование информации о памяти
    def log_memory(self):
        self.Logger.log_info(f'****************************************************************')
        self.Logger.log_info(f'*Data RAM {basename(argv[0])}: [{virtual_memory()[2]}%]')
        # Инициализируем NVML для сбора информации о GPU
        nvmlInit()
        deviceCount = nvmlDeviceGetCount()
        self.Logger.log_info(f'\ndeviceCount [{deviceCount}]')
        for i in range(deviceCount):
            handle = nvmlDeviceGetHandleByIndex(i)
            meminfo = nvmlDeviceGetMemoryInfo(handle)
            self.Logger.log_info(f"#GPU [{i}]: used memory [{int(meminfo.used / meminfo.total * 100)}%]")
            self.Logger.log_info(f'****************************************************************\n')
        # Освобождаем ресурсы NVML
        nvmlShutdown()

    # выводим состояние системы
    def system_status(self):
        print(f'\nName: {__name__}. \nСтарт приложения...\n') 
        file_start = basename(argv[0])
        path_start = path[0]
        msg = (
            f'File: [{file_start}]\n'
            f'Path: [{path_start}]\n'
            f'Data memory:'
                )
        print(msg)
        memory = virtual_memory()
        for field in memory._fields:
            print(f"{field}: {getattr(memory, field)}")    

#
# MAIN **************************
async def main():
    # Создаем экземпляр класса Start
    # создаем объект и в нем регистрируем хэндлеры Клиента
    hasher=HasherVid()
    hasher.log_memory() # логирование информации о памяти
    hasher.system_status() # выводим состояние системы
    minut=hasher.pause_minut
    while True:
        print(f'\nБот по сравнению видео ждет {minut} минут(ы) ...')
        sleep (int(60*minut))
        print(f'\nСодержание таблиц в БД...')
        await hasher.Db.print_tables('diff')
        # удаляем все файлы, которые старше...
        await hasher.delete_old_files()

        # формируем список сравнений из таблицы diff
        rows = await hasher.safe_await_execute(hasher.rows4diff(
                                                name_table = 'diff',  
                                                one_column_name = 'dnld', 
                                                one_params_status = 'dnlded',
                                                two_column_name = 'in_work', 
                                                two_params_status = 'not_diff',
                                                                ), 
                                                '[HasherVid main] hasher.rows4diff'
                                                ) 
        if not rows: 
            continue
        ### точка входа сравнения файлов 
        ### надо будет добавить параллельное выполнение
        for row in rows:
            path2file = await hasher.Cmp.compar_vid_hash(row)
            if not path2file:
                print(f'\n[HasherVid compar_vid_hash] Не записали на диск схожие кадры')


if __name__ == "__main__":
    run(main())


