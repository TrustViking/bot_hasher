#!/usr/bin/env python3
#
import subprocess, os, sys, psutil, pynvml, logging, asyncio
from bot_env.mod_log import Logger
import argparse
from argparse import ArgumentParser

class Start:
    """Module for START"""
    countInstance=0
    #
    def __init__(self, 
                 log_file='log.md', 
                 log_level=logging.DEBUG,
                 folder_video= 'diff_video',
                 folder_kframes = 'diff_kframes',
                 hash_factor=0.2,
                 threshold_keyframes=0.3,
                 logo_size=180,
                 withoutlogo=False,
                 max_download=3,
                 ):
        Start.countInstance += 1
        self.countInstance = Start.countInstance
        #
        self.log_file=log_file
        self.log_level=log_level
        self.folder_video=folder_video
        self.folder_kframes=folder_kframes
        self.hash_factor=hash_factor
        self.threshold_keyframes=threshold_keyframes
        self.logo_size=logo_size
        self.withoutlogo=withoutlogo
        self.max_download = max_download
        # Разбор аргументов командной строки
        self._arg_parser()
        # Logger
        self.Logger = Logger(self.log_file, self.log_level)
        self._print()

        #     
    # выводим № объекта
    def _print(self):
        print(f'\n[Start] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'\n[Start] countInstance: [{self.countInstance}]\n')
        print(f'Аргументы:\n'
              f'log_file: {self.log_file}\n'
              f'log_level: {self.log_level}\n'
              f'folder_video: {self.folder_video}\n'
              f'folder_kframes: {self.folder_kframes}\n'
              f'hash_factor: {self.hash_factor}\n'
              f'threshold_keyframes: {self.threshold_keyframes}\n'
              f'logo_size: {self.logo_size}\n'
              f'withoutlogo: {self.withoutlogo}\n'
              f'max_download: {self.max_download}\n'
              )
    # 
    # добавление аргументов строки
    def _arg_added(self, parser: ArgumentParser):
        # Добавление аргументов
        parser.add_argument('-v', '--folder_video', type=str, help='Папка для видео')
        parser.add_argument('-k', '--folder_kframes', type=str, help='Папка для схожих кадров')
        parser.add_argument('-f', '--log_file', type=str, help='Имя журнала логгирования')
        parser.add_argument('-l', '--log_level', type=str, help='Уровень логгирования')
        # множитель (0.n*len(hash)) для определения порога расстояния Хэминга  
        parser.add_argument('-m', '--hash_factor', type=float, help='Множитель порога')
        # порог (больше порог, меньше кадров) для гистограммы ключевых кадров (0-1)
        parser.add_argument('-t', '--threshold_keyframes', type=float, help='Порог ключевых кадров')
        parser.add_argument('-z', '--logo_size', type=int, help='Cторона квадрата для удаления лого')
        parser.add_argument('--withoutlogo', action='store_true', help='Удаляем лого')

    # Разбор аргументов строки
    def _arg_parser(self):
        # Инициализация парсера аргументов
        parser = ArgumentParser()
        # добавление аргументов строки
        self._arg_added(parser)
        args = parser.parse_args()

        if args.log_file: self.log_file=args.log_file
        # (CRITICAL, ERROR, WARNING,INFO, DEBUG)
        if args.log_level: self.log_level=logging.getLevelName(args.log_level.upper())
        #
        if args.folder_video: self.folder_video=args.folder_video
        if args.folder_kframes: self.folder_kframes=args.folder_kframes
        if args.hash_factor: self.hash_factor=args.hash_factor
        if args.threshold_keyframes: self.threshold_keyframes=args.threshold_keyframes
        if args.logo_size: self.logo_size=args.logo_size
        if args.withoutlogo: self.withoutlogo=True

    # Функция для логирования информации о памяти
    def log_memory(self):
        self.Logger.log_info(f'****************************************************************')
        self.Logger.log_info(f'*Data RAM {os.path.basename(sys.argv[0])}: [{psutil.virtual_memory()[2]}%]')
        # Инициализируем NVML для сбора информации о GPU
        pynvml.nvmlInit()
        deviceCount = pynvml.nvmlDeviceGetCount()
        self.Logger.log_info(f'\ndeviceCount [{deviceCount}]')
        for i in range(deviceCount):
            handle = pynvml.nvmlDeviceGetHandleByIndex(i)
            meminfo = pynvml.nvmlDeviceGetMemoryInfo(handle)
            self.Logger.log_info(f"#GPU [{i}]: used memory [{int(meminfo.used / meminfo.total * 100)}%]")
            self.Logger.log_info(f'****************************************************************\n')
        # Освобождаем ресурсы NVML
        pynvml.nvmlShutdown()

    # Асинхронная функция для запуска скрипта
    async def run_script(self, script):
        self.Logger.log_info(f'[run_script] command: {script}')
        # Используем asyncio для асинхронного запуска скрипта
        process = await asyncio.create_subprocess_shell(script)
        await process.wait()


# MAIN **************************
# Главная асинхронная функция
async def main():
    print(f'\nСтарт приложения...\n') 
    # print(f'\n==============================================================================\n')
    print(f'File: [{os.path.basename(sys.argv[0])}]')
    print(f'Path: [{sys.path[0]}]') 
    print(f'Data memory: [{psutil.virtual_memory()}]')
    # Создаем экземпляр класса Start
    start = Start()
    start.log_memory()
    #
    # Список скриптов для запуска
    args_bot_telega=''
    args_bot_hasher=''    
    
    if start.folder_video: 
        folder_video_arg=f'--folder_video {start.folder_video} '
        args_bot_telega+=folder_video_arg
        args_bot_hasher+=folder_video_arg

    if start.folder_kframes:
        folder_kframes_arg=f'--folder_kframes {start.folder_kframes} '
        args_bot_telega+=folder_kframes_arg
        args_bot_hasher+=folder_kframes_arg
    
    if start.log_file:
        log_file_arg=f'--log_file {start.log_file} '
        args_bot_telega+=log_file_arg
        args_bot_hasher+=log_file_arg

    if start.log_level:
        log_level_arg=f'--log_level {start.log_level} '
        args_bot_telega+=log_level_arg
        args_bot_hasher+=log_level_arg

    if start.hash_factor:
        hash_factor_arg=f'--hash_factor {start.hash_factor} '
        args_bot_hasher+=hash_factor_arg
    
    if start.threshold_keyframes:
        threshold_keyframes_arg=f'--threshold_keyframes {start.threshold_keyframes} '    
        args_bot_hasher+=threshold_keyframes_arg
    
    if start.logo_size:
        logo_size_arg=f'--logo_size {start.logo_size} '
        args_bot_hasher+=logo_size_arg
    
    if start.withoutlogo:
        withoutlogo_arg='--withoutlogo'
        args_bot_hasher+=withoutlogo_arg
    print(f'\n[main start] \nargs_bot_telega: {args_bot_telega}'
          f'\nargs_bot_hasher: {args_bot_hasher}')

    scripts = [
        os.path.join(sys.path[0], f'bot_telega.py {args_bot_telega}'),
        # os.path.join(sys.path[0], f'bot_hasher.py --folder_video {start.folder_video} --folder_kframes {start.folder_kframes} --log_file {start.log_file} --log_level {start.log_level} --hash_factor {start.hash_factor} --threshold_keyframes {start.threshold_keyframes}'),
        os.path.join(sys.path[0], f'bot_hasher.py {args_bot_hasher}'),
        os.path.join(sys.path[0], f'bot_sender.py {args_bot_telega}'),
                ]
    # создаем БД и таблицы
    # Запускаем скрипт make_db с помощью subprocess
    try:
        subprocess.run(["python3", os.path.join(sys.path[0], 'make_db.py')], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running make_db: {e}")
        start.Logger.log_info(f"Error running make_db: {e}")
    #
    # Запускаем скрипты асинхронно
    tasks = [start.run_script(script) for script in scripts]
    await asyncio.gather(*tasks)

# Запускаем главную асинхронную функцию
if __name__ == "__main__":
    asyncio.run(main())



