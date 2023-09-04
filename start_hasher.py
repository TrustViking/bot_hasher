#!/usr/bin/env python3
#
import subprocess, os, sys, psutil, pynvml, logging, asyncio
from bot_env.mod_log import Logger

class Start:
    """Module for START"""
    countInstance=0
    #
    def __init__(self, 
                 log_file='main_log.txt', 
                 log_level=logging.DEBUG,
                 max_download=3,
                 ):
        Start.countInstance += 1
        self.countInstance = Start.countInstance
        # Logger
        self.Logger = Logger(log_file=log_file, log_level=log_level)
        self.max_download = max_download
        self._print()

        #     
    # выводим № объекта
    def _print(self):
        print(f'[Start] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'\n[Start] countInstance: [{self.countInstance}]\n')
    # 
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
    #
    # Список скриптов для запуска
    scripts = [
        os.path.join(sys.path[0], 'bot_telega.py'),
        os.path.join(sys.path[0], 'bot_hasher.py'),
        # os.path.join(sys.path[0], 'bot_dnld.py'),
        # os.path.join(sys.path[0], 'bot_sender.py'),
                ]
    #
    print(f'\nСтарт приложения...') 
    print(f'\n==============================================================================\n')
    print(f'File: [{os.path.basename(sys.argv[0])}]')
    print(f'Path: [{sys.path[0]}]') 
    #
    # Создаем экземпляр класса Start
    start = Start(max_download=5)
    print(f'Data memory: [{psutil.virtual_memory()}]')
    start.log_memory()
    #
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



