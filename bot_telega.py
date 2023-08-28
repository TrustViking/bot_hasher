#!/usr/bin/env python3
#
import logging
from aiogram.utils import executor
#
from bot_env.create_obj4bot import dp
from bot_env.mod_log import Logger
from handlers.client import  Client2bot
#
#
class Telega:
    """Modul for TELEGRAM"""
    countInstance=0
    #
    def __init__(self, 
                 log_file='telega_log.txt', 
                 log_level=logging.DEBUG,
                 ):
        Telega.countInstance += 1
        self.countInstance = Telega.countInstance
        # Logger
        self.Logger = Logger(log_file=log_file, log_level=log_level)
        # Client
        self.client = Client2bot(logger=self.Logger)
        self._print()
    #
    # выводим № объекта
    def _print(self):
        print(f'[Telega] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'[Telega] countInstance: [{self.countInstance}]')
#
    # запускаем клиент бот-телеграм
    async def client_work(self):
        try:
            await self.client.register_handlers_client()            
        except Exception as eR:
            print (f'[main] error: {eR}')
            self.Logger.log_info(f'[main] error: {eR}')   
    #
# MAIN **************************
async def main(_):
    print(f'\n**************************************************************************')
    print(f'\nБот вышел в онлайн')
    # создаем объект и в нем регистрируем хэндлеры Клиента
    telega=Telega()  
    telega.Logger.log_info(f'\n[main] Создали объект Telega()')
    # print(f'\n[main] Создали объект Telega()')
    await telega.client_work()
#
if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=main)




