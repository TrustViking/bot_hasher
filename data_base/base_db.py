

from typing import List
from time import strftime
from os.path import join, abspath, dirname, isfile
from os import makedirs
from sys import platform, path
from sqlalchemy import update, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.future import select
#
from bot_env.mod_log import Logger
from data_base.table_db import DiffTable
from bot_env.bot_init import ConfigInitializer
from bot_env.decorators import safe_await_alchemy_exe



class MethodDB(ConfigInitializer):
    """
    Создаем асинхронную базу данных:

    Аргументы:
    - logger: Logger
    - config_path: str
        
        # Методы: type: <class 'sqlalchemy.engine.cursor.CursorResult'>
        # fetchone(): Возвращает следующую строку результата запроса.
        # fetchall(): Возвращает все строки результата запроса.
        # fetchmany(size=None): Возвращает заданное количество строк результата запроса (по умолчанию размер указан в параметрах курсора).
        # keys(): Возвращает список имен столбцов результата.
        # close(): Закрывает результат (курсор).

        # Атрибуты:
        # rowcount: Возвращает количество строк, затронутых запросом.
        # description: Список кортежей, представляющих описание столбцов результата. Каждый кортеж содержит информацию о столбце, такую как имя, тип и т.д.
        # closed: Флаг, показывающий, закрыт ли результат.
        
        # Методы объектов <class 'sqlalchemy.engine.row.Row'>:
        # items(): Возвращает пары ключ-значение для каждого столбца в строке.
        # keys(): Возвращает имена столбцов в строке.
        # values(): Возвращает значения столбцов в строке.
        # get(key, default=None): Получение значения по имени столбца. Если столбец не существует, возвращается значение default.
        # as_dict(): Возвращает строки в виде словаря, где ключи - это имена столбцов, а значения - значения столбцов.
        # index(key): Возвращает позицию столбца с указанным именем в строке.
        
        # Атрибуты:
        # keys(): Возвращает имена столбцов в строке.
        # _fields: Атрибут, хранящий имена столбцов в строке.
        # _data: Словарь, содержащий данные строки, где ключи - это имена столбцов, а значения - значения столбцов.

    """
    countInstance=0

    def __init__(self,
                 logger: Logger,
                 config_path: str,
                 ):
        MethodDB.countInstance+=1
        self.countInstance=MethodDB.countInstance
        self.cls_name = self.__class__.__name__
        self.os_abspath = dirname(abspath(__file__))
        self.sys_abspath = path[0]
        # Logger
        self.logger = logger
        # config
        self.config_path = config_path
        self.config = self.read_config(self.config_path)

        
        # Определение структуры таблицы 
        self.metadata = DiffTable.metadata
        
        # Импорт параметров таблицы
        self.name_table = DiffTable.tables
        
        # путь к директории БД
        self.path_db = join(self.sys_abspath, self.config.get('folder_db'))
        
        # абсолютный путь к файлу БД
        # self.full_path_db = join(self.path_db, self.config.get('name_db'))
        self.abspath_filedb = join(self.path_db, self.config.get('name_db'))
        self.check_file([self.abspath_filedb]) 
        
        # создаем url БД
        self.sqlalchemy_url = f'sqlite+aiosqlite:///{self.abspath_filedb}'
        
        # создаем объект engine для связи с БД
        self.engine = create_async_engine(self.sqlalchemy_url)
        
        # Создаем асинхронный объект Session 
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)
        
        self._print()
#

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
            'os_abspath',
            'sys_abspath',
            'config_path',
            'path_db'
            'abspath_filedb'
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
        Создает директорию, 
        если она не существует

         Аргументы:
        - paths: список строк, каждая из которых является путем к директории, которую необходимо создать.
        """
        _ = [makedirs(path,  exist_ok=True) for path in paths]


    def check_file(self, full_paths: list[str], comment: str = None):
        """
        Проверяем наличие файла в директории 

         Аргументы:
        - paths: список строк, каждая из которых является полным путем к файлу, который необходимо проверить.
        """
        notexist_paths = [full_path for full_path in full_paths if not isfile(full_path)]
        if notexist_paths:
            print(f'\n[{__name__}|{self.cls_name}] ERROR: Files do not exist at these paths: {notexist_paths}')
            raise FileNotFoundError(f'\n[{__name__}|{self.cls_name}] Files not found: {notexist_paths}')
        else: 
            for full_path in full_paths: 
                print(f'\n[{self.cls_name}] {comment}: {full_path}')


    # добавляем строку (словарь) в таблицу
    async def insert_data(self, name_table: str, dictions: List[dict]):
        @safe_await_alchemy_exe(logger=self.logger, name_method=f'[{__name__}|{self.cls_name}]')
        # async def _insert_data (name_table: str, dictions: List[dict]):
        async def _insert_data ():
            table=self.name_table.get(name_table)
            async with self.Session() as session:
                for diction in dictions:
                    await session.execute(table.insert().values(**diction))
                await session.commit()
        return await _insert_data()
    
    
    # читаем все данные из таблицы исходя из одного условия 
    async def read_data_one(self, name_table: str, column: str, params_status: str):
        @safe_await_alchemy_exe(self.logger, f'[{__name__}|{self.cls_name}]')
        async def _read_data_one():
            table=self.name_table.get(name_table)
            async with self.Session() as session:
                # Формируем запрос с фильтром
                text_filter=f'{column} = :status'
                result = await session.execute(select(table).
                                            where(text(text_filter)).
                                            params(status=params_status).
                                            order_by(table.c.id))
                return result
        return await _read_data_one()
    #
    # читаем все данные из таблицы исходя из двух условий 
    async def read_data_two(self, name_table: str, one_column: str, one_params: str, two_column: str, two_params: str):
        @safe_await_alchemy_exe(self.logger, f'[{__name__}|{self.cls_name}]')
        async def _read_data_two(): 
            table=self.name_table.get(name_table)
            async with self.Session() as session:
                # Формируем запрос с фильтром
                text_filter=f'{one_column} = :status_one AND {two_column}=:status_two'
                # Сортируем по порядковому номеру (предполагаем, что столбец id уникален)
                async_results = await session.execute(
                                    select(table)
                                    .where(text(text_filter))
                                    .params(status_one=one_params, 
                                            status_two=two_params)
                                    .order_by(table.c.id))   
                return async_results
        return await _read_data_two()
    #
    # Находим в таблице 'dnld', 'task' строки с vid и записываем словарь значений
    async def update_table_vid(self, name_tables: list, vid: str, diction: dict):
        @safe_await_alchemy_exe(self.logger, f'[{__name__}|{self.cls_name}]')
        async def _update_table_vid():
            list_res=[]
            for name_table in name_tables:
                table=self.name_table.get(name_table)
                # Находим все строки с vid и записываем словарь значений
                async with self.Session() as session:
                    stmt = (update(table).
                            where(table.c.video_id == vid).
                            values(diction))
                    await session.execute(stmt)
                    await session.commit()
                    list_res.append(diction)
            return list_res
        return await _update_table_vid()

    # Находим в таблице строки с date_message и user_id
    # записываем словарь значений
    async def update_table_date_userid(self, name_tables: list, date_message: str, user_id: str, diction: dict):
        @safe_await_alchemy_exe(self.logger, f'[{__name__}|{self.cls_name}]')
        async def _update_table_date_userid():
            list_res=[]
            for name_table in name_tables:
                table=self.name_table.get(name_table)
                # Находим все строки с date_message и user_id 
                async with self.Session() as session:
                    stmt = (update(table).
                            where(table.c.date_message == date_message, 
                                  table.c.user_id == user_id).
                            values(diction))               
                    await session.execute(stmt)
                    await session.commit()
                    list_res.append(diction)
            return list_res
        return await _update_table_date_userid()

    # Находим в таблице строки с date_message и chat_id
    # записываем словарь значений
    async def update_table_date_chatid(self, name_tables: list, date_message: str, chat_id: str, diction: dict):
        @safe_await_alchemy_exe(self.logger, f'[{__name__}|{self.cls_name}]')
        async def _update_table_date_chatid():
            list_res=[]
            for name_table in name_tables:
                table=self.name_table.get(name_table)
                # Находим все строки с date_message и chat_id 
                async with self.Session() as session:
                    stmt = (update(table).
                            where(table.c.date_message == date_message, 
                                table.c.chat_id == chat_id).
                            values(diction))               
                    await session.execute(stmt)
                    await session.commit()
                    list_res.append(diction)
            return list_res
        return await _update_table_date_chatid()


    # Находим в таблице 'task' и 'frag' строки с path_frag
    # записываем словарь значений
    async def update_table_path(self, name_tables: list, path_frag: str, diction: dict):
        @safe_await_alchemy_exe(self.logger, f'[{__name__}|{self.cls_name}]')
        async def _update_table_path():
            for name_table in name_tables:
                table=self.name_table.get(name_table)
                # Находим все строки с путем к фрагменту и записываем словарь значений
                async with self.Session() as session: 
                    stmt = (update(table).
                            where(table.c.path_frag == path_frag).
                            values(diction))               
                    await session.execute(stmt)
                    await session.commit()
            return path_frag, diction
        return await _update_table_path()

    # Находим в таблице 'task' и 'frag' строки с 
    # name_frag: z4vMgA7DOyg_610889428620230823132858
    # записываем словарь diction
    async def update_table_resend(self, name_tables: list, name_frag: str, diction: dict):
        @safe_await_alchemy_exe(self.logger, f'[{__name__}|{self.cls_name}]')
        async def _update_table_resend():
            list_res=[]
            for name_table in name_tables:
                table=self.name_table.get(name_table)
                # Находим все строки с путем к фрагменту и записываем словарь значений
                async with self.Session() as session: 
                    stmt = (update(table).
                            where(table.c.name_frag == name_frag).
                            values(diction))               
                    await session.execute(stmt)
                    await session.commit()
                    list_res.append(diction)
            return list_res
        return await _update_table_resend()

    # получаем все данные таблицы
    async def data_table(self, name_table: str):
        """
        Fetches all data from the specified table.
        
        :param name_table: The name of the table from which to fetch data.
        :return: A SQLAlchemy Result object containing the fetched data.
        """
        @safe_await_alchemy_exe(self.logger, f'[{__name__}|{self.cls_name}]')
        async def _data_table():
            table=self.name_table.get(name_table)
            async with self.Session() as session:
                async_results = await session.execute(select(table))
                return async_results
        return await _data_table()
    #
    # Выводим все данные таблицы
    async def print_tables(self, name_table: str):
        """
        Prints all the rows in the specified table.
        
        :param name_table: The name of the table to print.
        """        
        @safe_await_alchemy_exe(self.logger, f'[{__name__}|{self.cls_name}]')
        async def _print_tables():
            async_results = await self.data_table(name_table)
            rows=async_results.fetchall()
            print(f'\n*[print_data] в таблице [{name_table}] записано [{len(rows)}] строк:')
            for row in rows: 
                print(f'\n{row}')

