from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime, func
from datetime import datetime

metadata = MetaData()
#
name_table_task = 'diff'
table_task = Table(
    name_table_task, 
    metadata,
    Column("id", Integer, primary_key=True),
    Column("date_message", String(50)),
    Column("chat_id", String(100)),
    Column("username", String(50)),
    Column("time_task", Integer), # Получаем текущее время постановки задачи
    #
    Column("video_id_first", String(100)),
    Column("width_first", String(100)),
    Column("height_first", String(100)),
    Column("duration_first", String(100)),
    Column("mime_type_first", String(100)),
    Column("file_size_first", String(100)),
    Column("path_file_first", String(200)),
    #
    Column("video_id_second", String(100)),
    Column("width_second", String(100)),
    Column("height_second", String(100)),
    Column("duration_second", String(100)),
    Column("mime_type_second", String(100)),
    Column("file_size_second", String(100)),
    Column("path_file_second", String(200)),
    #
    Column("timeend_task", Integer), # Получаем текущее время постановки задачи
            ) 
#
# Объединение таблиц в словарь, где ключами будут имена таблиц, 
# а значениями - соответствующие объекты Table
tables = {
    name_table_task: table_task,
            }

