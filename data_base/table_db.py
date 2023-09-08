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
    Column("username", String(100)),
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
    Column("dnld", String(200)), # dnlded or None
    Column("in_work", String(100)), # diff or not_diff
    Column("num_kframe_1", String(10)), # 
    Column("num_kframe_2", String(10)), # 
    Column("result_kframe", String(100)), # 0-99 or not_kframe
    Column("result_diff", String(100)), # similar or not_similar
    Column("num_similar_pair", String(100)), # 0-99 or not_similar
    Column("path_sim_img", String(200)),
    Column("sender_user", String(200)), # sender or not_sender
    #
    # Column("timeend_task", Integer), # Получаем текущее время постановки задачи
            ) 
#
# Объединение таблиц в словарь, где ключами будут имена таблиц, 
# а значениями - соответствующие объекты Table
tables = {
    name_table_task: table_task,
            }

