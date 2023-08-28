from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime, func
from datetime import datetime

metadata = MetaData()
#
name_table_task = 'task'
table_task = Table(
    name_table_task, 
    metadata,
    Column("id", Integer, primary_key=True),
    Column("date_message", String(50)),
    Column("chat_id", String(100)),
    Column("user_id", String(50)),
    Column("username", String(50)),
    Column("time_task", Integer), # Получаем текущее время постановки задачи
    Column("update_time", Integer),
    # 
    Column("url_video_y2b", String(100)),
    Column("video_id", String(50)),
    Column("channel_title", String(200)),
    Column("video_title", String(200)),
    Column("video_duration", String(50)),
    Column("video_duration_sec", String(50)),
    Column("duration_minuts", String(50)),
    # Column("datatime_duration", DateTime),
    Column("default_audio_language", String(20)),
    Column("segment_duration", String(30)),
    Column("timestamp_start", String(30)),
    # Column("timestamp_start_dt", DateTime),
    Column("timestamp_end", String(30)),
    # Column("timestamp_end_dt", DateTime),
    #
    Column("in_work_download", String(30)), # downloaded or not_download
    Column("path_download", String(200)), # /path/ or not_path
    Column("in_work_frag", String(30)), # fraged or not_frag
    # Column("num_frag", Integer), # num or not_num
    Column("name_frag", String(300)), # z4vMgA7DOyg_6108894... or not_name.frag
    Column("path_frag", String(300)), # /path/z4vMgA7DOyg.mp4 or not_path
    Column("send", String(20)), # sended or not_send
    Column("send2group_file_id", String(300)), # id or not_id
    Column("resend", String(20)), # resended or not_resend
    Column("resend_file_id", String(300)), # id or not_id
            ) 
#
name_table_dnld = 'dnld'
table_download = Table(
    name_table_dnld,
    metadata,
    Column("id", Integer, primary_key=True),
    Column("time_task", Integer), # Получаем текущее время постановки задачи
    Column("update_time", Integer), 
    #
    Column("url_video_y2b", String(200)),
    Column("video_id", String(100)),
    # отметка скачанная или нет ссылка 
    Column("in_work_download", String(30)), # downloaded or not_download
    Column("path_download", String(200)), # /path/ or not_path
            ) 

name_table_frag = 'frag'
table_frag = Table(
    name_table_frag,
    metadata,
    Column("id", Integer, primary_key=True),
    Column("date_message", String(100)),
    Column("chat_id", String(50)),
    Column("user_id", String(50)),
    Column("username", String(50)),
    Column("time_task", Integer), # Получаем текущее время постановки задачи
    #
    Column("url_video_y2b", String(200)),
    Column("video_id", String(100)),
    #
    Column("timestamp_start", String(30)),
    Column("timestamp_end", String(30)),
    #
    Column("in_work_download", String(30)), # downloaded or not_download
    Column("path_download", String(200)), # /path/ or not_path
    Column("in_work_frag", String(30)), # fraged or not_frag
    # Column("num_frag", Integer), # num or not_num
    Column("name_frag", String(300)), # z4vMgA7DOyg_6108894... or not_name.frag
    Column("path_frag", String(300)), # /path/z4vMgA7DOyg.mp4 or not_path
    Column("send", String(20)), # sended or not_send
    Column("send2group_file_id", String(300)), # id or not_id
    Column("resend", String(20)), # resended or not_resend
    Column("resend_file_id", String(300)), # id or not_id
            ) 
# Объединение таблиц в словарь, где ключами будут имена таблиц, 
# а значениями - соответствующие объекты Table
tables = {
    name_table_task: table_task,
    name_table_dnld: table_download,
    name_table_frag: table_frag,

            }

