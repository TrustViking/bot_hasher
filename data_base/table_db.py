from sqlalchemy import MetaData, Table, Column, Integer, String, Float




class DiffTable:
    metadata = MetaData()
    #
    name_table = 'diff'
    #
    table = Table(
        name_table, 
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
        Column("num_kframe_1", String(10)), # 0-99
        Column("num_kframe_2", String(10)), # 0-99
        Column("hash_factor", Float(15)), # 0-99
        Column("threshold_kframes", Float(15)), # 0-99
        Column("withoutlogo", String(15)), # yes or no
        Column("number_corner", String(15)), # 1, 2, 3, 4, not
        Column("logo_size", String(15)), # 0-300, no

        #
        Column("phash", String(15)), # phash or None
        Column("phash_threshold", Integer), # 
        Column("phash_min_distance", Integer), # 
        Column("phash_max_distance", Integer), # 
        Column("phash_matrix_shape", Integer), # 
        #
        Column("dhash", String(15)), # dhash or None
        Column("dhash_threshold", Integer), # 
        Column("dhash_min_distance", Integer), # 
        Column("dhash_max_distance", Integer), # 
        Column("dhash_matrix_shape", Integer), # 
        #
        Column("wavelet", String(15)), # wavelet or None
        Column("wavelet_threshold", Integer), # 
        Column("wavelet_min_distance", Integer), # 
        Column("wavelet_max_distance", Integer), # 
        Column("wavelet_matrix_shape", Integer), # 
        #
        Column("marrihildreth", String(15)), # marrihildreth or None
        Column("marrihildreth_threshold", Integer), # 
        Column("marrihildreth_min_distance", Integer), # 
        Column("marrihildreth_max_distance", Integer), # 
        Column("marrihildreth_matrix_shape", Integer), # 
        #
        Column("blockhash", String(15)), # blockhash or None
        Column("blockhash_threshold", Integer), # 
        Column("blockhash_min_distance", Integer), # 
        Column("blockhash_max_distance", Integer), # 
        Column("blockhash_matrix_shape", Integer), # 
        #
        Column("result_diff", String(100)), # similar or not_similar
        Column("num_similar_pair", String(100)), # 0-99 or not_similar
        Column("save_sim_img", String(200)), # saved or not_save
        Column("path_sim_img", String(200)), # path or not_path
        Column("sender_user", String(200)), # sender or not_sender
                ) 
    #
    # Объединение таблиц в словарь, где ключами будут имена таблиц, 
    # а значениями - соответствующие объекты Table
    tables = {
        name_table: table,
                }

