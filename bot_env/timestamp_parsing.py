import re
from datetime import datetime, timedelta, time, date

#from datetime.date import strftime
from bot_env.mod_log import Logger


class ParseTime:
    """
    Аргументы:
    - logger: Logger
    - timestamp: str = None,
    - time_iso8601: str = None
    - time_sec: str = None

    Атрибуты:
    - timestamp: временные метки формата "ЧЧ:ММ:СС-ЧЧ:ММ:СС"
    - timestamp_start: временная метка начала фрагмента "ЧЧ:ММ:СС"
    - timestamp_end: временная метка окончания фрагмента "ЧЧ:ММ:СС"
    - time_iso8601: длительность в формате PT11H35M25S
    - seconds_duration: длительность в секундах
    """
    countInstance=0
    #
    def __init__(self,
                 logger: Logger,
                 timestamp: str = None,
                 time_iso8601: str = None,
                 time_sec: str = None,
                 ):
        ParseTime.countInstance+=1
        self.countInstance=ParseTime.countInstance
        self.Logger = logger
        self._print()
        self.time_iso8601=time_iso8601
        self.time_sec=time_sec
        self.seconds_duration=''
        self.datatime_duration=None
        self.timestamp=timestamp
        self.timestamp_start=''
        self.timestamp_end=''
        self.segment_duration=''
        self.duration_minuts=''
        #
    # выводим № объекта
    def _print(self):
        print(f'[ParseTime] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'[ParseTime] countInstance: [{self.countInstance}]')
        
    # парсим строку формата "ЧЧ:ММ:СС-ЧЧ:ММ:СС"
    def parse_time(self, timestamp: str = None):
        """
        Парсим строку формата "ЧЧ:ММ:СС-ЧЧ:ММ:СС"
        return (timestamp_start, timestamp_end, segment_duration)
        """
        if not self.timestamp and timestamp: 
            self.timestamp=timestamp
        if not self.timestamp and not timestamp: 
            print(f'[parse_time] Надо определить self.timestamp или timestamp \nself.timestamp: {self.timestamp} timestamp: {timestamp}')
            return None
        # Проверяем, соответствует ли строка формату "ЧЧ:ММ:СС-ЧЧ:ММ:СС"
        if re.match(r'^\d{2}:\d{2}:\d{2}-\d{2}:\d{2}:\d{2}$', self.timestamp):
            self.timestamp_start, self.timestamp_end = self.timestamp.split('-')
            # Проверяем, соответствует ли подстрока формату "ЧЧ:ММ:СС"
            if not all(re.match(r'^\d{2}:\d{2}:\d{2}$', time_str) for time_str in [self.timestamp_start, self.timestamp_end]):
                print(f'Формат видеометок введены с ошибкой. \ntimestamp_start: {self.timestamp_start} \ntimestamp_end: {self.timestamp_end} ')
                self.timestamp_start=self.timestamp_end=''
                return None
        
        # Преобразуем строки времени в объекты datetime
        start_time_dt = datetime.strptime(self.timestamp_start, "%H:%M:%S")
        end_time_dt = datetime.strptime(self.timestamp_end, "%H:%M:%S") 
        # Вычисляем разницу между временными метками, duration - тип timedelta
        duration = end_time_dt - start_time_dt
        # Получаем продолжительность в секундах
        self.segment_duration = str(int(duration.total_seconds()))
        #       
        return self.timestamp_start, start_time_dt, self.timestamp_end, end_time_dt, self.segment_duration
    #
    # переводим длительность PT11H35M25S в секунды
    def format_iso8601_sec_dt(self, time_iso8601: str = None):
        """
        Переводим длительность PT11H35M25S в секунды и объекты datetime
        return seconds_duration, datatime_duration
        """
        if not self.time_iso8601 and time_iso8601: 
            self.time_iso8601=time_iso8601
        if not self.time_iso8601 and not time_iso8601: 
            print(f'[format_iso8601_sec_dt] Надо определить self.time_iso8601 или time_iso8601 \nself.time_iso8601: {self.time_iso8601} time_iso8601: {time_iso8601}')
            return None
        h='00'
        m='00'
        s='00'
        hours=0
        minutes=0
        seconds=0
        iso8601=self.time_iso8601[2:] # обрезаем PT
        if 'H' in iso8601: 
            h, iso8601 = iso8601.split('H', 1)
            hours=int(h)
        if 'M' in iso8601: 
            m, iso8601 = iso8601.split('M', 1)
            minutes=int(m)
        if 'S' in iso8601: 
            s, iso8601 = iso8601.split('S', 1)
            seconds=int(s)
        self.seconds_duration=str(seconds+minutes*60+hours*60*60)
        self.datatime_duration=datetime.strptime(f'{h}:{m}:{s}', "%H:%M:%S")
        return self.seconds_duration, self.datatime_duration
        #
    # переводим секунды в ММ:СС
    def format_sec2minuts(self, time_sec: str = None):
        """
        Переводим секунды в ММ:СС
        return seconds_duration
        """
        if not self.time_sec and time_sec: 
            self.time_sec=time_sec
        if not self.time_sec and not time_sec: 
            print(f'[format_sec2minuts] Надо определить self.time_sec или time_sec \nself.time_sec: {self.time_sec} time_sec: {time_sec}')
            return None
        #
        # Преобразуем в объект timedelta
        duration_timedelta = timedelta(seconds=float(self.time_sec))
        # Создаем объект datetime с нулевой датой и временем равным интервалу
        zero_datetime = datetime.min + duration_timedelta
        #
        # Форматируем объект datetime в строку с двумя нулями в начале
        self.duration_minuts = zero_datetime.strftime("%H:%M:%S")        #self.duration_minuts = duration_timedelta.datetime.strftime("%H:%M:%S")
        print(f'[format_sec2minuts] duration_minuts {self.duration_minuts}')
        #
        return self.duration_minuts


