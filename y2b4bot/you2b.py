import os, re
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from bot_env.mod_log import Logger

class You2b:
    """
    Проверяем ссылку youtube:
    
    Аргументы:
    - url: str 
    - logger: Logger
    """
    countInstance=0

    def __init__(self, url: str, logger: Logger):
        You2b.countInstance+=1
        self.countInstance=You2b.countInstance
        self.url=url
        self.Logger=logger
        self._print()
        #
        self.response=None
        self.video_info = ''
        self.video_url = ''
        self.channel_title = '' 
        self.video_title = ''
        self.default_audio_language = ''
        self.duration_iso8601 = ''
        #
        self.api_key=os.getenv('Y2B_API_KEY')
        #self._print_API_KEY() # если надо вывести, то раскомментировать
        #
        self.video_id=self._extract_video_id()
        self._check_youtube_link()
        # self._print_fields() # если надо вывести поля, то раскомментировать
        #
    #
    # выводим № объекта
    def _print(self):
        print(f'[You2b] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'[You2b] countInstance: [{self.countInstance}]')
#
    # проверка API_KEY
    def _print_API_KEY(self):
        print(f'[_print_API_KEY] API_KEY: {self.api_key}')
        self.Logger.log_info(f'[_print_API_KEY] API_KEY: {self.api_key}')
        #
        print(f'[_print_API_KEY] url: {self.url}')
        self.Logger.log_info(f'[_print_API_KEY] url: {self.url}')
    #
    # поиск идентификатора видео
    def _extract_video_id(self):
        # Паттерн для поиска идентификатора видео
        pattern = r'(?:youtu\.be\/|youtube\.com\/(?:embed\/|v\/|watch\?v=|watch\?.+&v=))([^&]{11})'
        # Ищем совпадения с паттерном в ссылке
        match = re.search(pattern, self.url)
        video_id=None
        if match:
            # Получаем найденный video_id, match.group(1): m0ZRms4p7fc, type(video_id): <class 'str'>
            video_id = match.group(1)  
            print(f'[extract_video_id] video_id: {video_id}')
            self.Logger.log_info(f'[extract_video_id] video_id: {video_id}\ntype(video_id): {type(video_id)}')
            return video_id # Если video_id найден
        #
        print(f'[extract_video_id] video_id: {video_id}')
        self.Logger.log_info(f'[extract_video_id] video_id: {video_id}')
        return None  # Если video_id не найден
    # 
    # проверка ссылки ютуб
    def _check_youtube_link(self):
        """
        # При запросе информации о видео с использованием YouTube API v3 
        # вы можете указать следующие части (part) для получения 
        # различных типов данных:
        # snippet: Информация о видео, включая заголовок, описание, канал, теги и т.д.
        # contentDetails: Детали контента видео, такие как длительность, разрешение, формат и т.д.
        # statistics: Статистика видео, включая количество просмотров, лайков, дизлайков и комментариев.
        # status: Статус видео, такой как его приватность, доступность и т.д.
        # topicDetails: Детали тематики видео, такие как связанные темы и региональные ограничения.
        # recordingDetails: Детали записи видео, такие как дата и место съемки.
        # fileDetails: Детали файла видео, такие как тип кодека и битрейт.
        # liveStreamingDetails: Детали прямых трансляций видео, такие как расписание и состояние трансляции.
        # player: Параметры плеера, такие как встроенные видеоигроки и параметры воспроизведения.
        """
        # проверяем наличие идентификатора видео
        if not self.video_id:
            print(f'[check_youtube_link] video_id: [{self.video_id}] - return None')
            self.Logger.log_info(f'[check_youtube_link] video_id: [{self.video_id}] - return None')
            return None
        # запрос информации с YouTube API
        youtube = build('youtube', 'v3', developerKey=self.api_key)
        try:
            self.response = youtube.videos().list(part='snippet, contentDetails', id=self.video_id).execute()
        except HttpError as eR:
            self.Logger.log_info(f'[check_youtube_link] error: {eR}')   
            return None
        #
        if not self.response['items']: 
            print(f'[check_youtube_link] link: [{self.url}] response["items"] is None')
            self.Logger.log_info(f'[check_youtube_link] link: [{self.url}] response["items"] is None')
            return None
        #
        # Извлечение данных
        item=self.response.get('items', None)
        video_id=item[0].get('id', None)
        # проверяем video_id
        if video_id != self.video_id: 
            print(f'item[0].get("id") {video_id} != {self.video_id} self.video_id')
            return None
        self.video_info=item[0].get('snippet', None)
        print(f'\nvideo_info: {self.video_info}\n')
        self.video_url = f'https://www.youtube.com/watch?v={self.video_id}'
        self.channel_title = self.video_info.get('channelTitle', None)
        self.video_title = self.video_info.get('title', None)
        self.default_audio_language = self.video_info.get('defaultAudioLanguage', None)
        contentDetails=item[0].get('contentDetails', None)
        if contentDetails:
            self.duration_iso8601 = contentDetails.get('duration', None)
    # вывод полей
    def _print_fields(self):
        #print(f'[_print_fields] video_info: {str(self.video_info)} \ndescription: {str(self.description)}')
        print(f'[_print_fields] video_url: {str(self.video_url)} ')
        print(f'[_print_fields] channel_title: {str(self.channel_title)}')
        print(f'[_print_fields] video_title: {str(self.video_title)} ')
        print(f'[_print_fields] default_audio_language: {str(self.default_audio_language)}')
        print(f'[_print_fields] duration_iso8601: {str(self.duration_iso8601)}')

