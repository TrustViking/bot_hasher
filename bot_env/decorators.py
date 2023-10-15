from functools import wraps
from typing import Callable, Coroutine, Any
from sqlalchemy.exc import SQLAlchemyError
from aiogram.types.error_event import ErrorEvent
from aiogram.exceptions import AiogramError
from aiogram.filters import ExceptionTypeFilter
#
from bot_env.mod_log import Logger

# Асинхронный декоратор для безопасного выполнения методов
def safe_await_execute(logger: Logger, name_method: str = None):
    def decorator(coro_func: Callable[..., Coroutine[Any, Any, Any]]):
        @wraps(coro_func)
        async def wrapper(*args, **kwargs):
            try:
                return await coro_func(*args, **kwargs)
            except Exception as eR:
                print(f'\nERROR[{name_method}] ERROR: {eR}')
                logger.log_info(f'\nERROR[{name_method}] ERROR: {eR}') 
                return None
        return wrapper
    return decorator


# Асинхронный декоратор для безопасного выполнения методов SQLAlchemy
def safe_await_alchemy_exe(logger: Logger, name_method: str = None):
    def decorator(coro_func: Callable[..., Coroutine[Any, Any, Any]]):
        @wraps(coro_func)
        async def wrapper(*args, **kwargs):
            try:
                return await coro_func(*args, **kwargs)
            except SQLAlchemyError as eR:
                print(f'\nERROR[{name_method}] ERROR: {eR}')
                logger.log_info(f'\nERROR[{name_method}] ERROR: {eR}') 
                return None
        return wrapper
    return decorator


# Асинхронный декоратор для безопасного выполнения методов aiogram
def safe_await_aiogram_exe(logger: Logger, name_method: str = None):
    def decorator(coro_func: Callable[..., Coroutine[Any, Any, Any]]):
        @wraps(coro_func)
        async def wrapper(*args, **kwargs):
            try:
                return await coro_func(*args, **kwargs)
            except AiogramError as eR:
                print(f'\nERROR[{name_method}] ERROR: {eR}')
                logger.log_info(f'\nERROR[{name_method}] ERROR: {eR}') 
                return None
        return wrapper
    return decorator



# Синхронный декоратор для безопасного выполнения методов
def safe_execute(logger: Logger, name_method: str = None):
    def decorator(func: Callable[..., Any]):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as eR:
                print(f'\nERROR[{name_method}] ERROR: {eR}')
                logger.log_info(f'\nERROR[{name_method}] ERROR: {eR}') 
                return None
        return wrapper
    return decorator
