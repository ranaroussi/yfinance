"""
桌面端股票监控应用
基于 yfinance 和 PyQt5 开发

功能模块:
- config: 配置管理
- data_provider: 数据获取
- watchlist: 自选股管理
- news_manager: 新闻资讯
- screener: 选股工具
- gui: 图形界面
"""

import sys
import warnings

__version__ = "1.0.0"


def _apply_compatibility_patch():
    """
    应用 Python 版本兼容性补丁
    修复 multitasking 库与 Python 3.8 的兼容性问题
    """
    if sys.version_info >= (3, 9):
        return

    if sys.version_info < (3, 9):
        try:
            from typing import Type, Union
            import types

            original_type = type

            class _TypeMeta(type):
                def __getitem__(cls, item):
                    if not isinstance(item, tuple):
                        item = (item,)
                    return Union[tuple(original_type(x) for x in item)]

            class _Type(metaclass=_TypeMeta):
                pass

            if not hasattr(types, 'GenericAlias'):
                types.GenericAlias = type(list[int])

        except Exception as e:
            warnings.warn(f"兼容性补丁应用失败: {e}")


def _fix_multitasking_typing():
    """
    修复 multitasking 库的类型注解问题
    multitasking 在 Python 3.8 中使用了 type[Thread] 语法
    """
    try:
        import sys
        import importlib.util

        if sys.version_info >= (3, 9):
            return

        from typing import TypeVar, Type, Union, Optional, Any
        from typing_extensions import TypedDict

        multitasking_spec = importlib.util.find_spec("multitasking")
        if multitasking_spec is None:
            return

        import multitasking

        if hasattr(multitasking, 'PoolConfig'):
            return

        try:
            from threading import Thread
            from multiprocessing import Process

            class PoolConfig(TypedDict):
                max_workers: Optional[int]
                engine: Union[Type[Thread], Type[Process]]
                timeout: Optional[float]

            multitasking.PoolConfig = PoolConfig

        except Exception as e:
            warnings.warn(f"修复 multitasking 类型注解失败: {e}")

    except ImportError:
        pass
    except Exception as e:
        warnings.warn(f"兼容性处理失败: {e}")


_apply_compatibility_patch()
_fix_multitasking_typing()

__all__ = [
    'ConfigManager',
    'AppConfig',
    'DataProvider',
    'WatchlistManager',
    'NewsManager',
    'StockScreener',
]


def __getattr__(name):
    """
    延迟导入，避免在模块加载时就导入 yfinance
    这样可以让兼容性补丁有机会生效
    """
    if name == 'ConfigManager':
        from .config import ConfigManager
        return ConfigManager
    elif name == 'AppConfig':
        from .config import AppConfig
        return AppConfig
    elif name == 'DataProvider':
        from .data_provider import DataProvider
        return DataProvider
    elif name == 'WatchlistManager':
        from .watchlist import WatchlistManager
        return WatchlistManager
    elif name == 'NewsManager':
        from .news_manager import NewsManager
        return NewsManager
    elif name == 'StockScreener':
        from .screener import StockScreener
        return StockScreener

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
