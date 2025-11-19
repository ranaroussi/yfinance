import threading

class SingletonMeta(type):
    """
    Metaclass that creates a Singleton instance.
    """
    _instances = {}
    _lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
            else:
                # Update the existing instance
                if 'hide_exceptions' in kwargs or (args and len(args) > 0):
                    hide_exceptions = kwargs.get('hide_exceptions') if 'hide_exceptions' in kwargs else args[0]
                    cls._instances[cls]._set_hide_exceptions(hide_exceptions)
            return cls._instances[cls]


class YfConfig(metaclass=SingletonMeta):
    def __init__(self, hide_exceptions=True):
        self._hide_exceptions = hide_exceptions

    def _set_hide_exceptions(self, hide_exceptions):
        self._hide_exceptions = hide_exceptions

    @property
    def hide_exceptions(self):
        return self._hide_exceptions
    
