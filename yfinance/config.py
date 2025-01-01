import requests


class Config:
    def __init__(self) -> None:
        self.reset()

    def reset(self):
        self.set_lang("en-US")
        self.set_region("US")
        self.set_session(requests.Session())
        self.set_proxy(None)
        self.set_url("https://fc.yahoo.com")
        self.set_timeout(30)

    @property
    def config(self) -> 'dict':
        return {"lang": self.lang, "region": self.region, "session": self.session, "proxy": self.proxy, "url": self.url, "timeout": self.timeout}
  
    def set_config(self, lang: 'str'=None, region:'str'=None, session: 'requests.Session'=None, proxy: 'str'=None, url: 'str'=None, timeout: 'int'=None) -> 'Config':
        """
        Set the configuration for the yfinance package.

        Args:
            config (dict): The configuration to set.
        """
        if lang is not None:
            self.set_lang(lang)

        if region is not None:
            self.set_region(region)

        if session is not None:
            self.set_session(session)

        if proxy is not None:
            self.set_proxy(proxy)

        if timeout is not None:
            self.set_timeout(timeout)
        
        if url is not None:
            self.set_url(url)
        return self
    
    def from_dict(self,c:'dict'={}, **config) -> 'Config':
        """
        Set the configuration for the yfinance package.

        Args:
            config (dict): The configuration to set.
        """
        config.update(c)
        return self.set_config(**config)
    
    def set_session(self, session: 'requests.Session') -> 'Config':
        """
        Set the session for the yfinance package.

        Args:
            session (requests.Session): The session to be set.
        """
        self.session = session
        return self
    
    def set_proxy(self, proxy: 'str') -> 'Config':
        """
        Set the proxy for the yfinance package.

        Args:
            proxy (str): The proxy to be set.
        """
        self.proxy = proxy
        return self
    
    def set_lang(self, lang: 'str') -> 'Config':
        """
        Set the language for the yfinance package.

        Args:
            lang (str): The language to be set.
        """
        self.lang = lang
        return self
    
    def set_region(self, region: 'str') -> 'Config':
        """
        Set the region for the yfinance package.
        """
        self.region = region
        return self
    
    def set_url(self, url: 'str') -> 'Config':
        """
        Set the url for requests of the yfinance package.
        """
        self.url = url
        return self

    def set_timeout(self, timeout: 'int') -> 'Config':
        """
        Set the timeout for requests of the yfinance package.
        """
        self.timeout = timeout
        return self

class CurrentConfig(Config):
    def set_new_config(self, config:'Config') -> 'CurrentConfig':
        """
        Set the config for the yfinance package.

        Args:
            config (Config): The config to be set.
        """
        self.current = config
        self.from_dict(config.config)
        return self
    
    def get_config(self) -> 'Config':
        """
        Get the config for the yfinance package.

        Returns:
            Config: The config for the yfinance package.
        """
        self.current.from_dict(self.config)
        return self.current
    
Current = CurrentConfig().set_new_config(Config())