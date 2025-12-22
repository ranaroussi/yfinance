import json


class NestedConfig:
    def __init__(self, name, data):
        self.__dict__['name'] = name
        self.__dict__['data'] = data

    def __getattr__(self, key):
        return self.data.get(key)

    def __setattr__(self, key, value):
        self.data[key] = value

    def __len__(self):
        return len(self.__dict__['data'])

    def __repr__(self):
        return json.dumps(self.data, indent=4)

class ConfigMgr:
    def __init__(self):
        self._initialised = False

    def _load_option(self):
        self._initialised = True  # prevent infinite loop
        self.options = {}

        # Initialise defaults
        n = self.__getattr__('network')
        n.proxy = None
        n.retries = 0
        d = self.__getattr__('debug')
        d.hide_exceptions = True
        d.logging = False

    def __getattr__(self, key):
        if not self._initialised:
            self._load_option()

        if key not in self.options:
            self.options[key] = {}
        return NestedConfig(key, self.options[key])

    def __contains__(self, key):
        if not self._initialised:
            self._load_option()

        return key in self.options

    def __repr__(self):
        if not self._initialised:
            self._load_option()

        all_options = self.options.copy()
        return json.dumps(all_options, indent=4)

YfConfig = ConfigMgr()
