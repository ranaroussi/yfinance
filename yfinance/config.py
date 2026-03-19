"""Runtime configuration for yfinance."""

import json


class NestedConfig:
    """Provide attribute-style access to a nested configuration section."""

    def __init__(self, name, data):
        self.__dict__["name"] = name
        self.__dict__["data"] = data

    def __getattr__(self, key):
        return self.data.get(key)

    def __setattr__(self, key, value):
        self.data[key] = value

    def __len__(self):
        return len(self.__dict__["data"])

    def __repr__(self):
        return json.dumps(self.data, indent=4)


class ConfigMgr:
    """Manage yfinance global options and section-level defaults."""

    def __init__(self):
        self._initialised = False
        self.options = {}
        self._load_options()

    def _load_options(self):
        self._initialised = True
        self.options = {
            "network": {"proxy": None, "retries": 0},
            "debug": {"hide_exceptions": True, "logging": False},
        }

    def __getattr__(self, key):
        if not self._initialised:
            self._load_options()

        if key not in self.options:
            self.options[key] = {}
        return NestedConfig(key, self.options[key])

    def __contains__(self, key):
        if not self._initialised:
            self._load_options()

        return key in self.options

    def __repr__(self):
        if not self._initialised:
            self._load_options()

        all_options = self.options.copy()
        return json.dumps(all_options, indent=4)


YF_CONFIG = ConfigMgr()


def __getattr__(name):
    if name == "YfConfig":
        return YF_CONFIG
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
