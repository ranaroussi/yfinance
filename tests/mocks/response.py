import json as _json


class MockResponse:
    """Minimal requests.Response lookalike for mocking YfData HTTP calls."""

    def __init__(self, json_data=None, text=None, status_code=200, url=""):
        self._json = json_data
        self.status_code = status_code
        self.url = url
        if text is not None:
            self.text = text
        elif json_data is not None:
            self.text = _json.dumps(json_data)
        else:
            self.text = ""

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            try:
                from curl_cffi.requests.exceptions import HTTPError
                raise HTTPError(f"HTTP Error: status {self.status_code}", response=self)
            except ImportError:
                raise Exception(f"HTTP Error: status {self.status_code}")
