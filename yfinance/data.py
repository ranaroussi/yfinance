import functools
from functools import lru_cache

import hashlib
from base64 import b64decode
usePycryptodome = False  # slightly faster
# usePycryptodome = True
if usePycryptodome:
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import unpad
else:
    from cryptography.hazmat.primitives import padding
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

import requests as requests
import re
import pandas as _pd

from frozendict import frozendict

try:
    import ujson as json
except ImportError:
    import json as json

cache_maxsize = 64

prune_session_cache = True


def lru_cache_freezeargs(func):
    """
    Decorator transforms mutable dictionary and list arguments into immutable types
    Needed so lru_cache can cache method calls what has dict or list arguments.
    """

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        args = tuple([frozendict(arg) if isinstance(arg, dict) else arg for arg in args])
        kwargs = {k: frozendict(v) if isinstance(v, dict) else v for k, v in kwargs.items()}
        args = tuple([tuple(arg) if isinstance(arg, list) else arg for arg in args])
        kwargs = {k: tuple(v) if isinstance(v, list) else v for k, v in kwargs.items()}
        return func(*args, **kwargs)

    # copy over the lru_cache extra methods to this wrapper to be able to access them
    # after this decorator has been applied
    wrapped.cache_info = func.cache_info
    wrapped.cache_clear = func.cache_clear
    return wrapped


def decrypt_cryptojs_aes(data):
    encrypted_stores = data['context']['dispatcher']['stores']
    _cs = data["_cs"]
    _cr = data["_cr"]

    _cr = b"".join(int.to_bytes(i, length=4, byteorder="big", signed=True) for i in json.loads(_cr)["words"])
    password = hashlib.pbkdf2_hmac("sha1", _cs.encode("utf8"), _cr, 1, dklen=32).hex()

    encrypted_stores = b64decode(encrypted_stores)
    assert encrypted_stores[0:8] == b"Salted__"
    salt = encrypted_stores[8:16]
    encrypted_stores = encrypted_stores[16:]

    def EVPKDF(password, salt, keySize=32, ivSize=16, iterations=1, hashAlgorithm="md5") -> tuple:
        """OpenSSL EVP Key Derivation Function
        Args:
            password (Union[str, bytes, bytearray]): Password to generate key from.
            salt (Union[bytes, bytearray]): Salt to use.
            keySize (int, optional): Output key length in bytes. Defaults to 32.
            ivSize (int, optional): Output Initialization Vector (IV) length in bytes. Defaults to 16.
            iterations (int, optional): Number of iterations to perform. Defaults to 1.
            hashAlgorithm (str, optional): Hash algorithm to use for the KDF. Defaults to 'md5'.
        Returns:
            key, iv: Derived key and Initialization Vector (IV) bytes.

        Taken from: https://gist.github.com/rafiibrahim8/0cd0f8c46896cafef6486cb1a50a16d3
        OpenSSL original code: https://github.com/openssl/openssl/blob/master/crypto/evp/evp_key.c#L78
        """

        assert iterations > 0, "Iterations can not be less than 1."

        if isinstance(password, str):
            password = password.encode("utf-8")

        final_length = keySize + ivSize
        key_iv = b""
        block = None

        while len(key_iv) < final_length:
            hasher = hashlib.new(hashAlgorithm)
            if block:
                hasher.update(block)
            hasher.update(password)
            hasher.update(salt)
            block = hasher.digest()
            for _ in range(1, iterations):
                block = hashlib.new(hashAlgorithm, block).digest()
            key_iv += block

        key, iv = key_iv[:keySize], key_iv[keySize:final_length]
        return key, iv

    key, iv = EVPKDF(password, salt, keySize=32, ivSize=16, iterations=1, hashAlgorithm="md5")

    if usePycryptodome:
        cipher = AES.new(key, AES.MODE_CBC, iv=iv)
        plaintext = cipher.decrypt(encrypted_stores)
        plaintext = unpad(plaintext, 16, style="pkcs7")
    else:
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
        decryptor = cipher.decryptor()
        plaintext = decryptor.update(encrypted_stores) + decryptor.finalize()
        unpadder = padding.PKCS7(128).unpadder()
        plaintext = unpadder.update(plaintext) + unpadder.finalize()
        plaintext = plaintext.decode("utf-8")

    decoded_stores = json.loads(plaintext)
    return decoded_stores


_SCRAPE_URL_ = 'https://finance.yahoo.com/quote'


def enable_prune_session_cache():
    global prune_session_cache
    prune_session_cache = True
def disable_prune_session_cache():
    global prune_session_cache
    prune_session_cache = False
def check_Yahoo_response(r, *args, **kwargs):
    # Parse the data returned by Yahoo to determine if corrupt/incomplete.
    # If bad, set 'status_code' to 204 "No content" , that stops it 
    # entering a requests_cache.

    # Because this involves parsing, the output is added to response object 
    # with prefix "yf_" and reused elsewhere.

    if not "yahoo.com/" in r.url:
        # Only check Yahoo responses
        return

    attrs = dir(r)
    r_from_cache = "from_cache" in attrs and r.from_cache
    if "yf_data" in attrs or "yf_json" in attrs or "yf_html_pd" in attrs:
        # Have already parsed this response, successfully
        return

    if "Will be right back" in r.text:
        # Simple check, no parsing needed
        r.status_code = 204
        return r

    parse_failed = False
    r_modified = False

    if "/ws/fundamentals-timeseries" in r.url:
        # Timeseries
        try:
            data = r.json()
            r.yf_json = data
            r_modified = True
            data["timeseries"]["result"]
        except:
            parse_failed = True
    elif "/finance/chart/" in r.url:
        # Prices
        try:
            data = r.json()
            r.yf_json = data
            r_modified = True
            if data["chart"]["error"] is not None:
                parse_failed = True
        except Exception:
            parse_failed = True
    elif "/finance/options/" in r.url:
        # Options
        if not "expirationDates" in r.text:
            # Parse will fail
            parse_failed = True
    elif "/finance/search?" in r.url:
        # News, can't be bothered to check
        return
    elif "/calendar/earnings?" in r.url:
        try:
            dfs = _pd.read_html(r.text)
        except Exception:
            parse_failed = True
        else:
            r.yf_html_pd = dfs
            r_modified = True
    elif "root.App.main" in r.text:
        # JSON data stores
        try:
            json_str = r.text.split('root.App.main =')[1].split(
                '(this)')[0].split(';\n}')[0].strip()
        except IndexError:
            parse_failed = True

        if not parse_failed:
            data = json.loads(json_str)
            if "_cs" in data and "_cr" in data:
                data = decrypt_cryptojs_aes(data)
            if "context" in data and "dispatcher" in data["context"]:
                # Keep old code, just in case
                data = data['context']['dispatcher']['stores']

            if not "yf_data" in attrs:
                r.yf_data = data
                r_modified = True

            if "QuoteSummaryStore" not in data:
                parse_failed = True

    else:
        return

    if parse_failed:
        if not r_from_cache:
            r.status_code = 204  # No content
            r_modified = True

    if r_modified:
        return r


class TickerData:
    """
    Have one place to retrieve data from Yahoo API in order to ease caching and speed up operations
    """
    user_agent_headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

    def __init__(self, ticker: str, session=None):
        self.ticker = ticker
        self._session = session or requests

    def check_requests_cache_hook(self):
        try:
            c = self._session.cache
        except AttributeError:
            # Not a caching session
            return
        global prune_session_cache
        if not prune_session_cache:
            self._session.hooks["response"] = []
        elif prune_session_cache and not check_Yahoo_response in self._session.hooks["response"]:
            self._session.hooks["response"].append(check_Yahoo_response)

    def get(self, url, user_agent_headers=None, params=None, proxy=None, timeout=30):
        self.check_requests_cache_hook()

        proxy = self._get_proxy(proxy)
        response = self._session.get(
            url=url,
            params=params,
            proxies=proxy,
            timeout=timeout,
            headers=user_agent_headers or self.user_agent_headers)
        return response

    @lru_cache_freezeargs
    @lru_cache(maxsize=cache_maxsize)
    def cache_get(self, url, user_agent_headers=None, params=None, proxy=None, timeout=30):
        return self.get(url, user_agent_headers, params, proxy, timeout)

    def _get_proxy(self, proxy):
        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}
        return proxy

    @lru_cache_freezeargs
    @lru_cache(maxsize=cache_maxsize)
    def get_json_data_stores(self, sub_page: str = None, proxy=None) -> dict:
        '''
        get_json_data_stores returns a python dictionary of the data stores in yahoo finance web page.
        '''
        if sub_page:
            ticker_url = "{}/{}/{}".format(_SCRAPE_URL_, self.ticker, sub_page)
        else:
            ticker_url = "{}/{}".format(_SCRAPE_URL_, self.ticker)

        self.check_requests_cache_hook()

        response = self.get(url=ticker_url, proxy=proxy)

        if "yf_data" in dir(response):
            data = response.yf_data
        else:
            html = response.text

            # The actual json-data for stores is in a javascript assignment in the webpage
            try:
                json_str = html.split('root.App.main =')[1].split(
                    '(this)')[0].split(';\n}')[0].strip()
            except IndexError:
                # Problem with data so clear from session cache
                # self.session_cache_prune_url(ticker_url)
                # Then exit
                return {}

            data = json.loads(json_str)

            if "_cs" in data and "_cr" in data:
                data = decrypt_cryptojs_aes(data)

            if "context" in data and "dispatcher" in data["context"]:
                # Keep old code, just in case
                data = data['context']['dispatcher']['stores']

        # return data
        new_data = json.dumps(data).replace('{}', 'null')
        new_data = re.sub(
            r'{[\'|\"]raw[\'|\"]:(.*?),(.*?)}', r'\1', new_data)

        json_data = json.loads(new_data)

        return json_data
