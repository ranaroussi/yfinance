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

from frozendict import frozendict

try:
    import ujson as json
except ImportError:
    import json as json

cache_maxsize = 64


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


def decrypt_cryptojs_aes_stores(data):
    encrypted_stores = data['context']['dispatcher']['stores']

    if "_cs" in data and "_cr" in data:
        _cs = data["_cs"]
        _cr = data["_cr"]
        _cr = b"".join(int.to_bytes(i, length=4, byteorder="big", signed=True) for i in json.loads(_cr)["words"])
        password = hashlib.pbkdf2_hmac("sha1", _cs.encode("utf8"), _cr, 1, dklen=32).hex()
    else:
        # Currently assume one extra key in dict, which is password. Print error if 
        # more extra keys detected.
        new_keys = [k for k in data.keys() if k not in ["context", "plugins"]]
        l = len(new_keys)
        if l == 0:
            return None
        elif l == 1 and isinstance(data[new_keys[0]], str):
            password_key = new_keys[0]
        else:
            msg = "Yahoo has again changed data format, yfinance now unsure which key(s) is for decryption:"
            k = new_keys[0]
            k_str = k if len(k) < 32 else k[:32-3]+"..."
            msg += f" '{k_str}'->{type(data[k])}"
            for i in range(1, len(new_keys)):
                msg += f" , '{k_str}'->{type(data[k])}"
            raise Exception(msg)
        password_key = new_keys[0]
        password = data[password_key]

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

    try:
        key, iv = EVPKDF(password, salt, keySize=32, ivSize=16, iterations=1, hashAlgorithm="md5")
    except:
        raise Exception("yfinance failed to decrypt Yahoo data response")

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


class TickerData:
    """
    Have one place to retrieve data from Yahoo API in order to ease caching and speed up operations
    """
    user_agent_headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

    def __init__(self, ticker: str, session=None):
        self.ticker = ticker
        self._session = session or requests

    def get(self, url, user_agent_headers=None, params=None, proxy=None, timeout=30):
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

        html = self.get(url=ticker_url, proxy=proxy).text

        # The actual json-data for stores is in a javascript assignment in the webpage
        try:
            json_str = html.split('root.App.main =')[1].split(
                '(this)')[0].split(';\n}')[0].strip()
        except IndexError:
            # Fetch failed, probably because Yahoo spam triggered
            return {}

        data = json.loads(json_str)

        stores = decrypt_cryptojs_aes_stores(data)
        if stores is None:
            # Maybe Yahoo returned old format, not encrypted
            if "context" in data and "dispatcher" in data["context"]:
                stores = data['context']['dispatcher']['stores']
        if stores is None:
            raise Exception(f"{self.ticker}: Failed to extract data stores from web request")

        # return data
        new_data = json.dumps(stores).replace('{}', 'null')
        new_data = re.sub(
            r'{[\'|\"]raw[\'|\"]:(.*?),(.*?)}', r'\1', new_data)

        return json.loads(new_data)
