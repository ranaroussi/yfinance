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
from bs4 import BeautifulSoup
import random
import time

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


def _extract_extra_keys_from_stores(data):
    new_keys = [k for k in data.keys() if k not in ["context", "plugins"]]
    new_keys_values = set([data[k] for k in new_keys])

    # Maybe multiple keys have same value - keep one of each
    new_keys_uniq = []
    new_keys_uniq_values = set()
    for k in new_keys:
        v = data[k]
        if not v in new_keys_uniq_values:
            new_keys_uniq.append(k)
            new_keys_uniq_values.add(v)

    return [data[k] for k in new_keys_uniq]


def decrypt_cryptojs_aes_stores(data, keys=None):
    encrypted_stores = data['context']['dispatcher']['stores']

    password = None
    if keys is not None:
        if not isinstance(keys, list):
            raise TypeError("'keys' must be list")
        candidate_passwords = keys
    else:
        candidate_passwords = []

    if "_cs" in data and "_cr" in data:
        _cs = data["_cs"]
        _cr = data["_cr"]
        _cr = b"".join(int.to_bytes(i, length=4, byteorder="big", signed=True) for i in json.loads(_cr)["words"])
        password = hashlib.pbkdf2_hmac("sha1", _cs.encode("utf8"), _cr, 1, dklen=32).hex()

    encrypted_stores = b64decode(encrypted_stores)
    assert encrypted_stores[0:8] == b"Salted__"
    salt = encrypted_stores[8:16]
    encrypted_stores = encrypted_stores[16:]

    def _EVPKDF(password, salt, keySize=32, ivSize=16, iterations=1, hashAlgorithm="md5") -> tuple:
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

    def _decrypt(encrypted_stores, password, key, iv):
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
        return plaintext

    if not password is None:
        try:
            key, iv = _EVPKDF(password, salt, keySize=32, ivSize=16, iterations=1, hashAlgorithm="md5")
        except:
            raise Exception("yfinance failed to decrypt Yahoo data response")
        plaintext = _decrypt(encrypted_stores, password, key, iv)
    else:
        success = False
        for i in range(len(candidate_passwords)):
            # print(f"Trying candiate pw {i+1}/{len(candidate_passwords)}")
            password = candidate_passwords[i]
            try:
                key, iv = _EVPKDF(password, salt, keySize=32, ivSize=16, iterations=1, hashAlgorithm="md5")

                plaintext = _decrypt(encrypted_stores, password, key, iv)

                success = True
                break
            except:
                pass
        if not success:
            raise Exception("yfinance failed to decrypt Yahoo data response")

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

    def get_raw_json(self, url, user_agent_headers=None, params=None, proxy=None, timeout=30):
        response = self.get(url, user_agent_headers=user_agent_headers, params=params, proxy=proxy, timeout=timeout)
        response.raise_for_status()
        return response.json()

    def _get_decryption_keys_from_yahoo_js(self, soup):
        result = None

        key_count = 4
        re_script = soup.find("script", string=re.compile("root.App.main")).text
        re_data = json.loads(re.search("root.App.main\s+=\s+(\{.*\})", re_script).group(1))
        re_data.pop("context", None)
        key_list = list(re_data.keys())
        if re_data.get("plugins"):  # 1) attempt to get last 4 keys after plugins
            ind = key_list.index("plugins")
            if len(key_list) > ind+1:
                sub_keys = key_list[ind+1:]
                if len(sub_keys) == key_count:
                    re_obj = {}
                    missing_val = False
                    for k in sub_keys:
                        if not re_data.get(k):
                            missing_val = True
                            break
                        re_obj.update({k: re_data.get(k)})
                    if not missing_val:
                        result = re_obj

        if not result is None:
            return [''.join(result.values())]

        re_keys = []    # 2) attempt scan main.js file approach to get keys
        prefix = "https://s.yimg.com/uc/finance/dd-site/js/main."
        tags = [tag['src'] for tag in soup.find_all('script') if prefix in tag.get('src', '')]
        for t in tags:
            response_js = self.cache_get(t)
            #
            if response_js.status_code != 200:
                time.sleep(random.randrange(10, 20))
                response_js.close()
            else:
                r_data = response_js.content.decode("utf8")
                re_list = [
                    x.group() for x in re.finditer(r"context.dispatcher.stores=JSON.parse((?:.*?\r?\n?)*)toString", r_data)
                ]
                for rl in re_list:
                    re_sublist = [x.group() for x in re.finditer(r"t\[\"((?:.*?\r?\n?)*)\"\]", rl)]
                    if len(re_sublist) == key_count:
                        re_keys = [sl.replace('t["', '').replace('"]', '') for sl in re_sublist]
                        break
                response_js.close()
            if len(re_keys) == key_count:
                break
        if len(re_keys) > 0:
            re_obj = {}
            missing_val = False
            for k in re_keys:
                if not re_data.get(k):
                    missing_val = True
                    break
                re_obj.update({k: re_data.get(k)})
            if not missing_val:
                return [''.join(re_obj.values())]

        return []

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

        response = self.get(url=ticker_url, proxy=proxy)
        html = response.text

        # The actual json-data for stores is in a javascript assignment in the webpage
        try:
            json_str = html.split('root.App.main =')[1].split(
                '(this)')[0].split(';\n}')[0].strip()
        except IndexError:
            # Fetch failed, probably because Yahoo spam triggered
            return {}

        data = json.loads(json_str)

        # Gather decryption keys:
        soup = BeautifulSoup(response.content, "html.parser")
        keys = self._get_decryption_keys_from_yahoo_js(soup)
        # if len(keys) == 0:
        #     msg = "No decryption keys could be extracted from JS file."
        #     if "requests_cache" in str(type(response)):
        #         msg += " Try flushing your 'requests_cache', probably parsing old JS."
        #     print("WARNING: " + msg + " Falling back to backup decrypt methods.")
        if len(keys) == 0:
            keys = []
            try:
                extra_keys = _extract_extra_keys_from_stores(data)
                keys = [''.join(extra_keys[-4:])]
            except:
                pass
            #
            keys_url = "https://github.com/ranaroussi/yfinance/raw/main/yfinance/scrapers/yahoo-keys.txt"
            response_gh = self.cache_get(keys_url)
            keys += response_gh.text.splitlines()

        # Decrypt!
        stores = decrypt_cryptojs_aes_stores(data, keys)
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
