#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# yfinance - market data downloader
# https://github.com/ranaroussi/yfinance
#
# Copyright 2017-2019 Ran Aroussi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import datetime as _datetime
import pytz as _tz
import requests as _requests
import re as _re
import pandas as _pd
import numpy as _np
import sys as _sys
import os as _os
import appdirs as _ad

from base64 import b64decode
import hashlib
usePycryptodome = False  # slightly faster
# usePycryptodome = True
if usePycryptodome:
    # NOTE: if decide to use 'pycryptodome', set min version to 3.6.6
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import unpad
else:
    from cryptography.hazmat.primitives import padding
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from threading import Lock
mutex = Lock()

try:
    import ujson as _json
except ImportError:
    import json as _json


user_agent_headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}


def is_isin(string):
    return bool(_re.match("^([A-Z]{2})([A-Z0-9]{9})([0-9]{1})$", string))


def get_all_by_isin(isin, proxy=None, session=None):
    if not(is_isin(isin)):
        raise ValueError("Invalid ISIN number")

    from .base import _BASE_URL_
    session = session or _requests
    url = "{}/v1/finance/search?q={}".format(_BASE_URL_, isin)
    data = session.get(url=url, proxies=proxy, headers=user_agent_headers)
    try:
        data = data.json()
        ticker = data.get('quotes', [{}])[0]
        return {
            'ticker': {
                'symbol': ticker['symbol'],
                'shortname': ticker['shortname'],
                'longname': ticker['longname'],
                'type': ticker['quoteType'],
                'exchange': ticker['exchDisp'],
            },
            'news': data.get('news', [])
        }
    except Exception:
        return {}


def get_ticker_by_isin(isin, proxy=None, session=None):
    data = get_all_by_isin(isin, proxy, session)
    return data.get('ticker', {}).get('symbol', '')


def get_info_by_isin(isin, proxy=None, session=None):
    data = get_all_by_isin(isin, proxy, session)
    return data.get('ticker', {})


def get_news_by_isin(isin, proxy=None, session=None):
    data = get_all_by_isin(isin, proxy, session)
    return data.get('news', {})


def empty_df(index=[]):
    empty = _pd.DataFrame(index=index, data={
        'Open': _np.nan, 'High': _np.nan, 'Low': _np.nan,
        'Close': _np.nan, 'Adj Close': _np.nan, 'Volume': _np.nan})
    empty.index.name = 'Date'
    return empty


def empty_earnings_dates_df():
    empty = _pd.DataFrame(
        columns=["Symbol", "Company", "Earnings Date",
                 "EPS Estimate", "Reported EPS", "Surprise(%)"])
    return empty


def get_html(url, proxy=None, session=None):
    session = session or _requests
    html = session.get(url=url, proxies=proxy, headers=user_agent_headers).text
    return html



def decrypt_cryptojs_stores(data):
    """
    Yahoo has started encrypting data stores, this method decrypts it.
    :param data: Python dict of the json data
    :return: The decrypted string data in data['context']['dispatcher']['stores']
    """

    _cs = data["_cs"]
    # Assumes _cr has format like: '{"words":[-449732894,601032952,157396918,2056341829],"sigBytes":16}';
    _cr = _json.loads(data["_cr"])
    _cr = b"".join(int.to_bytes(i, length=4, byteorder="big", signed=True) for i in _cr["words"])

    password = hashlib.pbkdf2_hmac("sha1", _cs.encode("utf8"), _cr, 1, dklen=32).hex()

    encrypted_stores = data['context']['dispatcher']['stores']
    encrypted_stores = b64decode(encrypted_stores)
    assert encrypted_stores[0:8] == b"Salted__"
    salt = encrypted_stores[8:16]
    encrypted_stores = encrypted_stores[16:]

    key, iv = _EVPKDF(password, salt, keySize=32, ivSize=16, iterations=1, hashAlgorithm="md5")

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


def get_json(url, proxy=None, session=None):
    session = session or _requests
    html = session.get(url=url, proxies=proxy, headers=user_agent_headers).text

    if not "root.App.main =" in html:
        return {}

    json_str = html.split('root.App.main =')[1].split(
        '(this)')[0].split(';\n}')[0].strip()
    data = _json.loads(json_str)

    if "_cs" in data and "_cr" in data:
        data_stores = _json.loads(decrypt_cryptojs_stores(data))
    else:
        if "context" in data and "dispatcher" in data["context"]:
            # Keep old code, just in case
            data_stores = data['context']['dispatcher']['stores']
        else:
            data_stores = data

    if not 'QuoteSummaryStore' in data_stores:
        # Problem in data. Either delisted, or Yahoo spam triggered
        return {}

    data = data_stores['QuoteSummaryStore']
    # add data about Shares Outstanding for companies' tickers if they are available
    try:
        data['annualBasicAverageShares'] = \
            data_stores['QuoteTimeSeriesStore']['timeSeries']['annualBasicAverageShares']
    except Exception:
        pass

    # return data
    new_data = _json.dumps(data).replace('{}', 'null')
    new_data = _re.sub(
        r'\{[\'|\"]raw[\'|\"]:(.*?),(.*?)\}', r'\1', new_data)

    return _json.loads(new_data)


def camel2title(o):
    return [_re.sub("([a-z])([A-Z])", r"\g<1> \g<2>", i).title() for i in o]


def _parse_user_dt(dt, exchange_tz):
    if isinstance(dt, int):
        ## Should already be epoch, test with conversion:
        _datetime.datetime.fromtimestamp(dt)
    else:
        # Convert str/date -> datetime, set tzinfo=exchange, get timestamp:
        if isinstance(dt, str):
            dt = _datetime.datetime.strptime(str(dt), '%Y-%m-%d')
        if isinstance(dt, _datetime.date) and not isinstance(dt, _datetime.datetime):
            dt = _datetime.datetime.combine(dt, _datetime.time(0))
        if isinstance(dt, _datetime.datetime) and dt.tzinfo is None:
            # Assume user is referring to exchange's timezone
            dt = _tz.timezone(exchange_tz).localize(dt)
        dt = int(dt.timestamp())
    return dt


def auto_adjust(data):
    df = data.copy()
    ratio = df["Close"] / df["Adj Close"]
    df["Adj Open"] = df["Open"] / ratio
    df["Adj High"] = df["High"] / ratio
    df["Adj Low"] = df["Low"] / ratio

    df.drop(
        ["Open", "High", "Low", "Close"],
        axis=1, inplace=True)

    df.rename(columns={
        "Adj Open": "Open", "Adj High": "High",
        "Adj Low": "Low", "Adj Close": "Close"
    }, inplace=True)

    df = df[["Open", "High", "Low", "Close", "Volume"]]
    return df[["Open", "High", "Low", "Close", "Volume"]]


def back_adjust(data):
    """ back-adjusted data to mimic true historical prices """

    df = data.copy()
    ratio = df["Adj Close"] / df["Close"]
    df["Adj Open"] = df["Open"] * ratio
    df["Adj High"] = df["High"] * ratio
    df["Adj Low"] = df["Low"] * ratio

    df.drop(
        ["Open", "High", "Low", "Adj Close"],
        axis=1, inplace=True)

    df.rename(columns={
        "Adj Open": "Open", "Adj High": "High",
        "Adj Low": "Low"
    }, inplace=True)

    return df[["Open", "High", "Low", "Close", "Volume"]]


def parse_quotes(data):
    timestamps = data["timestamp"]
    ohlc = data["indicators"]["quote"][0]
    volumes = ohlc["volume"]
    opens = ohlc["open"]
    closes = ohlc["close"]
    lows = ohlc["low"]
    highs = ohlc["high"]

    adjclose = closes
    if "adjclose" in data["indicators"]:
        adjclose = data["indicators"]["adjclose"][0]["adjclose"]

    quotes = _pd.DataFrame({"Open": opens,
                            "High": highs,
                            "Low": lows,
                            "Close": closes,
                            "Adj Close": adjclose,
                            "Volume": volumes})

    quotes.index = _pd.to_datetime(timestamps, unit="s")
    quotes.sort_index(inplace=True)

    return quotes


def parse_actions(data):
    dividends = _pd.DataFrame(
        columns=["Dividends"], index=_pd.DatetimeIndex([]))
    splits = _pd.DataFrame(
        columns=["Stock Splits"], index=_pd.DatetimeIndex([]))

    if "events" in data:
        if "dividends" in data["events"]:
            dividends = _pd.DataFrame(
                data=list(data["events"]["dividends"].values()))
            dividends.set_index("date", inplace=True)
            dividends.index = _pd.to_datetime(dividends.index, unit="s")
            dividends.sort_index(inplace=True)

            dividends.columns = ["Dividends"]

        if "splits" in data["events"]:
            splits = _pd.DataFrame(
                data=list(data["events"]["splits"].values()))
            splits.set_index("date", inplace=True)
            splits.index = _pd.to_datetime(splits.index, unit="s")
            splits.sort_index(inplace=True)
            splits["Stock Splits"] = splits["numerator"] / \
                splits["denominator"]
            splits = splits["Stock Splits"]

    return dividends, splits


def fix_Yahoo_dst_issue(df, interval):
    if interval in ["1d","1w","1wk"]:
        # These intervals should start at time 00:00. But for some combinations of date and timezone, 
        # Yahoo has time off by few hours (e.g. Brazil 23:00 around Jan-2022). Suspect DST problem.
        # The clue is (a) minutes=0 and (b) hour near 0. 
        # Obviously Yahoo meant 00:00, so ensure this doesn't affect date conversion:
        f_pre_midnight = (df.index.minute == 0) & (df.index.hour.isin([22,23]))
        dst_error_hours = _np.array([0]*df.shape[0])
        dst_error_hours[f_pre_midnight] = 24-df.index[f_pre_midnight].hour
        df.index += _pd.TimedeltaIndex(dst_error_hours, 'h')
    return df


class ProgressBar:
    def __init__(self, iterations, text='completed'):
        self.text = text
        self.iterations = iterations
        self.prog_bar = '[]'
        self.fill_char = '*'
        self.width = 50
        self.__update_amount(0)
        self.elapsed = 1

    def completed(self):
        if self.elapsed > self.iterations:
            self.elapsed = self.iterations
        self.update_iteration(1)
        print('\r' + str(self), end='')
        _sys.stdout.flush()
        print()

    def animate(self, iteration=None):
        if iteration is None:
            self.elapsed += 1
            iteration = self.elapsed
        else:
            self.elapsed += iteration

        print('\r' + str(self), end='')
        _sys.stdout.flush()
        self.update_iteration()

    def update_iteration(self, val=None):
        val = val if val is not None else self.elapsed / float(self.iterations)
        self.__update_amount(val * 100.0)
        self.prog_bar += '  %s of %s %s' % (
            self.elapsed, self.iterations, self.text)

    def __update_amount(self, new_amount):
        percent_done = int(round((new_amount / 100.0) * 100.0))
        all_full = self.width - 2
        num_hashes = int(round((percent_done / 100.0) * all_full))
        self.prog_bar = '[' + self.fill_char * \
            num_hashes + ' ' * (all_full - num_hashes) + ']'
        pct_place = (len(self.prog_bar) // 2) - len(str(percent_done))
        pct_string = '%d%%' % percent_done
        self.prog_bar = self.prog_bar[0:pct_place] + \
            (pct_string + self.prog_bar[pct_place + len(pct_string):])

    def __str__(self):
        return str(self.prog_bar)


# Simple file cache of ticker->timezone:
_cache_dp = None
def get_cache_dirpath():
    if _cache_dp is None:
        dp = _os.path.join(_ad.user_cache_dir(), "py-yfinance")
    else:
        dp = _os.path.join(_cache_dp, "py-yfinance")
    return dp
def set_tz_cache_location(dp):
    global _cache_dp
    _cache_dp = dp

def cache_lookup_tkr_tz(tkr):
    fp = _os.path.join(get_cache_dirpath(), "tkr-tz.csv")
    if not _os.path.isfile(fp):
        return None

    mutex.acquire()
    df = _pd.read_csv(fp, index_col="Ticker", on_bad_lines="skip")
    mutex.release()
    if tkr in df.index:
        return df.loc[tkr,"Tz"]
    else:
        return None
def cache_store_tkr_tz(tkr,tz):

    dp = get_cache_dirpath()
    fp = _os.path.join(dp, "tkr-tz.csv")
    mutex.acquire()
    if not _os.path.isdir(dp):
        _os.makedirs(dp)
    if (not _os.path.isfile(fp)) and (tz is not None):
        df = _pd.DataFrame({"Tz":[tz]}, index=[tkr])
        df.index.name = "Ticker"
        df.to_csv(fp)

    else:
        df = _pd.read_csv(fp, index_col="Ticker", on_bad_lines="skip")
        if tz is None:
            # Delete if in cache:
            if tkr in df.index:
                df.drop(tkr).to_csv(fp)
        else:
            if tkr in df.index:
                raise Exception("Tkr {} tz already in cache".format(tkr))
            df.loc[tkr,"Tz"] = tz
            df.to_csv(fp)
    
    mutex.release()

