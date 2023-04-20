"""Source."""

from decimal import Decimal
import hmac
import hashlib
import logging
import time
from typing import Dict, List
from sys import stdout
import os

import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=stdout,
)
logger = logging.getLogger(__name__)


class Source:
    """Source class."""

    _api_url = "https://api.binance.com/api/"
    _version = "v3/"
    base_url = _api_url + _version

    _headers: Dict[str, str]
    _session: requests.Session

    mkt_cap_filter: int = 5_000_000

    def __init__(self, connection_string: str):
        credentials = dict(kv.split('=') for kv in connection_string.split(' '))

        self._api_key = credentials['API_KEY']
        self._secret_key = credentials['SECRET_KEY']

    def connect(self) -> None:
        self._session = requests.Session()
        self._headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
            # noqa
            "X-MBX-APIKEY": self._api_key,
        }
        self._session.headers.update(self._headers)

        self.ping()

    def ping(self):
        url = f'{self.base_url}ping'
        response = self._session.get(url)

        if response.status_code == 200:
            logger.info("Connected to the Binance API.")
        else:
            logger.info(f"Connection failed with status code {response.status_code}")

    def get_all_symbols(self):
        url = f'{self.base_url}exchangeInfo'
        response = self._session.get(url)

        if response.status_code == 200:
            symbols = [
                symbol["symbol"]
                for symbol in response.json()["symbols"]
                if symbol["symbol"][-4:] == "USDT"
                or symbol["symbol"][-4:] == "BUSD"
                # or symbol["symbol"][-3:] == "BTC"
                # or symbol["symbol"][-3:] == "ETH"
                # or symbol["symbol"][-3:] == "BNB"
            ]
            return symbols
        else:
            logger.warning(f"Request failed with status code {response.status_code}")
            return

    def get_daily_volume(self, symbol):
        """Returns the daily volume of a symbol in USDT."""
        if symbol[-4:] == "USDT":
            quote_currency = "USDT"
        elif symbol[-4:] == "BUSD":
            quote_currency = "BUSD"
        elif symbol[-3:] == "BTC":
            quote_currency = "BTC"
        elif symbol[-3:] == "ETH":
            quote_currency = "ETH"
        elif symbol[-3:] == "BNB":
            quote_currency = "BNB"
        else:
            logger.warning('Not valid symbol provided.')
            return

        # Get the trading pair's daily volume
        volume_url = f'{self.base_url}ticker/24hr?symbol={symbol}'
        response = self._session.get(volume_url)
        if response.status_code == requests.codes.ok:
            daily_volume = Decimal(response.json()['quoteVolume'])
        else:
            logger.warning(f"Request failed with status code {response.status_code}")
            return

        if quote_currency == 'USDT':
            return daily_volume
        else:
            # Get the current price of the quote currency
            ticker_url = f'{self.base_url}ticker/price?symbol={quote_currency}USDT'
            response = self._session.get(ticker_url)
            if response.status_code == requests.codes.ok:
                quote_price = Decimal(response.json()['price'])
            else:
                logger.warning(f"Request failed with status code {response.status_code}")
                return

            # Calculate the daily volume in dollars
            daily_volume_usd = daily_volume * quote_price

            return daily_volume_usd

    def filter_symbols(self, symbol_lst: List[str], mkt_cap: int = mkt_cap_filter):
        res = []
        i = 0
        n = len(symbol_lst)
        for symbol in symbol_lst:
            if i % 100 == 0:
                logger.info(f'Filtered {i}/{n} symbols...')
            if self.get_daily_volume(symbol) >= mkt_cap:
                res.append(symbol)
            i += 1
        logger.info(f'Filtered {n}/{n} symbols.')
        return res

    def get_symbols(self):
        res = self.filter_symbols(self.get_all_symbols())
        return res

    def get_klines(self, symbol: str, start_time: int = None, end_time: int = None, limit: int = 1000):
        url = f'{self.base_url}klines'
        if start_time is not None and end_time is not None:
            params = {
                'symbol': symbol,
                'interval': '1m',
                'startTime': start_time,
                'endTime': end_time,
                'limit': limit
            }
        elif start_time is not None:
            params = {
                'symbol': symbol,
                'interval': '1m',
                'startTime': start_time,
                'limit': limit
            }
        else:
            params = {
                'symbol': symbol,
                'interval': '1m',
                'limit': limit
            }

        timestamp = str(int(time.time() * 1000))
        query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
        signature = hmac.new(self._secret_key.encode('utf-8'), f"{query_string}&timestamp={timestamp}".encode('utf-8'),
                             hashlib.sha256).hexdigest()
        self._headers['X-MBX-TIMESTAMP'] = timestamp
        self._headers['X-MBX-SIGNATURE'] = signature
        self._session.headers.update(self._headers)

        response = self._session.get(url, params=params)

        if response.status_code == 200:
            # Print the response data
            data = response.json()
            return data
        else:
            # Print the error message
            logger.warning(f"Request failed with status code {response.status_code}")
            return

    def get_earliest_valid_timestamp(self, symbol):
        kline = self.get_klines(
            symbol=symbol,
            start_time=0,
            end_time=int(time.time() * 1000),
            limit=1
        )
        return kline[0][0]
