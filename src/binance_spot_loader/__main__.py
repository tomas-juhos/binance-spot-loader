"""Main."""

import logging
from datetime import datetime
from sys import stdout
import os
import time

import binance_spot_loader.date_helpers as date_helpers
from binance_spot_loader.model import Kline, Latest
from binance_spot_loader.persistence import source, target
from binance_spot_loader.queries import Spot1mQueries, LatestSpot1mQueries

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=stdout,
)
logger = logging.getLogger(__name__)


class Loader:

    def __init__(self):
        self.source = source.Source(os.environ.get('SOURCE'))
        self.target = target.Target(os.environ.get('TARGET'))

    def setup(self):
        self.source.connect()
        self.target.connect()

    def get_symbols(self):
        symbol_list = self.target.get_symbols()
        if not symbol_list:
            symbol_list = self.source.get_all_symbols()

        return symbol_list

    def run_once(self, symbol_lst):
        start: datetime = datetime.utcnow()
        end: datetime

        latest = self.target.get_latest()
        if not latest:
            keys = [(s, self.source.get_earliest_valid_timestamp(s)) for s in symbol_lst]
        else:
            # GETTING NEXT MINUTE FOR EVERY SYMBOL
            keys = [(e[0], date_helpers.datetime_to_binance_timestamp(e[1]) + 60_000) for e in latest]

        records = []
        new_latest = []
        for symbol, start_time in keys:
            # BY ADDING 1 MINUTE (TO THE NEXT KLINE) GUARANTEE IT   IS CLOSED
            if start_time + 60_000 > time.time() * 1000:
                logger.info(f'There are no new klines for symbol: {symbol}.')
                continue

            raw_records = self.source.get_klines(
                symbol=symbol,
                start_time=start_time
            )
            for record in raw_records:
                records.append(Kline.build_record([symbol] + record).as_tuple())

            new_latest.append(Latest.build_record([symbol, raw_records[-1][0]]).as_tuple())

        self.target.execute(Spot1mQueries.UPSERT, records)
        self.target.execute(LatestSpot1mQueries.UPSERT, new_latest)
        self.target.commit_transaction()

        end = datetime.utcnow()
        logger.info(
            f'Persisted klines for {len(symbol_lst)} symbols in {end - start}.'
        )

    def run_as_service(self):
        # ON THE FIRST RUN IT GETS SYMBOLS ACCORDING TO FILTERS
        # AFTER THAT IT ONLY UPDATE THOSE SYMBOLS
        logger.info('Fetching symbols...')
        symbol_list = self.get_symbols()
        logger.info('Running...')
        break_process = False
        while not break_process:
            try:
                self.run_once(symbol_list)
            except Exception as e:
                logger.warning("error while importing:", e)
                break_process = True

        logger.info("Terminating...")

    def run(self):
        logger.info('Starting process...')
        if os.environ.get('AS_SERVICE'):
            self.run_as_service()
        else:
            symbol_list = self.get_symbols()
            self.run_once(symbol_list)


loader = Loader()
loader.setup()
print(len(loader.source.get_all_symbols()))


# loader.run()





