"""Main."""

import logging
from datetime import datetime
from random import randint
from sys import stdout
import os
import time
from typing import Dict, List

import binance_spot_loader.date_helpers as date_helpers
from binance_spot_loader.model import Kline, Latest
from binance_spot_loader.persistence import source, target
from binance_spot_loader.queries import BaseQueries, Spot1hQueries, LatestSpot1hQueries

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL").upper(),
    format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=stdout,
)
logger = logging.getLogger(__name__)


class Loader:
    mode: str

    queries: Dict[str, BaseQueries] = {"1h": Spot1hQueries}

    latest_queries: Dict[str, BaseQueries] = {"1h": LatestSpot1hQueries}

    def __init__(self):
        self.interval = os.environ.get("INTERVAL")
        self.source = source.Source(os.environ.get("SOURCE"), self.interval)
        self.target = target.Target(os.environ.get("TARGET"))
        self.n_requests = 1
        quote_symbols_str = os.environ.get("QUOTE_SYMBOLS")
        self.quote_symbols = dict(
            (symbol, len(symbol)) for symbol in quote_symbols_str.split(sep=",")
        )
        self.source_name = "BINANCE"

    def setup(self):
        self.source.connect()
        self.target.connect()

    def check_request_limit(self):
        self.n_requests += 1
        if self.n_requests >= 1000:
            logger.info("Waiting before requesting more...")
            time.sleep(60)
            self.n_requests = 1

    def run_once(self, symbol_lst):
        start: datetime = datetime.utcnow()
        end: datetime
        n_symbols = len(symbol_lst)
        logger.info(f'Processing {n_symbols} symbols.')

        latest = self.target.get_latest(self.interval)
        if latest:
            new_symbols = set(symbol_lst) - set(k[0] for k in latest)
        else:
            new_symbols = symbol_lst
        if new_symbols:
            logger.info("Fetching earliest timestamps for new symbols...")
            keys = [
                (s, self.source.get_earliest_valid_timestamp(s)) for s in new_symbols
            ]
            self.check_request_limit()
        else:
            keys = [
                (
                    k[0],
                    date_helpers.get_next_interval(
                        self.interval, date_helpers.datetime_to_binance_timestamp(k[1])
                    ),
                )
                for k in latest
            ]

        record_objs: List[Kline] = []
        new_latest: List[Latest] = []
        self.mode = "SLOW"
        i = 1
        for symbol, start_time in keys:
            logger.info(f"Processing {symbol} ({i}/{n_symbols})...")
            i += 1

            raw_records = self.source.get_klines(
                symbol=symbol, interval=self.interval, start_time=start_time
            )
            self.check_request_limit()

            for record in raw_records:
                record_id = self.target.get_next_id(self.interval)
                record_objs.append(Kline.build_record([record_id, symbol] + record))

            if len(record_objs) > 1:
                last_closed_kline = record_objs[-2]
                new_latest.append(
                    Latest.build_record(
                        [
                            symbol,
                            last_closed_kline.id,
                            last_closed_kline.open_time,
                            self.source_name,
                        ]
                    )
                )
        records = [record.as_tuple() for record in record_objs]
        latest_records = [record.as_tuple() for record in new_latest]

        if len(records) != len(symbol_lst):
            self.mode = 'FAST'

        logger.info("Persiting records...")
        self.target.execute(self.queries[self.interval].UPSERT, records)
        self.target.execute(self.latest_queries[self.interval].UPSERT, latest_records)
        self.target.commit_transaction()

        end = datetime.utcnow()
        logger.info(
            f"Persisted klines ({len(records)}) for {len(symbol_lst)} symbols in {end - start}."
        )

    def run_as_service(self):
        # ON THE FIRST RUN IT GETS SYMBOLS ACCORDING TO FILTERS
        # AFTER THAT IT ONLY UPDATE THOSE SYMBOLS
        logger.info("Fetching symbols...")
        symbol_list = self.source.get_symbols(self.quote_symbols)
        logger.info("Running...")
        break_process = False
        while not break_process:
            try:
                self.run_once(symbol_list)
                if self.mode == "FAST":
                    time.sleep(randint(1, 5))
                elif self.mode == "SLOW":
                    interval_sec = int(
                        (date_helpers.interval_to_milliseconds(self.interval) / 1000)
                        / 4
                    )
                    time.sleep(randint(interval_sec, interval_sec + 10))
                else:
                    logger.warning("No waiting mode selected.")
            except Exception as e:
                logger.warning("Error while importing:", e)
                break_process = True

        logger.info("Terminating...")

    def run(self):
        logger.info("Starting process...")
        if os.environ.get("AS_SERVICE"):
            self.run_as_service()
        else:
            symbol_list = self.source.get_symbols(self.quote_symbols)
            self.run_once(symbol_list)


loader = Loader()
loader.setup()
loader.run()
