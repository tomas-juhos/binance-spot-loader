"""Latest model."""


from datetime import datetime
from typing import List, Tuple

import binance_spot_loader.date_helpers as date_helpers
from binance_spot_loader.model.base import State


class Latest(State):

    symbol: str
    open_time: datetime

    @classmethod
    def build_record(cls, record: List) -> "Latest":
        res = cls()

        res.symbol = record[0]
        res.open_time = date_helpers.binance_timestamp_to_datetime(record[1])

        return res

    def as_tuple(self) -> Tuple:
        return (
            self.symbol,
            self.open_time
        )
