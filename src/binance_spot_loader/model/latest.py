"""Latest model."""


from datetime import datetime
from typing import List, Tuple

import binance_spot_loader.date_helpers as date_helpers
from binance_spot_loader.model.base import State


class Latest(State):
    symbol: str
    id: int
    open_time: datetime
    source: str

    @classmethod
    def build_record(cls, record: List) -> "Latest":
        res = cls()

        res.symbol = record[0]
        res.id = record[1]
        res.open_time = record[2]
        res.source = record[3]

        return res

    def as_tuple(self) -> Tuple:
        return (self.symbol, self.id, self.open_time, self.source)
