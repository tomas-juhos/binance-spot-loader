"""Latest Spot 1m queries."""

from binance_spot_loader.queries.base import BaseQueries


class Queries(BaseQueries):
    """Latest Spot 1m queries."""

    UPSERT = (
        "INSERT INTO latest_spot_1m ("
        "   symbol, "
        "   open_time "
        ") VALUES %s "
        "ON CONFLICT (symbol) DO "
        "UPDATE SET "
        "    symbol=EXCLUDED.symbol,"
        "    open_time=EXCLUDED.open_time;"
    )