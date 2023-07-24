"""Latest Spot queries."""

from binance_spot_loader.queries.base import BaseQueries


class Queries(BaseQueries):
    """Latest Spot queries."""

    UPSERT = (
        "INSERT INTO latest_spot_1h("
        "   symbol, "
        "   id, "
        "   open_time, "
        "   source "
        ") VALUES %s "
        "ON CONFLICT (symbol) DO "
        "UPDATE SET "
        "    symbol=EXCLUDED.symbol, "
        "    open_time=EXCLUDED.open_time, "
        "    source=EXCLUDED.source;"
    )
