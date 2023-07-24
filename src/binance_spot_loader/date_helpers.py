from datetime import datetime, timezone
from typing import Dict, Optional


seconds_per_unit: Dict[str, int] = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60,
    }


def binance_timestamp_to_datetime(timestamp: int) -> datetime:
    timestamp = timestamp / 1000
    # UTC
    return datetime.utcfromtimestamp(timestamp)


def datetime_to_binance_timestamp(d: datetime):
    # UTC
    timestamp = int(d.replace(tzinfo=timezone.utc).timestamp() * 1000)
    return timestamp


def interval_to_milliseconds(interval: str) -> Optional[int]:
    """Convert a Binance interval string to milliseconds
    Args:
        interval: Binance interval string, e.g.: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w

    Returns:
        int value of interval in milliseconds
        None if interval prefix is not a decimal integer
        None if interval suffix is not one of m, h, d, w

    """
    try:
        return int(interval[:-1]) * seconds_per_unit[interval[-1]] * 1000
    except (ValueError, KeyError):
        return None


def get_next_interval(interval: str, timestamp: int):
    return timestamp + interval_to_milliseconds(interval)


def check_active(interval: str, d: datetime):
    ts = datetime_to_binance_timestamp(d)
    lag10 = 10 * int(interval[:-1]) * seconds_per_unit[interval[-1]] * 1000
    now = datetime_to_binance_timestamp(datetime.utcnow())
    if now - lag10 > ts:
        return False
    else:
        return True
