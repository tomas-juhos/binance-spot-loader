from datetime import datetime, timedelta


def binance_timestamp_to_datetime(timestamp: int) -> datetime:
    timestamp = timestamp / 1000
    # UTC +1
    return datetime.utcfromtimestamp(timestamp) + timedelta(hours=1)


def datetime_to_binance_timestamp(d: datetime):
    d = d - timedelta(hours=1)
    timestamp = int(datetime.timestamp(d) * 1000)
    return timestamp
