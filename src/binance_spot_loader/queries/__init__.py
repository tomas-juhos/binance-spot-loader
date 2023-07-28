"""Queries implementation."""

from .base import BaseQueries, BaseQueriesLatest
from .spot_1h import Queries as Spot1hQueries
from .spot_1h_latest import Queries as Spot1hLatestQueries


__all__ = [
    "BaseQueries",
    "BaseQueriesLatest",
    "Spot1hQueries",
    "Spot1hLatestQueries",
]
