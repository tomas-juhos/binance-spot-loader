"""Queries implementation."""

from .latest_spot_1m import Queries as LatestSpot1mQueries
from .spot_1m import Queries as Spot1mQueries

__all__ = [
    "LatestSpot1mQueries",
    "Spot1mQueries"
]
