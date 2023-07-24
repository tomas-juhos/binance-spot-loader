"""Queries implementation."""

from .base import BaseQueries
from .latest_spot_1h import Queries as LatestSpot1hQueries
from .spot_1h import Queries as Spot1hQueries

__all__ = ["BaseQueries", "LatestSpot1hQueries", "Spot1hQueries"]
