"""Base data model."""

from abc import ABC, abstractmethod
from typing import List, Optional, Tuple


class State(ABC):
    """Base state."""

    delivery_id: Optional[int] = None
    event_id: Optional[int] = None

    @classmethod
    @abstractmethod
    def build_record(cls, record: List) -> "State":
        """Creates object from source record."""

    @abstractmethod
    def as_tuple(self) -> Tuple:
        """Returns object values as a tuple."""
