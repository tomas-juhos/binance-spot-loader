"""Base data model."""

from abc import ABC, abstractmethod
from typing import List, Optional, Tuple


class BaseModel(ABC):
    """Base entity model."""

    delivery_id: Optional[int] = None
    event_id: Optional[int] = None

    @classmethod
    @abstractmethod
    def build_record(cls, record: List) -> "BaseModel":
        """Creates object from source record."""

    @abstractmethod
    def as_tuple(self) -> Tuple:
        """Returns object values as a tuple."""
