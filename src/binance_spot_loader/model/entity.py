"""Entity data model."""

from enum import Enum


class Entity(str, Enum):
    """Type of Entity."""

    SPOT_1M = "SPOT_1M"

    def __repr__(self) -> str:
        return str(self.value)
