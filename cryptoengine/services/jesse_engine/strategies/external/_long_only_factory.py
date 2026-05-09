"""Factory for long-only strategy variants."""
from __future__ import annotations


def make_long_only(base_class):
    """Return a subclass of base_class with should_short() always returning False."""

    class LongOnlyVariant(base_class):
        def should_short(self) -> bool:
            return False

        def go_short(self):
            pass

        def update_position(self):
            if self.is_short:
                self.liquidate()
                return
            super().update_position()

    LongOnlyVariant.__name__ = f'{base_class.__name__}_LongOnly'
    LongOnlyVariant.__qualname__ = f'{base_class.__name__}_LongOnly'
    return LongOnlyVariant
