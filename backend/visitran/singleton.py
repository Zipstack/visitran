from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from visitran.visitran import VisitranModel


class Singleton(type):
    """VisitranModels base class, If model/class B,C inherits from class A
    which inherits from Singleton class, then both B and C share same class A
    not a different invocation.

    Here as a class storage we are using a weakener dictionary instead of
    normal because, weakener dictionary will clean up the singleton
    references between each invocation when we invoke visitran run
    commands in quick succession like in testcases.
    """

    # Using thread local to avoid memory leak
    _instances = threading.local()

    def __call__(cls: Singleton, *args: tuple[()], **kwargs: dict[str, Any]) -> VisitranModel:
        if not hasattr(cls._instances, "cache"):
            cls._instances.cache = {}

        class_str = str(cls)
        if class_str not in cls._instances.cache:
            # debugpy.breakpoint()
            # This variable declaration is required to force a
            # strong reference on the instance.
            instance: VisitranModel = super().__call__(*args, **kwargs)
            cls._instances.cache[class_str] = instance
        return cls._instances.cache[class_str]

    @classmethod
    def reset_cache(cls) -> None:
        if hasattr(cls._instances, "cache"):
            cls._instances.cache.clear()
