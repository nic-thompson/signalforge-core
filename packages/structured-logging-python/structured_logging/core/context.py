from __future__ import annotations

from threading import Lock


class ServiceContext:
    """
    Holds environment-level metadata injected into every log record.

    Designed to remain lightweight immutable after initialisation.
    """

    _service_name: str | None = None
    _environment: str | None = None
    _initialised: bool = False
    _lock = Lock()

    @classmethod
    def initialise(
        cls,
        service_name: str,
        environment: str,
    ) -> None:
        """
        Initialise service metadata context.

        Must be called exactly once before logging begins.
        """

        with cls._lock:
            if cls._initialised:
                raise RuntimeError("ServiceContext already initialised")

            if not service_name:
                raise ValueError("service_name must be non-empty")

            if not environment:
                raise ValueError("environment must be non-empty")

            cls._service_name = service_name
            cls._environment = environment
            cls._initialised = True

    @classmethod
    def service_name(cls) -> str:
        if not cls._service_name:
            raise RuntimeError("ServiceContext not initialised")
        return cls._service_name

    @classmethod
    def environment(cls) -> str:
        if not cls._environment:
            raise RuntimeError("ServiceContext not initialised")
        return cls._environment

    @classmethod
    def _reset_for_tests(cls) -> None:
        """
        Reset singleton state between pytest runs.

        Not part of public API.
        """
        with cls._lock:
            cls._service_name = None
            cls._environment = None
            cls._initialised = False