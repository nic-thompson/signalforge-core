from __future__ import annotations

import logging
import os

from typing import Any, Dict, Optional

from structured_logging.core.context import ServiceContext
from structured_logging.core.formatter import StructuredJSONFormatter

class StructuredLogger:
    """
    Production structured logger wrapper.

    Provides schema-compliant structured logging across services. 
    """

    def __init__(self, name: str) -> None:
        self.logger = logging.getLogger(name)
        self.configure_logger()

    def configure_logger(self) -> None:
        """
        Configure logging backend.
        """

        if self.logger.handlers:
            return
        
        log_level = getattr(
            logging,
            os.getenv("LOG_LEVEL", "INFO").upper(),
            logging.INFO
        )
    
        handler = logging.StreamHandler()
        handler.setFormatter(StructuredJSONFormatter())

        self.logger.setLevel(log_level)
        self.logger.addHandler(handler)
        self.logger.propagate = False

    def log(
            self, 
            level: int, 
            message: str,
            event_type: str,
            metadata: Dict[str, Any] | None = None,
            trace_id: str | None = None
    ) -> None:
        
        extra = {
            "event_type": event_type,
            "metadata": metadata or {},
            "trace_id": trace_id
        }

        self.logger.log(level, message, extra=extra)

    def info(
            self, 
            message: str,
            event_type: str = "log.info",
            metadata: Dict[str, Any] | None = None,
            trace_id: str | None = None
    ) -> None:
    
        self.log(
            logging.INFO, 
            message, 
            event_type, 
            metadata, 
            trace_id
        )
    
    def warning(
            self, 
            message: str,
            event_type: str = "log.warning",
            metadata: Dict[str, Any] | None = None,
            trace_id: str | None = None
    ) -> None:
    
        self.log(
            logging.WARNING, 
            message, 
            event_type, 
            metadata, 
            trace_id
        )

    def error(
            self, 
            message: str,
            event_type: str = "log.error",
            metadata: Dict[str, Any] | None = None,
            trace_id: str | None = None
    ) -> None:
    
        self.log(
            logging.ERROR, 
            message, 
            event_type, 
            metadata, 
            trace_id
        )

    def debug(
            self, 
            message: str,
            event_type: str = "log.debug",
            metadata: Dict[str, Any] | None = None,
            trace_id: str | None = None
    ) -> None:
    
        self.log(
            logging.DEBUG, 
            message, 
            event_type, 
            metadata, 
            trace_id
        )
        