"""
stream_pipeline

Analytics, detection, replay and dataset control plane for the SignalForge
telemetry intelligence platform.

This top-level package re-exports the stable public surface as it is built
out phase by phase. Internal modules should be imported directly from their
submodule path.
"""

from stream_pipeline.config.platform_settings import PlatformSettings

__all__ = [
    "PlatformSettings",
    "__version__",
]

__version__ = "0.1.0"
