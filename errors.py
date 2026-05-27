from __future__ import annotations


class ConnectorError(Exception):
    """Base class for connector failures with user-actionable context."""


class ConfigurationError(ConnectorError):
    """Invalid connector configuration."""


class ManifestError(ConnectorError):
    """Invalid discovered source metadata or schema mismatch."""


class SnapshotError(ConnectorError):
    """Invalid or unreadable snapshot input."""


class IncrementalError(ConnectorError):
    """Invalid or unsafe TiCDC incremental input."""


class SchemaChecksumError(IncrementalError):
    """TiCDC schema file checksum mismatch."""


__all__ = [
    "ConfigurationError",
    "ConnectorError",
    "IncrementalError",
    "ManifestError",
    "SchemaChecksumError",
    "SnapshotError",
]
