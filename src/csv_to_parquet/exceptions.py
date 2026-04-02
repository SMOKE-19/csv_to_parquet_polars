class CsvToParquetError(Exception):
    """Base exception for the package."""


class ConfigValidationError(CsvToParquetError):
    """Raised when the Python request is invalid before native execution."""


class ConversionFailedError(CsvToParquetError):
    """Raised when the native converter returns a fatal error."""
