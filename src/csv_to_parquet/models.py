from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import polars as pl

CompressionName = Literal["snappy", "zstd", "none"]
PolarsType = pl.DataType


@dataclass(slots=True)
class OutputPathOptions:
    create_parent_dirs: bool = True
    exist_ok: bool = True


@dataclass(slots=True)
class ConvertCsvToParquetRequest:
    input_csv_path: str | Path
    output_parquet_dir: str | Path
    column_type_map: dict[str, PolarsType]
    exclude_columns: list[str]
    index_source_column: str
    output_path_options: OutputPathOptions = field(default_factory=OutputPathOptions)
    delimiter: str | None = None
    compression: CompressionName = "snappy"
    max_rows_per_chunk: int = 32_768
    max_rows_per_batch: int = 65_536
    parser_workers: int = 4
    trim_whitespace: bool = True
    debug_collect_failures: bool = False
    cast_failure_limit: int = 1_000


@dataclass(slots=True)
class CastFailure:
    row_number: int
    column_name: str
    raw_value: str
    target_type: str
    reason: str


@dataclass(slots=True)
class ColumnStats:
    column_name: str
    parse_success: int
    parse_failure: int
    null_count: int


@dataclass(slots=True)
class ConvertCsvToParquetResult:
    output_parquet_dir: str
    output_dataset_dir: str
    total_rows_read: int
    total_rows_written: int
    total_batches_built: int
    total_row_groups_written: int
    max_batch_size: int
    elapsed_ms: int
    throughput_rows_per_sec: float
    cast_failure_total_count: int
    cast_failures: list[CastFailure] = field(default_factory=list)
    column_stats: list[ColumnStats] = field(default_factory=list)
    derived_failure_count: int = 0
    effective_delimiter: str = ","
    effective_exclude_columns: list[str] = field(default_factory=list)
    normalized_columns: list[str] = field(default_factory=list)
