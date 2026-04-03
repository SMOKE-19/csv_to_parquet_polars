from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
import csv
import shutil
import time

import polars as pl

from .exceptions import ConfigValidationError, ConversionFailedError
from .models import (
    CastFailure,
    ColumnStats,
    ConvertCsvToParquetRequest,
    ConvertCsvToParquetResult,
    OutputPathOptions,
)

SUPPORTED_DELIMITERS = {",", "\t", "|", ";"}
SUPPORTED_COMPRESSIONS = {"snappy", "zstd", "none"}
SUPPORTED_TYPES = {
    "string",
    "bool",
    "int64",
    "float64",
    "float32",
    "uint16",
    "uint8",
    "date32",
    "timestamp_ms",
}
DEFAULT_NULL_LIKE_VALUES = {"", "null", "nan", "n/a"}
SOURCE_COLUMN_NAME = "__source"
INDEX_A_COLUMN_NAME = "index_A"
INDEX_B_COLUMN_NAME = "index_B"
ROW_NUMBER_COLUMN_NAME = "__row_number__"


def convert_csv_to_parquet(
    request: ConvertCsvToParquetRequest,
) -> ConvertCsvToParquetResult:
    normalized = _normalize_request(request)
    started = time.perf_counter()
    try:
        payload = _convert_with_polars(normalized)
    except Exception as exc:
        if isinstance(exc, ConfigValidationError):
            raise
        raise ConversionFailedError(str(exc)) from exc
    payload["elapsed_ms"] = int((time.perf_counter() - started) * 1000)
    payload["throughput_rows_per_sec"] = (
        payload["total_rows_written"] / (payload["elapsed_ms"] / 1000)
        if payload["elapsed_ms"] > 0
        else float(payload["total_rows_written"])
    )
    return _build_result(payload)


def convert_csv_to_parquet_simple(
    input_csv_path: str | Path,
    output_parquet_dir: str | Path,
    column_type_map: dict[str, str],
    *,
    exclude_columns: list[str] | None = None,
    index_source_column: str,
    output_path_options: OutputPathOptions | None = None,
    delimiter: str | None = None,
    compression: str = "snappy",
    max_rows_per_chunk: int = 32_768,
    max_rows_per_batch: int = 65_536,
    parser_workers: int = 4,
    trim_whitespace: bool = True,
    debug_collect_failures: bool = False,
    cast_failure_limit: int = 1_000,
) -> ConvertCsvToParquetResult:
    request = ConvertCsvToParquetRequest(
        input_csv_path=input_csv_path,
        output_parquet_dir=output_parquet_dir,
        column_type_map=column_type_map,
        exclude_columns=exclude_columns or [],
        index_source_column=index_source_column,
        output_path_options=output_path_options or OutputPathOptions(),
        delimiter=delimiter,
        compression=compression,
        max_rows_per_chunk=max_rows_per_chunk,
        max_rows_per_batch=max_rows_per_batch,
        parser_workers=parser_workers,
        trim_whitespace=trim_whitespace,
        debug_collect_failures=debug_collect_failures,
        cast_failure_limit=cast_failure_limit,
    )
    return convert_csv_to_parquet(request)


def _convert_with_polars(payload: dict) -> dict:
    input_path = Path(payload["input_csv_path"])
    output_root_dir = Path(payload["output_parquet_dir"])
    dataset_dir = output_root_dir / input_path.stem
    effective_delimiter = payload["delimiter"] or _detect_delimiter(input_path)
    normalized_headers = _read_normalized_headers(input_path, effective_delimiter)

    if payload["index_source_column"] not in normalized_headers:
        raise ConfigValidationError(
            f"index_source_column not found: {payload['index_source_column']}"
        )

    effective_exclude = [c for c in payload["exclude_columns"] if c != payload["index_source_column"]]
    output_columns = [SOURCE_COLUMN_NAME, INDEX_A_COLUMN_NAME, INDEX_B_COLUMN_NAME]
    output_columns.extend(
        column_name
        for column_name in normalized_headers
        if column_name not in effective_exclude
    )

    schema_overrides = {name: pl.String for name in normalized_headers}
    batch_iter = pl.scan_csv(
        input_path,
        separator=effective_delimiter,
        has_header=True,
        schema_overrides=schema_overrides,
        new_columns=normalized_headers,
        infer_schema=False,
        truncate_ragged_lines=False,
        row_index_name=ROW_NUMBER_COLUMN_NAME,
        row_index_offset=1,
    ).collect_batches(
        chunk_size=payload["max_rows_per_chunk"],
        maintain_order=True,
    )

    if dataset_dir.exists():
        shutil.rmtree(dataset_dir)
    dataset_dir.mkdir(parents=True, exist_ok=True)

    part_count = 0
    total_rows = 0
    max_batch_size = 0
    cast_failures: list[dict] = []
    cast_failure_total_count = 0
    derived_failure_count = 0

    for batch in batch_iter:
        if batch.height == 0:
            continue
        transformed, batch_debug = _transform_batch(
            batch=batch,
            payload=payload,
            input_path=input_path,
            normalized_headers=normalized_headers,
            effective_exclude=effective_exclude,
            output_columns=output_columns,
            remaining_failure_budget=payload["cast_failure_limit"] - len(cast_failures),
        )

        if payload["debug_collect_failures"]:
            cast_failures.extend(batch_debug["cast_failures"])
            cast_failure_total_count += batch_debug["cast_failure_total_count"]
            derived_failure_count += batch_debug["derived_failure_count"]

        part_path = dataset_dir / f"part-{part_count:05d}.parquet"
        transformed.write_parquet(
            part_path,
            compression=_map_compression(payload["compression"]),
            row_group_size=payload["max_rows_per_batch"],
            mkdir=True,
        )
        part_count += 1
        total_rows += transformed.height
        max_batch_size = max(max_batch_size, transformed.height)

    return {
        "output_parquet_dir": str(output_root_dir),
        "output_dataset_dir": str(dataset_dir),
        "total_rows_read": total_rows,
        "total_rows_written": total_rows,
        "total_batches_built": part_count,
        "total_row_groups_written": part_count,
        "max_batch_size": max_batch_size,
        "elapsed_ms": 0,
        "throughput_rows_per_sec": 0.0,
        "cast_failure_total_count": cast_failure_total_count,
        "cast_failures": cast_failures,
        "column_stats": [],
        "derived_failure_count": derived_failure_count,
        "effective_delimiter": effective_delimiter,
        "effective_exclude_columns": effective_exclude,
        "normalized_columns": output_columns,
    }


def _transform_batch(
    *,
    batch: pl.DataFrame,
    payload: dict,
    input_path: Path,
    normalized_headers: list[str],
    effective_exclude: list[str],
    output_columns: list[str],
    remaining_failure_budget: int,
) -> tuple[pl.DataFrame, dict]:
    df = batch
    if payload["trim_whitespace"]:
        df = df.with_columns(
            [pl.col(name).str.strip_chars().alias(name) for name in normalized_headers]
        )

    df = df.with_columns(
        pl.lit(str(input_path)).alias(SOURCE_COLUMN_NAME),
        pl.when(pl.col(payload["index_source_column"]).str.len_chars() >= 3)
        .then(pl.col(payload["index_source_column"]).str.slice(0, 3))
        .otherwise(None)
        .alias(INDEX_A_COLUMN_NAME),
        pl.when(pl.col(payload["index_source_column"]).str.len_chars() >= 6)
        .then(pl.col(payload["index_source_column"]).str.slice(0, 6))
        .otherwise(None)
        .alias(INDEX_B_COLUMN_NAME),
    )

    transformed_columns: list[pl.Expr] = []
    failure_specs: list[tuple[str, str, pl.Expr, pl.Expr]] = []
    for column_name in normalized_headers:
        if column_name in effective_exclude:
            continue
        target_type = payload["column_type_map"].get(column_name, "float32")
        casted, null_mask, failure_mask = _build_cast_expr(column_name, target_type)
        transformed_columns.append(casted.alias(column_name))
        failure_specs.append((column_name, target_type, null_mask, failure_mask))

    transformed = df.with_columns(transformed_columns).select(output_columns)

    debug_payload = {
        "cast_failure_total_count": 0,
        "derived_failure_count": 0,
        "cast_failures": [],
    }
    if not payload["debug_collect_failures"]:
        return transformed, debug_payload

    index_source = payload["index_source_column"]
    remaining_budget = max(remaining_failure_budget, 0)
    for derived_name, min_len in ((INDEX_A_COLUMN_NAME, 3), (INDEX_B_COLUMN_NAME, 6)):
        too_short = df.filter(pl.col(index_source).str.len_chars() < min_len)
        debug_payload["derived_failure_count"] += too_short.height
        debug_payload["cast_failure_total_count"] += too_short.height
        if remaining_budget <= 0 or too_short.height == 0:
            continue
        sample = too_short.select(ROW_NUMBER_COLUMN_NAME, index_source).head(remaining_budget)
        for row in sample.iter_rows(named=True):
            debug_payload["cast_failures"].append(
                {
                    "row_number": row[ROW_NUMBER_COLUMN_NAME],
                    "column_name": derived_name,
                    "raw_value": row[index_source] or "",
                    "target_type": "string",
                    "reason": "derived_prefix_too_short",
                }
            )
        remaining_budget = payload["cast_failure_limit"] - len(debug_payload["cast_failures"])

    for column_name, target_type, null_mask, failure_mask in failure_specs:
        failed = df.with_columns(
            null_mask.alias("__null_mask__"),
            failure_mask.alias("__failure_mask__"),
        ).filter(pl.col("__failure_mask__"))
        debug_payload["cast_failure_total_count"] += failed.height
        if remaining_budget <= 0 or failed.height == 0:
            continue
        sample = failed.select(ROW_NUMBER_COLUMN_NAME, pl.col(column_name).alias("__raw_value__")).head(
            remaining_budget
        )
        for row in sample.iter_rows(named=True):
            debug_payload["cast_failures"].append(
                {
                    "row_number": row[ROW_NUMBER_COLUMN_NAME],
                    "column_name": column_name,
                    "raw_value": row["__raw_value__"] or "",
                    "target_type": target_type,
                    "reason": "parse_error",
                }
            )
        remaining_budget = payload["cast_failure_limit"] - len(debug_payload["cast_failures"])
        if remaining_budget <= 0:
            break

    return transformed, debug_payload


def _build_cast_expr(column_name: str, target_type: str) -> tuple[pl.Expr, pl.Expr, pl.Expr]:
    source = pl.col(column_name)
    lower = source.str.to_lowercase()
    null_mask = source.is_null() | (source == "") | lower.is_in(list(DEFAULT_NULL_LIKE_VALUES))

    if target_type == "string":
        casted = source
    elif target_type == "bool":
        true_values = ["true", "1", "y", "yes"]
        false_values = ["false", "0", "n", "no"]
        casted = (
            pl.when(lower.is_in(true_values))
            .then(pl.lit(True))
            .when(lower.is_in(false_values))
            .then(pl.lit(False))
            .otherwise(None)
        )
    elif target_type == "int64":
        casted = source.cast(pl.Int64, strict=False)
    elif target_type == "float64":
        casted = source.cast(pl.Float64, strict=False)
    elif target_type == "float32":
        casted = source.cast(pl.Float32, strict=False)
    elif target_type == "uint16":
        casted = source.cast(pl.UInt16, strict=False)
    elif target_type == "uint8":
        casted = source.cast(pl.UInt8, strict=False)
    elif target_type == "date32":
        casted = source.str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    elif target_type == "timestamp_ms":
        casted = source.str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
    else:
        raise ConfigValidationError(f"unsupported type '{target_type}'")

    failure_mask = (~null_mask) & casted.is_null()
    return casted, null_mask, failure_mask


def _map_compression(value: str) -> str:
    return "uncompressed" if value == "none" else value


def _detect_delimiter(path: Path) -> str:
    header_line = path.read_text(encoding="utf-8").splitlines()[0]
    best = None
    for candidate in [",", "\t", "|", ";"]:
        count = header_line.count(candidate)
        if count <= 0:
            continue
        if best is None or count > best[1]:
            best = (candidate, count)
    if best is None:
        raise ConfigValidationError("failed to detect delimiter from header line")
    return best[0]


def _read_normalized_headers(path: Path, delimiter: str) -> list[str]:
    with path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.reader(fp, delimiter=delimiter)
        raw_headers = next(reader)

    seen: dict[str, int] = {}
    normalized: list[str] = []
    for header in raw_headers:
        base = header.strip()
        count = seen.get(base, 0) + 1
        seen[base] = count
        normalized.append(base if count == 1 else f"{base}__{count}")
    return normalized


def _normalize_request(request: ConvertCsvToParquetRequest) -> dict:
    input_path = _normalize_existing_input_path(request.input_csv_path)
    output_dir = _normalize_output_dir(
        request.output_parquet_dir,
        request.output_path_options,
    )
    column_type_map = _normalize_column_type_map(request.column_type_map)
    exclude_columns = _normalize_exclude_columns(
        request.exclude_columns,
        request.index_source_column,
    )
    delimiter = _normalize_delimiter(request.delimiter)

    _require_non_empty("index_source_column", request.index_source_column)
    _require_positive("max_rows_per_chunk", request.max_rows_per_chunk)
    _require_positive("max_rows_per_batch", request.max_rows_per_batch)
    _require_positive("parser_workers", request.parser_workers)
    _require_positive("cast_failure_limit", request.cast_failure_limit)

    if request.compression not in SUPPORTED_COMPRESSIONS:
        raise ConfigValidationError(
            f"compression must be one of {sorted(SUPPORTED_COMPRESSIONS)}"
        )

    return {
        "input_csv_path": str(input_path),
        "output_parquet_dir": str(output_dir),
        "column_type_map": column_type_map,
        "exclude_columns": exclude_columns,
        "index_source_column": request.index_source_column,
        "output_path_options": asdict(request.output_path_options),
        "delimiter": delimiter,
        "compression": request.compression,
        "max_rows_per_chunk": request.max_rows_per_chunk,
        "max_rows_per_batch": request.max_rows_per_batch,
        "parser_workers": request.parser_workers,
        "trim_whitespace": request.trim_whitespace,
        "debug_collect_failures": request.debug_collect_failures,
        "cast_failure_limit": request.cast_failure_limit,
    }


def _normalize_existing_input_path(value: str | Path) -> Path:
    path = Path(value).expanduser().resolve()
    if not path.exists():
        raise ConfigValidationError(f"input_csv_path does not exist: {path}")
    if not path.is_file():
        raise ConfigValidationError(f"input_csv_path is not a file: {path}")
    return path


def _normalize_output_dir(value: str | Path, options: OutputPathOptions) -> Path:
    path = Path(value).expanduser().resolve()
    if options.create_parent_dirs:
        path.mkdir(parents=True, exist_ok=options.exist_ok)
    elif not path.exists():
        raise ConfigValidationError(f"output directory does not exist: {path}")
    elif not path.is_dir():
        raise ConfigValidationError(f"output path is not a directory: {path}")
    return path


def _normalize_column_type_map(column_type_map: dict[str, str]) -> dict[str, str]:
    if not column_type_map:
        raise ConfigValidationError("column_type_map must not be empty")

    normalized: dict[str, str] = {}
    for column_name, type_name in column_type_map.items():
        _require_non_empty("column_type_map key", column_name)
        normalized_type = type_name.strip().lower()
        if normalized_type not in SUPPORTED_TYPES:
            raise ConfigValidationError(
                f"unsupported type '{type_name}' for column '{column_name}'"
            )
        normalized[column_name] = normalized_type
    return normalized


def _normalize_exclude_columns(
    exclude_columns: list[str],
    index_source_column: str,
) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for column in exclude_columns:
        name = column.strip()
        if not name or name == index_source_column or name in seen:
            continue
        seen.add(name)
        deduped.append(name)
    return deduped


def _normalize_delimiter(delimiter: str | None) -> str | None:
    if delimiter is None:
        return None
    if delimiter not in SUPPORTED_DELIMITERS:
        raise ConfigValidationError(
            f"delimiter must be one of {sorted(SUPPORTED_DELIMITERS)}"
        )
    return delimiter


def _require_non_empty(field_name: str, value: str) -> None:
    if not value or not value.strip():
        raise ConfigValidationError(f"{field_name} must not be empty")


def _require_positive(field_name: str, value: int) -> None:
    if value <= 0:
        raise ConfigValidationError(f"{field_name} must be greater than 0")


def _build_result(payload: dict) -> ConvertCsvToParquetResult:
    cast_failures = [
        CastFailure(
            row_number=item["row_number"],
            column_name=item["column_name"],
            raw_value=item["raw_value"],
            target_type=item["target_type"],
            reason=item["reason"],
        )
        for item in payload.get("cast_failures", [])
    ]
    column_stats = [
        ColumnStats(
            column_name=item["column_name"],
            parse_success=item["parse_success"],
            parse_failure=item["parse_failure"],
            null_count=item["null_count"],
        )
        for item in payload.get("column_stats", [])
    ]
    return ConvertCsvToParquetResult(
        output_parquet_dir=payload["output_parquet_dir"],
        output_dataset_dir=payload["output_dataset_dir"],
        total_rows_read=payload["total_rows_read"],
        total_rows_written=payload["total_rows_written"],
        total_batches_built=payload["total_batches_built"],
        total_row_groups_written=payload["total_row_groups_written"],
        max_batch_size=payload["max_batch_size"],
        elapsed_ms=payload["elapsed_ms"],
        throughput_rows_per_sec=payload["throughput_rows_per_sec"],
        cast_failure_total_count=payload["cast_failure_total_count"],
        cast_failures=cast_failures,
        column_stats=column_stats,
        derived_failure_count=payload.get("derived_failure_count", 0),
        effective_delimiter=payload.get("effective_delimiter", ","),
        effective_exclude_columns=payload.get("effective_exclude_columns", []),
        normalized_columns=payload.get("normalized_columns", []),
    )
