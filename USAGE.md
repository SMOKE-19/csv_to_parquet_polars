# `convert_csv_to_parquet_simple()` Usage

이 문서는 외부 사용자가 `csv_to_parquet` 패키지의 공개 진입점인 `convert_csv_to_parquet_simple()` 함수를 사용하는 방법을 설명한다.

이 프로젝트는 Rust extension이 아니라 Python `polars` 기반 구현이며, 출력은 단일 Parquet 파일이 아니라 dataset 디렉토리다.

```bash
python -m pip install polars
```

## 1. 기본 사용 예시

```python
from csv_to_parquet import convert_csv_to_parquet_simple

result = convert_csv_to_parquet_simple(
    input_csv_path="/data/input.csv",
    output_parquet_dir="/data/out",
    column_type_map={
        "code": "string",
        "amount": "float32",
        "qty": "uint16",
        "created_at": "timestamp_ms",
    },
    exclude_columns=["debug_col", "tmp_col"],
    index_source_column="code",
)

print(result.output_parquet_dir)
print(result.output_dataset_dir)
print(result.total_rows_written)
```

예를 들어 입력이 `/data/input.csv`면 결과는 대략 아래처럼 저장된다.

- `/data/out/input/part-00000.parquet`
- `/data/out/input/part-00001.parquet`

## 2. 함수 시그니처

```python
convert_csv_to_parquet_simple(
    input_csv_path,
    output_parquet_dir,
    column_type_map,
    *,
    exclude_columns=None,
    index_source_column,
    output_path_options=None,
    delimiter=None,
    compression="snappy",
    max_rows_per_chunk=32768,
    max_rows_per_batch=65536,
    parser_workers=4,
    trim_whitespace=True,
    debug_collect_failures=False,
    cast_failure_limit=1000,
)
```

## 3. 주요 인자 설명

- `input_csv_path`
  - 입력 CSV 파일 경로
- `output_parquet_dir`
  - 결과 dataset를 저장할 루트 디렉토리
  - 실제 출력은 `<output_parquet_dir>/<csv파일명>/part-xxxxx.parquet`
- `column_type_map`
  - 컬럼별 타입 지정 dict
  - 지정되지 않은 컬럼은 `float32`로 처리된다
- `exclude_columns`
  - Parquet 출력에서 제외할 컬럼 목록
- `index_source_column`
  - `index_A`, `index_B` 생성 기준 컬럼
- `output_path_options`
  - 출력 루트 디렉터리 생성 옵션
- `delimiter`
  - 직접 지정 시 `,`, `\t`, `|`, `;` 중 하나만 가능
  - `None`이면 자동 판별
- `compression`
  - `"snappy"`, `"zstd"`, `"none"` 중 하나
- `debug_collect_failures`
  - `True`일 때만 cast failure 샘플을 수집한다
- `cast_failure_limit`
  - 디버그 모드에서 반환할 cast failure 샘플 최대 개수

## 4. 출력 디렉터리 자동 생성

기본적으로 출력 루트 디렉터리는 자동 생성된다.

```python
from csv_to_parquet import convert_csv_to_parquet_simple, OutputPathOptions

result = convert_csv_to_parquet_simple(
    input_csv_path="/data/input.csv",
    output_parquet_dir="/data/new_dir",
    column_type_map={"code": "string", "amount": "float32"},
    index_source_column="code",
    output_path_options=OutputPathOptions(
        create_parent_dirs=True,
        exist_ok=True,
    ),
)
```

## 5. 지원 타입

`column_type_map`에서 지원하는 타입:

- `"string"`
- `"bool"`
- `"int64"`
- `"float64"`
- `"float32"`
- `"uint16"`
- `"uint8"`
- `"date32"`
- `"timestamp_ms"`

## 6. 반환값

반환값은 `ConvertCsvToParquetResult` 객체다.

자주 쓰게 될 필드:

- `output_parquet_dir`
- `output_dataset_dir`
- `total_rows_read`
- `total_rows_written`
- `total_batches_built`
- `effective_delimiter`
- `normalized_columns`

디버그 모드에서만 의미가 큰 필드:

- `cast_failure_total_count`
- `cast_failures`
- `derived_failure_count`

## 7. 예외 처리

사전 검증 실패:

- `ConfigValidationError`

Polars 변환 중 fatal error:

- `ConversionFailedError`

기본 모드에서는 실패 관측을 최소화하므로, non-fatal cast failure 상세는 수집하지 않는다.

## 8. 구현상 참고 사항

- 입력 CSV는 첫 레코드를 항상 header로 처리한다.
- delimiter 자동 판별 후보는 `,`, `\t`, `|`, `;` 네 가지다.
- `exclude_columns`에 `index_source_column`이 들어 있어도 내부적으로 제외 목록에서 제거된다.
- `column_type_map`에 없는 칼럼은 모두 `float32`로 처리된다.
- `index_A`, `index_B`, `__source`는 현재 [`api.py`](./src/csv_to_parquet/api.py)에 하드코딩되어 있다.
- 이 프로젝트는 Rust bridge 없이 Python `polars`만 사용한다.
- 내부적으로 `scan_csv().collect_batches()`를 사용해 chunk 단위로 part parquet를 생성한다.

## 9. 권장 호출 패턴

```python
from csv_to_parquet import (
    ConfigValidationError,
    ConversionFailedError,
    convert_csv_to_parquet_simple,
)

try:
    result = convert_csv_to_parquet_simple(
        input_csv_path="/data/input.csv",
        output_parquet_dir="/data/out",
        column_type_map={"code": "string", "amount": "float32"},
        index_source_column="code",
    )
except ConfigValidationError as exc:
    print(f"invalid request: {exc}")
except ConversionFailedError as exc:
    print(f"conversion failed: {exc}")
else:
    print(result.output_dataset_dir)
```
