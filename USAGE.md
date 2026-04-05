# `csv_to_parquet` 사용법

이 문서는 외부 사용자가 `csv_to_parquet` 패키지를 import 해서 변환을 실행하는 방법을 설명한다.

## 설치

```bash
pip install -e .
```

## 공개 진입점

가장 간단한 진입점은 `convert_csv_to_parquet_simple()` 이다.

```python
from pathlib import Path

import polars as pl

from csv_to_parquet import convert_csv_to_parquet_simple

result = convert_csv_to_parquet_simple(
    input_csv_path="sample.csv",
    output_parquet_dir="out",
    column_type_map={
        "id": pl.Int64,
        "name": pl.String,
        "amount": pl.Float64,
    },
    index_source_column="id",
)

print(result.output_dataset_dir)
print(result.total_rows_written)
```

## 필수 인자

- `input_csv_path`: 입력 CSV 파일 경로
- `output_parquet_dir`: 출력 루트 디렉터리
- `column_type_map`: 컬럼명별 Polars 타입
- `index_source_column`: `index_A`, `index_B` 파생에 사용할 원본 컬럼

## 주요 옵션

- `exclude_columns`: 제외할 컬럼 목록
- `delimiter`: 구분자 직접 지정
- `compression`: `snappy`, `zstd`, `none`
- `max_rows_per_chunk`: 입력 batch 크기
- `max_rows_per_batch`: Parquet row group 크기
- `trim_whitespace`: 문자열 trim 여부
- `debug_collect_failures`: cast 실패 샘플 수집 여부
- `cast_failure_limit`: 실패 샘플 최대 개수

## 요청 dataclass 방식

세부 옵션을 명시적으로 다루고 싶다면 `ConvertCsvToParquetRequest` 를 직접 넘길 수 있다.

```python
from csv_to_parquet import ConvertCsvToParquetRequest, convert_csv_to_parquet
import polars as pl

request = ConvertCsvToParquetRequest(
    input_csv_path="sample.csv",
    output_parquet_dir="out",
    column_type_map={"id": pl.Int64, "name": pl.String},
    exclude_columns=[],
    index_source_column="id",
)

result = convert_csv_to_parquet(request)
```

## 반환값

반환 타입은 `ConvertCsvToParquetResult` 이다.

자주 보는 필드:

- `output_parquet_dir`
- `output_dataset_dir`
- `total_rows_read`
- `total_rows_written`
- `total_batches_built`
- `elapsed_ms`
- `throughput_rows_per_sec`
- `cast_failure_total_count`
- `effective_delimiter`

## 주의사항

- 출력은 dataset 디렉터리이며, 단일 파일이 아니다.
- `index_source_column` 이 입력 헤더에 없으면 `ConfigValidationError` 가 발생한다.
- `index_A`, `index_B`, `__source` 컬럼은 내부 규칙에 따라 추가된다.
