# Build Guide

이 문서는 [`csv_to_parquet_polars`](./README.md) 프로젝트를 로컬에서 설치하고 빌드하는 방법을 설명한다.

이 프로젝트는 Rust 빌드가 없는 순수 Python 패키지다. 따라서 일반적인 `pip` / `build` 흐름으로 설치하면 된다.

## 요구 사항

- Python `3.10+`
- `pip`

## 1. 가상환경 생성

프로젝트 루트 상위에서 다음처럼 가상환경을 만든다.

```bash
cd /home/smoke_nb_sv/dev/projects/pj-etl-simple
python -m venv .venv_polars
```

활성화:

```bash
source .venv_polars/bin/activate
```

## 2. 개발용 설치

프로젝트 폴더로 이동해서 editable 설치를 수행한다.

```bash
cd /home/smoke_nb_sv/dev/projects/pj-etl-simple/csv_to_parquet_polars
python -m pip install -U pip
python -m pip install -e .
```

설치되면 다음 import가 동작해야 한다.

```bash
python -c "from csv_to_parquet import convert_csv_to_parquet_simple; print('ok')"
```

## 3. wheel 빌드

배포용 wheel / sdist를 만들고 싶으면:

```bash
cd /home/smoke_nb_sv/dev/projects/pj-etl-simple/csv_to_parquet_polars
python -m pip install build
python -m build
```

결과물은 `dist/` 아래에 생성된다.

예:

- `dist/csv_to_parquet_polars-0.1.0.tar.gz`
- `dist/csv_to_parquet_polars-0.1.0-py3-none-any.whl`

## 4. 빠른 실행 확인

```bash
python - <<'PY'
from csv_to_parquet import convert_csv_to_parquet_simple

result = convert_csv_to_parquet_simple(
    input_csv_path="/tmp/input.csv",
    output_parquet_dir="/tmp/out",
    column_type_map={
        "code": "string",
        "amount": "float32",
    },
    index_source_column="code",
    memory_limit="4 GB",
)
print(result.output_dataset_dir)
PY
```

## 5. 참고

- 출력은 단일 `.parquet` 파일이 아니라 dataset 디렉토리다.
- 실제 결과는 `<output_parquet_dir>/<csv파일명>/part-xxxxx.parquet` 형태로 생성된다.
- 사용 예시는 [`USAGE.md`](./USAGE.md)를 보면 된다.
