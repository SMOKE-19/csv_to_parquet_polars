# csv_to_parquet_polars

Python `polars` 기반 CSV -> Parquet 변환 패키지다.

현재 디렉토리는 메모리 사용량을 낮추기 위해 chunk 기반 dataset 출력 방식으로 구성된 `polars` 구현이다.

## 현재 상태

- 공개 API:
  - `convert_csv_to_parquet_simple()`
- 구현 방식:
  - pure Python
  - `polars.scan_csv().collect_batches()`
  - chunk 단위 eager transform
  - dataset directory + `part-xxxxx.parquet`
- 주요 기능:
  - header 읽기
  - delimiter 자동 판별
  - 중복 header rename
  - `column_type_map`에 전달한 Polars dtype 기반 캐스팅
  - 미지정 컬럼 `pl.Float32`
  - cast failure 디버그 수집
  - `__source`, `index_A`, `index_B` 생성

## 디렉토리 구조

- [`pyproject.toml`](./pyproject.toml)
  - Python 패키징 설정
- [`USAGE.md`](./USAGE.md)
  - 공개 API 사용 예시
- [`src/csv_to_parquet/api.py`](./src/csv_to_parquet/api.py)
  - Polars 기반 변환 구현
- [`src/csv_to_parquet/models.py`](./src/csv_to_parquet/models.py)
  - request/result dataclass
- [`src/csv_to_parquet/exceptions.py`](./src/csv_to_parquet/exceptions.py)
  - 예외 타입

## 로컬 개발 메모

- 이 프로젝트는 Rust extension을 사용하지 않는다.
- 벤치마크와 실행은 `polars`가 설치된 Python 환경에서 돌려야 한다.
- 현재 프로젝트에서는 [`../.venv_polars`](../.venv_polars)를 사용했다.
- 출력은 단일 `.parquet` 파일이 아니라 dataset 디렉토리다.
- 예: `output_parquet_dir="/data/out"` 이면 `"/data/out/<csv파일명>/part-00000.parquet"` 형태로 저장된다.

## 구현 참고

현재 `__source`, `index_A`, `index_B` 칼럼명은 [`api.py`](./src/csv_to_parquet/api.py)에 하드코딩되어 있다.

이 프로젝트는 Rust crate를 직접 작성해서 붙인 구조가 아니다.

- Rust `pyo3` bridge 없음
- `rust_src/`, `Cargo.toml`, `target/` 없음
- 변환 구현은 전부 [`api.py`](./src/csv_to_parquet/api.py)에 있다
