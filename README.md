# `csv_to_parquet_polars`

`csv_to_parquet_polars`는 CSV 파일을 Polars 기반으로 읽어 Parquet dataset 디렉터리로 변환하는 Python 패키지다.

실제 import 경로는 `csv_to_parquet` 이다.

## 현재 구조

- `pyproject.toml`: 패키지 메타데이터
- `src/csv_to_parquet/__init__.py`: 공개 API 노출
- `src/csv_to_parquet/api.py`: 변환 로직
- `src/csv_to_parquet/models.py`: 요청/응답 dataclass
- `src/csv_to_parquet/exceptions.py`: 예외 타입

## 주요 기능

- `convert_csv_to_parquet()`
  - 요청 dataclass를 받아 변환 수행

- `convert_csv_to_parquet_simple()`
  - 경로와 타입 매핑만 바로 넘겨서 간단히 호출 가능

- 자동 구분자 감지
- batch 단위 Parquet part 파일 생성
- 변환 결과 요약 반환
- 필요 시 cast 실패 수집

## 출력 형태

출력은 단일 `.parquet` 파일이 아니라 dataset 디렉터리다.

예를 들어 입력 파일이 `sample.csv` 이고 출력 루트가 `out/` 이면:

- `out/sample/part-00000.parquet`
- `out/sample/part-00001.parquet`

같은 형태로 생성된다.

## 설치

```bash
pip install -e .
```

## 빠른 확인

```bash
python -c "from csv_to_parquet import convert_csv_to_parquet_simple; print('ok')"
```

자세한 사용 예시는 [USAGE.md](./USAGE.md) 를 보면 된다.
