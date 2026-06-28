#!/usr/bin/env python3
# =============================================================================
# SDV(Synthetic Data Vault) 금융 JSON 생성기
# =============================================================================
#
# [목적]
#   SBI 파이프라인 테스트·벤치마크용 가짜 금융 거래 JSON 데이터를 생성합니다.
#   실제 고객 데이터 없이도 HDFS → Ozone → Iceberg 파이프라인 전체를 검증할 수 있습니다.
#
# [출력 형식]
#   JSONL (JSON Lines): 한 줄에 JSON 객체 하나
#   예) {"transaction_id": "...", "amount": 1234.56, ...}
#
# [사용 예]
#   pip install -r requirements/financial.txt
#   python3 data_gen/generate_financial_json.py --target-gb 10
#   python3 data_gen/generate_financial_json.py --target-gb 0.1   # 로컬 빠른 테스트
#
# [SDV란?]
#   소량 seed 데이터의 통계 패턴을 학습해 대량의 realistic synthetic data를 생성하는 라이브러리
# =============================================================================

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

# 프로젝트 루트를 Python path에 추가 (data_gen.financial_schema import 가능하게)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_gen.financial_schema import build_seed_dataframe

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# 1 GB = 1024^3 bytes (바이너리 기준)
BYTES_PER_GB = 1024**3


def parse_args() -> argparse.Namespace:
    """커맨드라인 인자 파싱."""
    parser = argparse.ArgumentParser(description="SDV financial JSON generator")
    parser.add_argument(
        "--output-dir",
        default="data/output/financial",
        help="생성된 JSONL 파일 저장 디렉터리",
    )
    parser.add_argument(
        "--target-gb",
        type=float,
        default=10.0,
        help="목표 데이터 크기 (GB). 10=약 10GB",
    )
    parser.add_argument(
        "--seed-rows",
        type=int,
        default=5000,
        help="SDV 학습용 seed 행 수 (많을수록 패턴 정확, 느림)",
    )
    parser.add_argument(
        "--batch-rows",
        type=int,
        default=100000,
        help="한 번에 생성·저장하는 행 수 (메모리 vs 속도 trade-off)",
    )
    return parser.parse_args()


def _fit_synthesizer(seed_df: pd.DataFrame):
    """
    SDV GaussianCopulaSynthesizer 학습.

    seed_df의 컬럼별 분포(금액, 날짜, 카테고리 등)를 학습한 뒤
    synthesizer.sample()으로 무한히 유사 데이터 생성 가능.
    """
    from sdv.metadata import SingleTableMetadata
    from sdv.single_table import GaussianCopulaSynthesizer

    metadata = SingleTableMetadata()
    metadata.detect_from_dataframe(seed_df)

    # 컬럼 타입 명시 — SDV가 더 정확한 분포를 학습하도록 도움
    metadata.update_column("transaction_id", sdtype="id")
    metadata.update_column("transaction_ts", sdtype="datetime")
    metadata.update_column("amount", sdtype="numerical")
    metadata.update_column("balance_after", sdtype="numerical")

    synthesizer = GaussianCopulaSynthesizer(metadata)
    synthesizer.fit(seed_df)
    return synthesizer


def _normalize_batch(df: pd.DataFrame) -> pd.DataFrame:
    """Spark/Hive가 읽기 쉬운 형식으로 컬럼 값 정규화."""
    df = df.copy()
    if "transaction_id" in df.columns:
        df["transaction_id"] = df["transaction_id"].astype(str)
    if "transaction_ts" in df.columns:
        # ISO 8601 문자열 (Spark to_timestamp() 호환)
        df["transaction_ts"] = pd.to_datetime(df["transaction_ts"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    if "amount" in df.columns:
        df["amount"] = df["amount"].round(2)
    if "balance_after" in df.columns:
        df["balance_after"] = df["balance_after"].round(2)
    return df


def generate_jsonl(output_dir: Path, target_gb: float, seed_rows: int, batch_rows: int) -> dict:
    """
    target_gb에 도달할 때까지 JSONL 파일을 배치 단위로 생성.

    Returns:
        manifest dict — 생성 결과 요약 (행 수, 파일 수, 실제 GB)
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: seed 데이터로 SDV 학습
    seed_df = build_seed_dataframe(seed_rows)
    synthesizer = _fit_synthesizer(seed_df)

    target_bytes = int(target_gb * BYTES_PER_GB)
    total_bytes = 0
    total_rows = 0
    file_index = 0

    logger.info("SDV synthesizer trained on %s seed rows", seed_rows)
    logger.info("Target size: %.2f GB (%s bytes)", target_gb, target_bytes)

    # Step 2: 목표 크기까지 반복 생성
    while total_bytes < target_bytes:
        batch = _normalize_batch(synthesizer.sample(num_rows=batch_rows))
        file_path = output_dir / f"financial_transactions_{file_index:05d}.jsonl"

        with file_path.open("w", encoding="utf-8") as handle:
            for record in batch.to_dict(orient="records"):
                line = json.dumps(record, ensure_ascii=False) + "\n"
                handle.write(line)
                total_bytes += len(line.encode("utf-8"))
                total_rows += 1
                if total_bytes >= target_bytes:
                    break

        logger.info(
            "Wrote %s — cumulative %.2f GB, %s rows",
            file_path.name,
            total_bytes / BYTES_PER_GB,
            total_rows,
        )
        file_index += 1

    # Step 3: manifest.json — Airflow/운영에서 생성 결과 추적용
    manifest = {
        "target_gb": target_gb,
        "actual_gb": round(total_bytes / BYTES_PER_GB, 4),
        "total_rows": total_rows,
        "files": file_index,
        "output_dir": str(output_dir),
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    logger.info("Generation complete: %s", manifest)
    return manifest


def main() -> int:
    args = parse_args()
    generate_jsonl(
        output_dir=Path(args.output_dir),
        target_gb=args.target_gb,
        seed_rows=args.seed_rows,
        batch_rows=args.batch_rows,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
