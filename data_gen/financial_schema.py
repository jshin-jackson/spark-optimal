"""
금융 거래(Financial Transaction) 스키마 정의 — SDV seed 데이터용.

[포함 필드]
  transaction_id   : 거래 고유 ID (UUID)
  account_id       : 계좌 번호
  customer_id      : 고객 ID
  transaction_type : DEBIT, CREDIT, UPI, NEFT 등
  amount           : 거래 금액
  merchant_category: MCC 카테고리 (GROCERY, FUEL, ...)
  channel          : ATM, UPI, MOBILE, BRANCH 등
  transaction_ts   : 거래 시각 (ISO 8601)
  status           : SUCCESS / FAILED / PENDING
  balance_after    : 거래 후 잔액

[사용처]
  - data_gen/generate_financial_json.py (SDV seed)
  - spark_optimal/governance/standards/data_standards.py (품질 검증)
"""

from __future__ import annotations

from datetime import datetime, timedelta
import random
import uuid

import pandas as pd

# 은행 거래 유형 코드
TRANSACTION_TYPES = ["DEBIT", "CREDIT", "TRANSFER", "UPI", "NEFT", "RTGS", "IMPS"]

# 거래 채널
CHANNELS = ["ATM", "UPI", "NEFT", "MOBILE", "BRANCH", "INTERNET"]

# 가맹점 카테고리 (MCC 기반)
MERCHANT_CATEGORIES = [
    "GROCERY",
    "FUEL",
    "RESTAURANT",
    "ECOMMERCE",
    "UTILITIES",
    "HEALTHCARE",
    "TRAVEL",
    "INSURANCE",
    "SALARY",
    "LOAN_REPAYMENT",
]

STATUSES = ["SUCCESS", "FAILED", "PENDING"]
CURRENCIES = ["INR"]


def build_seed_dataframe(num_rows: int = 5000) -> pd.DataFrame:
    """
    SDV 학습용 seed DataFrame 생성.

    Args:
        num_rows: 생성할 seed 행 수 (기본 5000)

    Returns:
        pandas DataFrame — SDV fit() 입력
    """
    base = datetime(2024, 1, 1)
    rows = []
    for _ in range(num_rows):
        txn_ts = base + timedelta(
            days=random.randint(0, 730),      # 2년 범위
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )
        rows.append(
            {
                "transaction_id": str(uuid.uuid4()),
                "account_id": f"ACC{random.randint(100000, 999999)}",
                "customer_id": f"CUST{random.randint(10000, 99999)}",
                "transaction_type": random.choice(TRANSACTION_TYPES),
                "amount": round(random.uniform(10.0, 500000.0), 2),
                "currency": "INR",
                "merchant_name": f"Merchant_{random.randint(1, 5000)}",
                "merchant_category": random.choice(MERCHANT_CATEGORIES),
                "channel": random.choice(CHANNELS),
                "transaction_ts": txn_ts.isoformat(),
                "branch_code": f"BR{random.randint(100, 999)}",
                # SUCCESS 92%, FAILED 5%, PENDING 3% — realistic 비율
                "status": random.choices(STATUSES, weights=[0.92, 0.05, 0.03])[0],
                "balance_after": round(random.uniform(0.0, 2000000.0), 2),
                "country_code": "IN",
            }
        )
    return pd.DataFrame(rows)


# ETL 품질 검증에서 사용하는 필수 컬럼 목록
FINANCIAL_REQUIRED_COLUMNS = list(build_seed_dataframe(1).columns)
