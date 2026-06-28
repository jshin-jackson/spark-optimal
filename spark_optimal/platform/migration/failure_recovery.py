"""Migration failure recovery with retry policy."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Callable, List, TypeVar

from spark_optimal.platform.migration.progress_tracker import BatchResult

logger = logging.getLogger(__name__)
T = TypeVar("T")


@dataclass
class RetryPolicy:
    max_retries: int = 3
    backoff_seconds: float = 5.0


class FailureRecoveryEngine:
    def __init__(self, policy: RetryPolicy | None = None) -> None:
        self.policy = policy or RetryPolicy()
        self.failures: List[dict] = []

    def handle_failure(self, batch_id: int, error: Exception) -> None:
        logger.error("Batch %s failed: %s", batch_id, error)
        self.failures.append({"batch_id": batch_id, "error": str(error), "time": time.time()})

    def retry_failed_batches(self, failed_batch_ids: List[int], migrate_fn: Callable[[int], BatchResult]) -> List[BatchResult]:
        results: List[BatchResult] = []
        for batch_id in failed_batch_ids:
            for attempt in range(1, self.policy.max_retries + 1):
                try:
                    logger.info("Retry batch %s attempt %s", batch_id, attempt)
                    result = migrate_fn(batch_id)
                    results.append(result)
                    break
                except Exception as exc:
                    self.handle_failure(batch_id, exc)
                    time.sleep(self.policy.backoff_seconds * attempt)
            else:
                results.append(BatchResult(batch_id=batch_id, row_count=0, success=False, error="max retries exceeded"))
        return results
