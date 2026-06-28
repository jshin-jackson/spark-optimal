"""Kerberos ticket renewal scheduling for long-running gateway sessions."""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Optional

from spark_optimal.governance.security.kerberos_manager import GatewayKerberosManager

logger = logging.getLogger(__name__)


class TokenRenewalScheduler:
    """Periodically re-run kinit before ticket expiry on the gateway node."""

    def __init__(
        self,
        kerberos_manager: Optional[GatewayKerberosManager] = None,
        interval_seconds: int = 3600,
    ) -> None:
        self.kerberos_manager = kerberos_manager or GatewayKerberosManager()
        self.interval_seconds = interval_seconds
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _renew_loop(self) -> None:
        while not self._stop_event.wait(self.interval_seconds):
            try:
                result = self.kerberos_manager.perform_kinit(force=True)
                if result.success:
                    logger.info("Scheduled Kerberos renewal succeeded")
                else:
                    logger.error("Scheduled Kerberos renewal failed: %s", result.error)
            except Exception as exc:
                logger.exception("Kerberos renewal error: %s", exc)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._renew_loop, daemon=True, name="kinit-renewal")
        self._thread.start()
        logger.info("Token renewal scheduler started (interval=%ss)", self.interval_seconds)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Token renewal scheduler stopped")

    def run_once(self) -> bool:
        return self.kerberos_manager.perform_kinit(force=True).success
