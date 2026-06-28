"""Gateway node Kerberos helpers."""

from __future__ import annotations

import logging
import os
import subprocess
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


class KerberosAuthError(RuntimeError):
    pass


@dataclass
class KinitResult:
    success: bool
    error: Optional[str] = None


@dataclass
class AuthStatus:
    is_valid: bool
    principal: Optional[str] = None
    error: Optional[str] = None


class GatewayKerberosManager:
    """Manage kinit on the gateway node using systest.keytab."""

    def __init__(
        self,
        keytab_path: str | None = None,
        principal: str | None = None,
    ) -> None:
        self.keytab_path = keytab_path or os.environ.get("KEYTAB", "/opt/cloudera/systest.keytab")
        self.principal = principal or os.environ.get("PRINCIPAL", "systest@QE-INFRA-AD.CLOUDERA.COM")

    def perform_kinit(self, force: bool = False) -> KinitResult:
        if not force and self.check_auth_status().is_valid:
            logger.info("Existing Kerberos ticket is valid for %s", self.principal)
            return KinitResult(success=True)

        if not os.path.isfile(self.keytab_path):
            return KinitResult(success=False, error=f"Keytab not found: {self.keytab_path}")

        cmd = ["kinit", "-kt", self.keytab_path, self.principal]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=False)
        except (OSError, subprocess.TimeoutExpired) as exc:
            return KinitResult(success=False, error=str(exc))

        if result.returncode == 0:
            logger.info("kinit succeeded for %s", self.principal)
            return KinitResult(success=True)
        return KinitResult(success=False, error=result.stderr.strip() or result.stdout.strip())

    def check_auth_status(self) -> AuthStatus:
        try:
            result = subprocess.run(
                ["klist", "-s"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return AuthStatus(is_valid=False, error=str(exc))

        if result.returncode != 0:
            return AuthStatus(is_valid=False, error="No valid Kerberos ticket cache")

        principal = self._read_principal()
        return AuthStatus(is_valid=True, principal=principal)

    def _read_principal(self) -> Optional[str]:
        try:
            result = subprocess.run(
                ["klist"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            return None
        for line in result.stdout.splitlines():
            if "Default principal" in line:
                return line.split(":", 1)[1].strip()
        return None

    def ensure_authenticated(self) -> None:
        status = self.perform_kinit(force=False)
        if not status.success:
            raise KerberosAuthError(status.error or "Kerberos authentication failed")
