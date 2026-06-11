"""
kubectl_executor.py
-------------------
Reusable utility to safely execute kubectl commands via subprocess.
Returns structured output. Does NOT interpret the results.
"""

import subprocess
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def run_kubectl(args: list, context: Optional[str] = None, timeout: int = 30) -> dict:
    """
    Execute a kubectl command and return structured output.

    Args:
        args:    kubectl arguments, e.g. ["get", "pods", "-A"]
        context: optional kubectl context name (--context flag)
        timeout: max seconds to wait for the command

    Returns:
        {
            "success":    bool,
            "stdout":     str,
            "stderr":     str,
            "returncode": int
        }
    """
    cmd = ["kubectl"]
    if context:
        cmd += ["--context", context]
    cmd += args

    logger.info(f"Running: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        success = result.returncode == 0
        if not success:
            logger.warning(f"kubectl exited {result.returncode}: {result.stderr.strip()[:200]}")
        return {
            "success":    success,
            "stdout":     result.stdout.strip(),
            "stderr":     result.stderr.strip(),
            "returncode": result.returncode
        }

    except subprocess.TimeoutExpired:
        logger.error(f"kubectl timed out after {timeout}s: {' '.join(cmd)}")
        return {
            "success":    False,
            "stdout":     "",
            "stderr":     f"timeout after {timeout}s",
            "returncode": -1
        }
    except FileNotFoundError:
        logger.error("kubectl not found in PATH")
        return {
            "success":    False,
            "stdout":     "",
            "stderr":     "kubectl not found in PATH",
            "returncode": -1
        }
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {
            "success":    False,
            "stdout":     "",
            "stderr":     str(e),
            "returncode": -1
        }
