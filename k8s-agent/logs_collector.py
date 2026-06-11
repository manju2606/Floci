"""
logs_collector.py
-----------------
Fetches the last N lines of logs from failed pods and filters
for lines that look like errors, exceptions, or startup failures.

Does NOT store, forward, or analyse the logs — evidence gathering only.
"""

import logging
from typing import Optional
from kubectl_executor import run_kubectl

logger = logging.getLogger(__name__)

# Keywords that suggest an error line worth surfacing
ERROR_KEYWORDS = [
    "error", "exception", "fatal", "panic", "fail", "crash",
    "refused", "timeout", "oomkilled", "killed", "sigkill", "sigterm",
    "no such file", "permission denied", "missing", "not found",
    "traceback", "stacktrace", "envvar", "env var",
    "image pull", "back-off", "backoff",
]

MAX_LOG_LINES = 100   # lines fetched from kubectl
MAX_ERROR_LINES = 50  # error lines returned per pod


def _is_error_line(line: str) -> bool:
    lower = line.lower()
    return any(kw in lower for kw in ERROR_KEYWORDS)


def collect_logs(
    pod_name:  str,
    namespace: str = "default",
    previous:  bool = False,
    context:   Optional[str] = None,
) -> dict:
    """
    Fetch logs for a single pod and return only error-relevant lines.

    Tries current container first, then --previous if current is empty.
    """
    base_args = ["logs", pod_name, "-n", namespace, "--tail", str(MAX_LOG_LINES)]

    result = run_kubectl(base_args, context=context)
    raw_lines = result["stdout"].splitlines() if result["success"] else []

    # If no output and container may have already restarted, try --previous
    if not raw_lines:
        prev_result = run_kubectl(base_args + ["--previous"], context=context)
        if prev_result["success"] and prev_result["stdout"]:
            raw_lines = prev_result["stdout"].splitlines()
            logger.info(f"Used --previous logs for {namespace}/{pod_name}")

    error_lines = [line for line in raw_lines if _is_error_line(line)]

    return {
        "pod":         pod_name,
        "namespace":   namespace,
        "lines_fetched": len(raw_lines),
        "error_lines": error_lines[:MAX_ERROR_LINES],
        "error_count": len(error_lines),
        "fetch_error": result["stderr"] if not result["success"] else None,
    }


def collect_logs_for_pods(
    problematic_pods: list,
    context: Optional[str] = None,
) -> dict:
    """
    Collect logs for every pod in the problematic_pods list.

    Returns a dict keyed by "namespace/pod-name".
    """
    results = {}
    for pod in problematic_pods:
        name = pod["name"]
        ns   = pod.get("namespace", "default")
        key  = f"{ns}/{name}"
        logger.info(f"Collecting logs for {key}")
        results[key] = collect_logs(name, ns, context=context)
    return results
