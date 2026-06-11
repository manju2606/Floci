"""
pod_inspector.py
----------------
Detects unhealthy pods by reading their real container state,
the same way kubectl derives the STATUS column.

Detects: CrashLoopBackOff, ImagePullBackOff, ErrImagePull,
         Error, OOMKilled, Terminating, Pending, Init:N/M, etc.
"""

import json
import logging
from typing import Optional
from kubectl_executor import run_kubectl

logger = logging.getLogger(__name__)

# Statuses that indicate a pod is NOT healthy
UNHEALTHY_REASONS = {
    "CrashLoopBackOff",
    "ImagePullBackOff",
    "ErrImagePull",
    "Error",
    "OOMKilled",
    "ContainerCreating",
    "InvalidImageName",
    "CreateContainerConfigError",
    "CreateContainerError",
    "RunContainerError",
}


def _get_pod_status_str(pod: dict) -> str:
    """Derive the real status string the same way kubectl does."""
    meta   = pod.get("metadata", {})
    status = pod.get("status", {})
    spec   = pod.get("spec", {})

    # Terminating takes priority
    if meta.get("deletionTimestamp"):
        return "Terminating"

    # Init containers in progress?
    for i, ic in enumerate(status.get("initContainerStatuses", [])):
        waiting = ic.get("state", {}).get("waiting", {})
        if waiting.get("reason"):
            return f"Init:{waiting['reason']}"
        if not ic.get("ready"):
            total = len(spec.get("initContainers", []))
            return f"Init:{i}/{total}"

    # Regular container states
    for cs in status.get("containerStatuses", []):
        waiting    = cs.get("state", {}).get("waiting", {})
        terminated = cs.get("state", {}).get("terminated", {})
        if waiting.get("reason"):
            return waiting["reason"]
        if terminated.get("reason"):
            return terminated["reason"]
        if terminated.get("exitCode", 0) != 0:
            return "Error"

    return status.get("phase", "Unknown")


def _get_restart_count(pod: dict) -> int:
    statuses = pod.get("status", {}).get("containerStatuses", [])
    return sum(cs.get("restartCount", 0) for cs in statuses)


def _is_unhealthy(status_str: str, phase: str) -> bool:
    if status_str in UNHEALTHY_REASONS:
        return True
    if phase in ("Failed", "Unknown"):
        return True
    # Stuck init container  e.g.  Init:0/1
    if status_str.startswith("Init:") and "/" in status_str:
        return True
    return False


def inspect_pods(namespace: Optional[str] = None, context: Optional[str] = None) -> dict:
    """
    Return all pod statuses and a list of problematic pods.

    Returns:
        {
            "healthy":          bool,
            "total":            int,
            "problematic_pods": [...],
            "all_pods":         [...]
        }
    """
    args = ["get", "pods", "-o", "json"]
    if namespace:
        args += ["-n", namespace]
    else:
        args.append("-A")

    result = run_kubectl(args, context=context)
    if not result["success"]:
        return {
            "healthy":          False,
            "error":            result["stderr"],
            "total":            0,
            "problematic_pods": [],
            "all_pods":         []
        }

    try:
        data = json.loads(result["stdout"])
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse pod JSON: {e}")
        return {
            "healthy":          False,
            "error":            "Failed to parse kubectl output",
            "total":            0,
            "problematic_pods": [],
            "all_pods":         []
        }

    problematic = []
    all_pods    = []

    for pod in data.get("items", []):
        meta   = pod.get("metadata", {})
        status = pod.get("status", {})
        spec   = pod.get("spec", {})

        name       = meta.get("name", "unknown")
        ns         = meta.get("namespace", "default")
        phase      = status.get("phase", "Unknown")
        status_str = _get_pod_status_str(pod)
        restarts   = _get_restart_count(pod)
        node       = spec.get("nodeName")

        ready_count = sum(
            1 for cs in status.get("containerStatuses", []) if cs.get("ready")
        )
        total_count = len(spec.get("containers", []))

        pod_info = {
            "name":      name,
            "namespace": ns,
            "status":    status_str,
            "phase":     phase,
            "ready":     f"{ready_count}/{total_count}",
            "restarts":  restarts,
            "node":      node,
        }
        all_pods.append(pod_info)

        if _is_unhealthy(status_str, phase):
            problematic.append({
                "name":      name,
                "namespace": ns,
                "status":    status_str,
                "phase":     phase,
                "restarts":  restarts,
                "node":      node,
            })

    return {
        "healthy":          len(problematic) == 0,
        "total":            len(all_pods),
        "problematic_pods": problematic,
        "all_pods":         all_pods,
    }
