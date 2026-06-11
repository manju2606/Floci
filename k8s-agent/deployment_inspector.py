"""
deployment_inspector.py
-----------------------
Inspects deployments for replica health, rollout failures,
and unhealthy conditions.  Read-only — does NOT fix anything.
"""

import json
import logging
from typing import Optional
from kubectl_executor import run_kubectl

logger = logging.getLogger(__name__)


def _deployment_healthy(dep: dict) -> bool:
    spec_replicas = dep.get("spec", {}).get("replicas", 1)
    status        = dep.get("status", {})
    available     = status.get("availableReplicas", 0) or 0
    ready         = status.get("readyReplicas", 0) or 0
    unavailable   = status.get("unavailableReplicas", 0) or 0

    if spec_replicas > 0 and available < spec_replicas:
        return False
    if unavailable > 0:
        return False

    for condition in status.get("conditions", []):
        if condition.get("type") == "Available" and condition.get("status") != "True":
            return False
        if condition.get("type") == "Progressing" and condition.get("reason") == "ProgressDeadlineExceeded":
            return False

    return True


def _get_conditions(dep: dict) -> list:
    return [
        {
            "type":    c.get("type"),
            "status":  c.get("status"),
            "reason":  c.get("reason"),
            "message": c.get("message", "")[:200],
        }
        for c in dep.get("status", {}).get("conditions", [])
    ]


def inspect_deployments(namespace: Optional[str] = None, context: Optional[str] = None) -> dict:
    """
    Return all deployments and flag unhealthy ones.
    """
    args = ["get", "deployments", "-o", "json"]
    if namespace:
        args += ["-n", namespace]
    else:
        args.append("-A")

    result = run_kubectl(args, context=context)
    if not result["success"]:
        return {
            "healthy":               False,
            "error":                 result["stderr"],
            "total":                 0,
            "unhealthy_deployments": [],
            "all_deployments":       [],
        }

    try:
        data = json.loads(result["stdout"])
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse deployments JSON: {e}")
        return {
            "healthy":               False,
            "error":                 "Failed to parse kubectl output",
            "total":                 0,
            "unhealthy_deployments": [],
            "all_deployments":       [],
        }

    unhealthy     = []
    all_deployments = []

    for dep in data.get("items", []):
        meta   = dep.get("metadata", {})
        status = dep.get("status", {})
        spec   = dep.get("spec", {})

        name      = meta.get("name", "unknown")
        ns        = meta.get("namespace", "default")
        desired   = spec.get("replicas", 0) or 0
        ready     = status.get("readyReplicas", 0) or 0
        available = status.get("availableReplicas", 0) or 0
        updated   = status.get("updatedReplicas", 0) or 0
        healthy   = _deployment_healthy(dep)
        conditions = _get_conditions(dep)

        dep_info = {
            "name":       name,
            "namespace":  ns,
            "desired":    desired,
            "ready":      ready,
            "available":  available,
            "updated":    updated,
            "healthy":    healthy,
            "conditions": conditions,
        }
        all_deployments.append(dep_info)

        if not healthy:
            unhealthy.append(dep_info)

    return {
        "healthy":               len(unhealthy) == 0,
        "total":                 len(all_deployments),
        "unhealthy_deployments": unhealthy,
        "all_deployments":       all_deployments,
    }
