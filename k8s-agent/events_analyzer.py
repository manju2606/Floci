"""
events_analyzer.py
------------------
Reads Kubernetes events and surfaces Warning events.
Does NOT diagnose — only gathers and categorizes.
"""

import json
import logging
from typing import Optional
from kubectl_executor import run_kubectl

logger = logging.getLogger(__name__)

WARNING_REASONS = {
    "FailedScheduling",
    "BackOff",
    "FailedMount",
    "FailedPull",
    "ErrImagePull",
    "Unhealthy",
    "FailedCreate",
    "EvictionThresholdMet",
    "NodeNotReady",
    "OOMKilling",
    "FreeDiskSpaceFailed",
    "NetworkNotReady",
    "FailedAttachVolume",
    "FailedDetachVolume",
    "KubeletNotReady",
    "Killing",
    "Preempting",
    "ProviderCreateError",
}

MAX_EVENTS = 100


def analyze_events(namespace: Optional[str] = None, context: Optional[str] = None) -> dict:
    """
    Fetch all Kubernetes events and return a summary of warning events.
    """
    args = ["get", "events", "-o", "json"]
    if namespace:
        args += ["-n", namespace]
    else:
        args.append("-A")

    result = run_kubectl(args, context=context)
    if not result["success"]:
        return {
            "healthy":         False,
            "error":           result["stderr"],
            "total":           0,
            "warning_count":   0,
            "warning_events":  [],
            "normal_count":    0,
        }

    try:
        data = json.loads(result["stdout"])
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse events JSON: {e}")
        return {
            "healthy":        False,
            "error":          "Failed to parse kubectl output",
            "total":          0,
            "warning_count":  0,
            "warning_events": [],
            "normal_count":   0,
        }

    warning_events = []
    normal_count   = 0

    for event in data.get("items", []):
        event_type = event.get("type", "Normal")
        reason     = event.get("reason", "")
        message    = event.get("message", "")
        ns         = event.get("metadata", {}).get("namespace", "")
        name       = event.get("metadata", {}).get("name", "")
        count      = event.get("count", 1)
        obj        = event.get("involvedObject", {})
        obj_kind   = obj.get("kind", "")
        obj_name   = obj.get("name", "")
        last_ts    = event.get("lastTimestamp") or event.get("eventTime") or ""

        if event_type == "Warning" or reason in WARNING_REASONS:
            warning_events.append({
                "namespace":   ns,
                "reason":      reason,
                "message":     message[:300],
                "object_kind": obj_kind,
                "object_name": obj_name,
                "count":       count,
                "last_time":   last_ts,
            })
        else:
            normal_count += 1

    warning_events.sort(key=lambda e: e.get("count", 0), reverse=True)

    return {
        "healthy":        len(warning_events) == 0,
        "total":          normal_count + len(warning_events),
        "warning_count":  len(warning_events),
        "warning_events": warning_events[:MAX_EVENTS],
        "normal_count":   normal_count,
    }
