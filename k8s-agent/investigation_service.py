"""
investigation_service.py
------------------------
Orchestrator: runs all inspectors in sequence and returns
a single structured investigation payload.

No AI, no root cause analysis, no fixes — evidence only.
"""

import logging
from typing import Optional

from pod_inspector       import inspect_pods
from logs_collector      import collect_logs_for_pods
from events_analyzer     import analyze_events
from deployment_inspector import inspect_deployments
from network_inspector   import inspect_network

logger = logging.getLogger(__name__)


def run_investigation(
    namespace: Optional[str] = None,
    context:   Optional[str] = None,
) -> dict:
    """
    Run the full Kubernetes investigation pipeline.

    Steps (in order):
        1. Pod Inspector   — detect unhealthy pods
        2. Logs Collector  — fetch error logs for failed pods
        3. Events Analyzer — surface Warning events
        4. Deployment Inspector — check replica health
        5. Network Inspector    — detect empty/missing endpoints

    Returns:
        {
            "status": "success" | "partial" | "error",
            "context": str | None,
            "namespace": str | None,
            "investigation": {
                "pods":        { ... },
                "logs":        { ... },
                "events":      { ... },
                "deployments": { ... },
                "network":     { ... },
            }
        }
    """
    logger.info(f"Starting investigation — context={context!r}, namespace={namespace!r}")
    errors = []

    # 1. Pods
    try:
        pod_result = inspect_pods(namespace=namespace, context=context)
    except Exception as e:
        logger.error(f"Pod inspection failed: {e}")
        pod_result = {"healthy": False, "error": str(e), "total": 0, "problematic_pods": [], "all_pods": []}
        errors.append("pods")

    # 2. Logs — only for pods that are problematic
    try:
        problematic = pod_result.get("problematic_pods", [])
        logs_result = collect_logs_for_pods(problematic, context=context)
    except Exception as e:
        logger.error(f"Log collection failed: {e}")
        logs_result = {"error": str(e)}
        errors.append("logs")

    # 3. Events
    try:
        events_result = analyze_events(namespace=namespace, context=context)
    except Exception as e:
        logger.error(f"Events analysis failed: {e}")
        events_result = {"healthy": False, "error": str(e), "total": 0, "warning_count": 0, "warning_events": []}
        errors.append("events")

    # 4. Deployments
    try:
        deployment_result = inspect_deployments(namespace=namespace, context=context)
    except Exception as e:
        logger.error(f"Deployment inspection failed: {e}")
        deployment_result = {"healthy": False, "error": str(e), "total": 0, "unhealthy_deployments": [], "all_deployments": []}
        errors.append("deployments")

    # 5. Network
    try:
        network_result = inspect_network(namespace=namespace, context=context)
    except Exception as e:
        logger.error(f"Network inspection failed: {e}")
        network_result = {"healthy": False, "error": str(e), "total": 0, "issues": []}
        errors.append("network")

    overall_status = "error" if len(errors) == 5 else ("partial" if errors else "success")

    logger.info(f"Investigation complete — status={overall_status}, errors={errors}")

    return {
        "status":    overall_status,
        "context":   context,
        "namespace": namespace,
        "investigation": {
            "pods":        pod_result,
            "logs":        logs_result,
            "events":      events_result,
            "deployments": deployment_result,
            "network":     network_result,
        },
    }
