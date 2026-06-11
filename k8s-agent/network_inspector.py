"""
network_inspector.py
--------------------
Inspects services and endpoints for selector mismatches
and missing/empty endpoint slices.  Read-only observation only.
"""

import json
import logging
from typing import Optional
from kubectl_executor import run_kubectl

logger = logging.getLogger(__name__)


def _get_services(namespace: Optional[str], context: Optional[str]) -> list:
    args = ["get", "services", "-o", "json"]
    if namespace:
        args += ["-n", namespace]
    else:
        args.append("-A")

    result = run_kubectl(args, context=context)
    if not result["success"]:
        logger.warning(f"Could not fetch services: {result['stderr']}")
        return []
    try:
        return json.loads(result["stdout"]).get("items", [])
    except json.JSONDecodeError:
        return []


def _get_endpoints(namespace: Optional[str], context: Optional[str]) -> dict:
    """Returns a dict keyed by 'namespace/name' → endpoint item."""
    args = ["get", "endpoints", "-o", "json"]
    if namespace:
        args += ["-n", namespace]
    else:
        args.append("-A")

    result = run_kubectl(args, context=context)
    if not result["success"]:
        return {}
    try:
        items = json.loads(result["stdout"]).get("items", [])
        return {
            f"{i['metadata']['namespace']}/{i['metadata']['name']}": i
            for i in items
        }
    except (json.JSONDecodeError, KeyError):
        return {}


def _endpoints_empty(ep_item: dict) -> bool:
    subsets = ep_item.get("subsets") or []
    if not subsets:
        return True
    for subset in subsets:
        if subset.get("addresses"):
            return False
    return True


def inspect_network(namespace: Optional[str] = None, context: Optional[str] = None) -> dict:
    """
    Check each service for:
    - Missing endpoints entry entirely
    - Empty endpoints (selector exists but no pods match)
    - Services of type ExternalName or headless (no selector — benign, flagged informational)
    """
    services  = _get_services(namespace, context)
    endpoints = _get_endpoints(namespace, context)

    issues         = []
    all_services   = []

    for svc in services:
        meta    = svc.get("metadata", {})
        spec    = svc.get("spec", {})
        name    = meta.get("name", "unknown")
        ns      = meta.get("namespace", "default")
        svc_type = spec.get("type", "ClusterIP")
        selector = spec.get("selector") or {}

        ep_key = f"{ns}/{name}"
        ep_item = endpoints.get(ep_key)

        issue = None

        if svc_type == "ExternalName":
            # ExternalName services have no pods — skip
            svc_info_flag = "external-name"
        elif not selector:
            # Headless or manually managed endpoints
            svc_info_flag = "no-selector"
        elif ep_item is None:
            issue = "missing-endpoints"
            svc_info_flag = "missing-endpoints"
        elif _endpoints_empty(ep_item):
            issue = "empty-endpoints"
            svc_info_flag = "empty-endpoints"
        else:
            svc_info_flag = "ok"

        svc_info = {
            "name":      name,
            "namespace": ns,
            "type":      svc_type,
            "selector":  selector,
            "flag":      svc_info_flag,
        }
        all_services.append(svc_info)

        if issue:
            issues.append({
                "name":      name,
                "namespace": ns,
                "type":      svc_type,
                "issue":     issue,
                "selector":  selector,
            })

    return {
        "healthy":      len(issues) == 0,
        "total":        len(all_services),
        "issue_count":  len(issues),
        "issues":       issues,
        "all_services": all_services,
    }
