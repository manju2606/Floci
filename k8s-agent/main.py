"""
main.py
-------
FastAPI application for the Kubernetes Investigation Agent.

Endpoints:
    GET  /health            — liveness check
    GET  /contexts          — list available kubectl contexts
    POST /investigate       — run full investigation
"""

import logging
import json
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from kubectl_executor     import run_kubectl
from investigation_service import run_investigation

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="K8s Investigation Agent",
    description="Evidence-gathering layer for Kubernetes troubleshooting. Read-only — no fixes.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Request / Response models ──────────────────────────────────────────────────

class InvestigateRequest(BaseModel):
    context:   Optional[str] = None
    namespace: Optional[str] = None


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "k8s-investigation-agent"}


@app.get("/contexts")
def list_contexts():
    """Return all kubectl contexts from the current kubeconfig."""
    result = run_kubectl(["config", "get-contexts", "-o", "name"])
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result["stderr"])
    contexts = [c.strip() for c in result["stdout"].splitlines() if c.strip()]
    return {"contexts": contexts}


@app.post("/investigate")
def investigate(req: InvestigateRequest):
    """
    Run the full Kubernetes investigation pipeline.

    Returns structured evidence from:
    - Pod inspection
    - Log collection (failed pods only)
    - Event analysis
    - Deployment inspection
    - Network inspection
    """
    logger.info(f"POST /investigate — context={req.context!r}, namespace={req.namespace!r}")
    result = run_investigation(namespace=req.namespace, context=req.context)
    return result


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=False)
