# Changelog

All notable changes to the Floci local DevOps dashboard are documented here.

---

## [v4.2] — 2026-06-12

### Live Version Banner in Upgrade Agent

- Version banner added inside the runtime panel — fills in progressively as the SSE stream runs
- **Phase 1 done** → Current version pill appears immediately (blue, e.g. `v1.28.0`)
- **Phase 3 done** → Latest version pill appears alongside it with an arrow, plus a green **✓ Up to date** or orange **↑ Upgrade available** badge — before the analysis phase even starts
- Users see both versions seconds earlier rather than waiting for the full results card to render
- `setVersionBanner()` used by both the mid-stream banner and the final status strip message

---

## [v4.1] — 2026-06-12

### K8s Upgrade Agent

New agent page (`k8s-upgrade.html`) that checks the cluster's Kubernetes version against the latest stable release and generates a tailored upgrade playbook.

#### Features
- **4-phase SSE stream**: Cluster Connection → Node Versions → Latest K8s Release (GitHub API) → Upgrade Analysis
- **Cluster-type detection**: Kind clusters (detected by `kind-` context prefix) get delete + recreate steps; kubeadm clusters get one-minor-at-a-time `kubeadm upgrade apply` steps
- **Version comparison card**: current version (blue badge) vs latest stable (green badge) with skew count
- **Upgrade path visualisation**: `v1.28.0 → v1.29.x → v1.30.x → v1.31.2` showing each required hop
- **Nodes table**: name, role (control-plane / worker), kubelet version, Ready status
- **Upgrade playbook**: numbered step cards with click-to-copy commands and orange warning boxes on destructive operations (e.g. Kind cluster recreation)
- Graceful fallback if GitHub API is unreachable (still shows current version)
- New card on main dashboard; **↑ Upgrade** nav link added to K8s Investigation Agent header
- Backend: `GET /api/upgrade/stream`, `generateUpgradePlan()`, `parseVersion()`, `fetchGitHub()`

---

## [v4.0] — 2026-06-12 · Agent Added

**Milestone release — K8s Agent suite is complete.**

### What's in v4.0

This release bundles everything built across the v3.x series into a cohesive AI-assisted Kubernetes operations platform:

- **K8s Investigation Agent** — real-time SSE-streamed cluster investigation across 5 stages (Pods, Logs, Events, Deployments, Network) with live progress bars
- **AI Reasoning Engine** — rule-based confidence scoring (0–100), severity classification, and root-cause identification; no LLM required
- **K8s Resolution Engine** — problem-by-problem fix guide with what/why/how breakdown, copy-ready kubectl commands, and log evidence
- **DynamoDB history** — every investigation and resolution run is persisted to LocalStack DynamoDB (`k8s` and `k8s-resolutions` tables) for full audit trail
- **3-level DynamoDB navigation** — Tables → Clusters → Runs drill-down for both investigation and resolution history
- **S3 bucket browser** — click into any bucket, navigate folders, inspect object metadata
- **Dark/light theme** across all pages

### Summary of changes since v3.0

| Version | Highlight |
|---------|-----------|
| v3.1 | Cluster health warn-badges and auto-refresh countdown |
| v3.2 | K8s Investigation Agent foundation — evidence collection layer |
| v3.3 | Live SSE investigation stream with per-stage progress |
| v3.4 | EKS commands reference page |
| v3.5 | K8s Resolution Engine — structured fix guide per finding |
| v3.6 | AI Reasoning Engine with confidence score on Resolution page |
| v3.7 | DynamoDB `k8s` table — investigation history per cluster |
| v3.8 | 3-level DynamoDB navigation (Tables → Clusters → Runs) |
| v3.9 | Resolution history — `k8s-resolutions` table with same 3-level nav |
| v3.10 | S3 drill-down — browse bucket objects and folders |

---

## [v3.10] — 2026-06-12

### S3 Bucket Drill-Down Navigation

- Bucket names are now clickable — click any bucket to enter it and browse contents
- Folder navigation using S3 `CommonPrefixes` (simulated folders via key delimiter `/`)
- Objects table: Name, Full Key, Size, Last Modified, Storage Class badge, ETag
- Breadcrumb bar shows full path — each segment is clickable for back-navigation
- Truncation notice with CLI equivalent when a folder contains > 500 objects
- New backend endpoint: `GET /api/s3/bucket/:name/objects?prefix=`

---

## [v3.9] — 2026-06-11

### Resolution History

- Each visit to the Resolve Issues page saves a resolution run to a new `k8s-resolutions` DynamoDB table
- History panel on the resolution page auto-opens showing past runs per cluster
- Columns: Timestamp, Namespace, Findings count, Severity, Confidence, Breakdown (C/H/W), AI Summary
- "View in DynamoDB" button navigates directly to the cluster's runs in the DynamoDB explorer
- DynamoDB page: `k8s-resolutions` is a first-class 3-level table (Tables → Clusters → Resolution Runs)
- Resolution run columns differ from investigation runs: shows critical/high/warning finding counts instead of pod/dep/event/net breakdown
- Bug fix: resolution history panel now auto-opens; save is awaited before loading history so the current run is always included

---

## [v3.8] — 2026-06-11

### 3-Level DynamoDB Navigation

- DynamoDB page rebuilt as a 3-level SPA: Tables → Clusters → Investigation Runs
- Clicking the `k8s` table shows a cluster card grid (using `/api/k8s/clusters`) instead of jumping straight to all items
- Each cluster card shows: run count, last severity badge, last run timestamp
- Clicking a cluster shows its investigation runs with full breakdown table
- Non-`k8s` tables still use direct 2-level navigation (table → items)
- Breadcrumb bar (`Tables / k8s / cluster-name`) with clickable segments at every level
- New backend endpoint: `GET /api/k8s/clusters` — scans `k8s` table, aggregates per-cluster stats

---

## [v3.7] — 2026-06-10

### DynamoDB Investigation History

- Every investigation run is automatically saved to a `k8s` DynamoDB table in LocalStack
- Table key: `cluster` (partition) + `run_id` (ISO timestamp sort key)
- Stored fields: issues count, severity breakdown (pods/deployments/events/network), AI severity, confidence, summary
- History panel on the K8s Agent page shows the last 50 runs per cluster, collapsible
- History rows link to the DynamoDB page at `?table=k8s&cluster=<name>` for deep inspection
- Backend: `ensureK8sTable()` auto-creates the table on first run, `GET /api/history/:ctx`, `GET /api/history/:ctx/:runId`

---

## [v3.6] — 2026-06-10

### AI Reasoning Engine with Confidence Score

- Rule-based AI reasoning engine runs as a hidden stage 6 after the 5-stage investigation
- Confidence score (0–100) based on corroborating evidence: CrashLoopBackOff +20, ImagePullBackOff +18, OOMKilled +15, log corroboration +15, events/deployments/network +8 each; errored stages −12 each
- Severity classification: healthy / info / warning / high / critical
- Root cause tags with affected resource names
- Reasoning steps rendered as a 2-column grid on the Resolution page
- AI Reasoning card is shown on the Resolution page only; invisible in the investigation runtime panel

---

## [v3.5] — 2026-06-10

### K8s Resolution Engine

- Dedicated `k8s-resolution.html` page — problem-by-problem fix guide
- For each finding: What is happening, Likely causes, How to fix it (numbered steps), Commands to run (click-to-copy), Log evidence
- Sidebar issue navigator and top progress pill strip for multi-finding navigation
- Resolution Knowledge Base covering: CrashLoopBackOff, ImagePullBackOff, ErrImagePull, OOMKilled, Pending, Error, Terminating, UnavailableReplicas, ProgressDeadlineExceeded, empty/missing endpoints, FailedScheduling, BackOff, Unhealthy
- Investigation payload passed via `sessionStorage` from the Agent page

---

## [v3.4] — 2026-06-09

### EKS Commands Reference

- New `kubectl-commands.html` reference page with organised kubectl command cards
- Categories: cluster info, pod management, deployment ops, log inspection, namespace management, config and context
- Click-to-copy on all command blocks

---

## [v3.3] — 2026-06-09

### Live Investigation Stream (SSE)

- Investigation now runs as a Server-Sent Events stream — the UI updates in real time as each stage completes
- 5 stages with animated progress: Pods → Logs → Events → Deployments → Network
- Each stage shows elapsed time, item count, and issue count on completion
- Error stages are tolerated — investigation continues and reports partial evidence
- "Resolve Issues" button appears when complete and passes the full payload to the resolution page

---

## [v3.2] — 2026-06-09

### K8s Investigation Agent Foundation

- Evidence-collection layer: pod status inspection, log scraping for error keywords, event analysis, deployment replica health, service endpoint/selector validation
- Rule engine classifies pod status (`CrashLoopBackOff`, `ImagePullBackOff`, `OOMKilled`, etc.)
- `POST /api/investigate` endpoint returns structured investigation JSON
- K8s Agent page (`k8s-agent.html`) with context/namespace selectors and results panel

---

## [v3.1] — 2026-06-09

### Cluster Health Indicators & Auto-Refresh

- Warn badge on the cluster tab when unhealthy pods or nodes are detected
- Auto-refresh with live countdown timer on all data pages (S3, DynamoDB, K8s)
- `apiServerPort: 6444` mapping added to `kind-cluster.yaml`

---

## [v3.0] — 2026-06-09

### kubectl Commands & K8s Dashboard Link

- `Open K8s Dashboard` button added to the kubectl commands hero section
- kubectl commands reference integrated into the K8s dashboard page

---

## [v2.x] — 2026-06-08

### Dashboard Foundation

| Version | Change |
|---------|--------|
| v2.1 | Native S3 and DynamoDB dashboard pages |
| v2.2 | Remove duplicate Open K8s Dashboard button |
| v2.3 | Dark/light theme toggle across all pages |
| v2.4 | Kubernetes split view with collapsible sections |
| v2.5 | Hide offline clusters by default with show/hide toggle |
| v2.6 | Live cluster status indicator with last-checked timestamp |
| v2.7 | Pod/node health highlighting with kubectl-style status strings |
| v2.8 | kubectl commands reference page |

---

## [v1.0] — Initial Release

- Floci stack: LocalStack S3/DynamoDB emulator + S3 Manager UI via Docker Compose
- Multi-cluster Kubernetes dashboard with live data
- Nginx reverse proxy eliminating CORS issues
