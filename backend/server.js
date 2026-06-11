const express = require('express');
const https   = require('https');
const fs      = require('fs');
const yaml    = require('js-yaml');
const { S3Client, ListBucketsCommand, ListObjectsV2Command, GetBucketLocationCommand } = require('@aws-sdk/client-s3');
const { DynamoDBClient, ListTablesCommand, DescribeTableCommand } = require('@aws-sdk/client-dynamodb');

const FLOCI_ENDPOINT = process.env.FLOCI_ENDPOINT || 'http://floci:4566';
const AWS_CREDS = { region: 'us-east-1', credentials: { accessKeyId: 'test', secretAccessKey: 'test' } };

const s3     = new S3Client({ ...AWS_CREDS, endpoint: FLOCI_ENDPOINT, forcePathStyle: true });
const dynamo = new DynamoDBClient({ ...AWS_CREDS, endpoint: FLOCI_ENDPOINT });

const app  = express();
const PORT = 3000;
const TIMEOUT = 6000;

app.use(express.json());
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  next();
});

function readKubeconfig() {
  const path = process.env.KUBECONFIG || '/root/.kube/config';
  return yaml.load(fs.readFileSync(path, 'utf8'));
}

function request(url, opts, timeout) {
  return new Promise((resolve, reject) => {
    const req = https.request(url, opts, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => { try { resolve(JSON.parse(data)); } catch(e) { reject(new Error('bad json')); } });
    });
    req.setTimeout(timeout || TIMEOUT, () => { req.destroy(); reject(new Error('timeout')); });
    req.on('error', reject);
    req.end();
  });
}

function buildClient(contextName, kc) {
  const ctx = kc.contexts.find(c => c.name === contextName)?.context;
  if (!ctx) throw new Error('Context not found');
  const cluster = kc.clusters.find(c => c.name === ctx.cluster)?.cluster;
  const user    = kc.users.find(u => u.name === ctx.user)?.user || {};

  const agentOpts = { rejectUnauthorized: false };
  if (cluster['certificate-authority-data'])
    agentOpts.ca = Buffer.from(cluster['certificate-authority-data'], 'base64');

  const headers = { 'Content-Type': 'application/json' };
  if (user.token) {
    headers['Authorization'] = `Bearer ${user.token}`;
  } else if (user['client-certificate-data']) {
    agentOpts.cert = Buffer.from(user['client-certificate-data'], 'base64');
    agentOpts.key  = Buffer.from(user['client-key-data'], 'base64');
  } else {
    throw new Error('exec-auth not supported (cloud cluster)');
  }

  // From inside Docker on Windows, 127.0.0.1 resolves to the container itself.
  // Remap to host.docker.internal so kind cluster API servers are reachable.
  const server = cluster.server.replace('https://127.0.0.1', 'https://host.docker.internal');
  return { server, agentOpts, headers };
}

async function fetchK8s(contextName, path) {
  const kc = readKubeconfig();
  const { server, agentOpts, headers } = buildClient(contextName, kc);
  return request(server + path, { method: 'GET', headers, agent: new https.Agent(agentOpts) });
}

const RESOURCE_PATHS = {
  version:     '/version',
  nodes:       '/api/v1/nodes',
  pods:        '/api/v1/pods',
  services:    '/api/v1/services',
  namespaces:  '/api/v1/namespaces',
  deployments: '/apis/apps/v1/deployments',
};

// List all contexts
app.get('/api/contexts', (req, res) => {
  try {
    const kc = readKubeconfig();
    const current = kc['current-context'];
    res.json({
      current,
      contexts: kc.contexts.map(c => ({
        name:    c.name,
        cluster: c.context.cluster,
        current: c.name === current,
      }))
    });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// Fetch a resource for a given context
app.get('/api/cluster/:context/:resource', async (req, res) => {
  const { resource } = req.params;
  const context = decodeURIComponent(req.params.context);
  const path = RESOURCE_PATHS[resource];
  if (!path) return res.status(400).json({ error: 'unknown resource' });
  try {
    const data = await fetchK8s(context, path);
    res.json(data);
  } catch(e) {
    res.status(503).json({ error: e.message, offline: true });
  }
});

// ── Investigation helpers ─────────────────────────────────────────────────────

function requestText(url, opts, timeout) {
  return new Promise((resolve, reject) => {
    const req = https.request(url, opts, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve(data));
    });
    req.setTimeout(timeout || TIMEOUT, () => { req.destroy(); reject(new Error('timeout')); });
    req.on('error', reject);
    req.end();
  });
}

async function fetchK8sText(contextName, path) {
  const kc = readKubeconfig();
  const { server, agentOpts, headers } = buildClient(contextName, kc);
  return requestText(server + path, { method: 'GET', headers, agent: new https.Agent(agentOpts) });
}

const UNHEALTHY_REASONS = new Set([
  'CrashLoopBackOff','ImagePullBackOff','ErrImagePull','Error','OOMKilled',
  'ContainerCreating','InvalidImageName','CreateContainerConfigError',
  'CreateContainerError','RunContainerError',
]);
const WARNING_REASONS = new Set([
  'FailedScheduling','BackOff','FailedMount','FailedPull','ErrImagePull',
  'Unhealthy','FailedCreate','OOMKilling','NodeNotReady','NetworkNotReady',
  'FailedAttachVolume','Killing',
]);
const ERROR_KW = ['error','exception','fatal','panic','fail','crash','refused',
  'timeout','oomkilled','killed','no such file','permission denied','traceback',
  'image pull','back-off','backoff','missing','not found'];

function podStatusStr(pod) {
  const meta = pod.metadata || {};
  const status = pod.status || {};
  const spec = pod.spec || {};
  if (meta.deletionTimestamp) return 'Terminating';
  for (let i = 0; i < (status.initContainerStatuses || []).length; i++) {
    const ic = status.initContainerStatuses[i];
    const w = (ic.state || {}).waiting || {};
    if (w.reason) return `Init:${w.reason}`;
    if (!ic.ready) return `Init:${i}/${(spec.initContainers || []).length}`;
  }
  for (const cs of (status.containerStatuses || [])) {
    const w = (cs.state || {}).waiting || {};
    const t = (cs.state || {}).terminated || {};
    if (w.reason) return w.reason;
    if (t.reason) return t.reason;
    if ((t.exitCode || 0) !== 0) return 'Error';
  }
  return status.phase || 'Unknown';
}

function isPodUnhealthy(statusStr, phase) {
  if (UNHEALTHY_REASONS.has(statusStr)) return true;
  if (phase === 'Failed' || phase === 'Unknown') return true;
  if (statusStr.startsWith('Init:') && statusStr.includes('/')) return true;
  return false;
}

function inspectPods(podsData, nsFilter) {
  const items = (podsData.items || []).filter(p =>
    !nsFilter || p.metadata.namespace === nsFilter
  );
  const allPods = [], problematic = [];
  for (const pod of items) {
    const name = pod.metadata.name;
    const ns   = pod.metadata.namespace;
    const phase = (pod.status || {}).phase || 'Unknown';
    const statusStr = podStatusStr(pod);
    const restarts = ((pod.status || {}).containerStatuses || [])
      .reduce((s, cs) => s + (cs.restartCount || 0), 0);
    const ready = ((pod.status || {}).containerStatuses || []).filter(c => c.ready).length;
    const total = ((pod.spec || {}).containers || []).length;
    const info = { name, namespace: ns, status: statusStr, phase, ready: `${ready}/${total}`, restarts, node: (pod.spec || {}).nodeName };
    allPods.push(info);
    if (isPodUnhealthy(statusStr, phase)) problematic.push({ name, namespace: ns, status: statusStr, phase, restarts });
  }
  return { healthy: problematic.length === 0, total: allPods.length, problematic_pods: problematic, all_pods: allPods };
}

function inspectDeployments(depsData, nsFilter) {
  const items = (depsData.items || []).filter(d =>
    !nsFilter || d.metadata.namespace === nsFilter
  );
  const allDeps = [], unhealthy = [];
  for (const dep of items) {
    const name = dep.metadata.name;
    const ns   = dep.metadata.namespace;
    const spec = dep.spec || {};
    const st   = dep.status || {};
    const desired   = spec.replicas || 0;
    const ready     = st.readyReplicas || 0;
    const available = st.availableReplicas || 0;
    const unavail   = st.unavailableReplicas || 0;
    const isHealthy = desired > 0 ? available >= desired && unavail === 0 : true;
    const conditions = (st.conditions || []).map(c => ({ type: c.type, status: c.status, reason: c.reason, message: (c.message||'').substring(0,200) }));
    const info = { name, namespace: ns, desired, ready, available, healthy: isHealthy, conditions };
    allDeps.push(info);
    if (!isHealthy) unhealthy.push(info);
  }
  return { healthy: unhealthy.length === 0, total: allDeps.length, unhealthy_deployments: unhealthy, all_deployments: allDeps };
}

function analyzeEvents(eventsData, nsFilter) {
  const items = (eventsData.items || []).filter(e =>
    !nsFilter || (e.metadata || {}).namespace === nsFilter
  );
  const warns = [];
  let normalCount = 0;
  for (const ev of items) {
    const type   = ev.type || 'Normal';
    const reason = ev.reason || '';
    if (type === 'Warning' || WARNING_REASONS.has(reason)) {
      warns.push({
        namespace:   (ev.metadata || {}).namespace,
        reason,
        message:     (ev.message || '').substring(0, 300),
        object_kind: (ev.involvedObject || {}).kind,
        object_name: (ev.involvedObject || {}).name,
        count:       ev.count || 1,
        last_time:   ev.lastTimestamp || ev.eventTime || '',
      });
    } else {
      normalCount++;
    }
  }
  warns.sort((a, b) => b.count - a.count);
  return { healthy: warns.length === 0, total: items.length, warning_count: warns.length, warning_events: warns.slice(0, 100), normal_count: normalCount };
}

function inspectNetwork(svcData, epData, nsFilter) {
  const svcs = (svcData.items || []).filter(s => !nsFilter || s.metadata.namespace === nsFilter);
  const epMap = {};
  for (const ep of (epData.items || [])) {
    epMap[`${ep.metadata.namespace}/${ep.metadata.name}`] = ep;
  }
  const allSvcs = [], issues = [];
  for (const svc of svcs) {
    const name = svc.metadata.name;
    const ns   = svc.metadata.namespace;
    const type = (svc.spec || {}).type || 'ClusterIP';
    const selector = (svc.spec || {}).selector || {};
    const epItem = epMap[`${ns}/${name}`];
    let flag = 'ok';
    if (type === 'ExternalName') flag = 'external-name';
    else if (!Object.keys(selector).length) flag = 'no-selector';
    else if (!epItem) flag = 'missing-endpoints';
    else {
      const subsets = epItem.subsets || [];
      const hasAddr = subsets.some(s => (s.addresses || []).length > 0);
      if (!hasAddr) flag = 'empty-endpoints';
    }
    const info = { name, namespace: ns, type, selector, flag };
    allSvcs.push(info);
    if (flag === 'missing-endpoints' || flag === 'empty-endpoints') {
      issues.push({ name, namespace: ns, type, issue: flag, selector });
    }
  }
  return { healthy: issues.length === 0, total: allSvcs.length, issue_count: issues.length, issues, all_services: allSvcs };
}

async function collectLogs(contextName, problematicPods) {
  const result = {};
  const errorKwRe = new RegExp(ERROR_KW.join('|'), 'i');
  for (const pod of problematicPods.slice(0, 5)) { // cap at 5 pods
    const key = `${pod.namespace}/${pod.name}`;
    try {
      const raw = await fetchK8sText(contextName, `/api/v1/namespaces/${pod.namespace}/pods/${pod.name}/log?tailLines=100`);
      const lines = raw.split('\n').filter(Boolean);
      const errorLines = lines.filter(l => errorKwRe.test(l));
      result[key] = { pod: pod.name, namespace: pod.namespace, lines_fetched: lines.length, error_lines: errorLines.slice(0, 50), error_count: errorLines.length };
    } catch(e) {
      result[key] = { pod: pod.name, namespace: pod.namespace, lines_fetched: 0, error_lines: [], error_count: 0, fetch_error: e.message };
    }
  }
  return result;
}

// POST /api/investigate
app.post('/api/investigate', async (req, res) => {
  const { context: ctx, namespace: ns } = req.body || {};
  if (!ctx) return res.status(400).json({ error: 'context is required' });

  const nsPath = ns ? `/namespaces/${ns}` : '';
  const safe   = encodeURIComponent(ctx);

  try {
    // Fetch all resources in parallel; tolerate individual failures
    const [podsR, depsR, svcR, epR, evR] = await Promise.allSettled([
      fetchK8s(ctx, ns ? `/api/v1/namespaces/${ns}/pods`        : '/api/v1/pods'),
      fetchK8s(ctx, ns ? `/apis/apps/v1/namespaces/${ns}/deployments` : '/apis/apps/v1/deployments'),
      fetchK8s(ctx, ns ? `/api/v1/namespaces/${ns}/services`    : '/api/v1/services'),
      fetchK8s(ctx, ns ? `/api/v1/namespaces/${ns}/endpoints`   : '/api/v1/endpoints'),
      fetchK8s(ctx, ns ? `/api/v1/namespaces/${ns}/events`      : '/api/v1/events'),
    ]);

    const podsData  = podsR.status  === 'fulfilled' ? podsR.value  : { items: [], error: podsR.reason?.message };
    const depsData  = depsR.status  === 'fulfilled' ? depsR.value  : { items: [], error: depsR.reason?.message };
    const svcData   = svcR.status   === 'fulfilled' ? svcR.value   : { items: [] };
    const epData    = epR.status    === 'fulfilled' ? epR.value    : { items: [] };
    const evData    = evR.status    === 'fulfilled' ? evR.value    : { items: [] };

    const podResult  = inspectPods(podsData, null);
    const depResult  = inspectDeployments(depsData, null);
    const evResult   = analyzeEvents(evData, null);
    const netResult  = inspectNetwork(svcData, epData, null);
    const logsResult = await collectLogs(ctx, podResult.problematic_pods);

    if (podsData.error) podResult.error = podsData.error;

    const badCount = podResult.problematic_pods.length + depResult.unhealthy_deployments.length +
                     evResult.warning_count + netResult.issue_count;

    res.json({
      status:    'success',
      context:   ctx,
      namespace: ns || null,
      investigation: {
        pods:        podResult,
        logs:        logsResult,
        events:      evResult,
        deployments: depResult,
        network:     netResult,
      }
    });
  } catch(e) {
    res.status(503).json({ status: 'error', error: e.message });
  }
});

// ── Reasoning / Resolution engine ────────────────────────────────────────────
const RESOLUTION_KB = {
  // Pod problems
  CrashLoopBackOff: {
    severity: 'critical',
    title:    'Container is crash-looping',
    what:     'The container starts, crashes immediately, and Kubernetes keeps restarting it with exponential back-off. This means the process inside the container exits with a non-zero code shortly after launch.',
    causes:   ['Application exception or panic at startup','Missing required environment variable or config','Wrong container command / entrypoint','Port already in use inside the pod','Out-of-memory on startup'],
    solution: ['Check the pod logs for the last crash: kubectl logs <pod> --previous -n <ns>','Describe the pod to see exit codes: kubectl describe pod <pod> -n <ns>','Verify all required environment variables are set in the Deployment spec','Check resource limits — if memory limit is too low the OOM killer will terminate it','Confirm the container image runs correctly locally: docker run <image>'],
    commands: ['kubectl logs {pod} --previous -n {ns}','kubectl describe pod {pod} -n {ns}','kubectl get events -n {ns} --field-selector involvedObject.name={pod}'],
  },
  ImagePullBackOff: {
    severity: 'critical',
    title:    'Cannot pull container image',
    what:     'Kubernetes tried to pull the container image from the registry but failed, and is now backing off before retrying. The pod will stay in Pending state until this is resolved.',
    causes:   ['Image name or tag is misspelled','Image does not exist in the registry','Registry requires authentication (imagePullSecret missing)','Network cannot reach the registry','Private registry URL is wrong'],
    solution: ['Verify the image name and tag exist: docker pull <image>','Check the image name in the Deployment spec for typos','Add an imagePullSecret if the registry is private','Confirm the node has network access to the registry','Check events: kubectl describe pod <pod> -n <ns>'],
    commands: ['kubectl describe pod {pod} -n {ns}','kubectl get events -n {ns} --field-selector involvedObject.name={pod}'],
  },
  ErrImagePull: {
    severity: 'critical',
    title:    'Image pull error (first attempt)',
    what:     'The first attempt to pull the container image failed. Kubernetes will transition to ImagePullBackOff if this persists.',
    causes:   ['Same as ImagePullBackOff — wrong image, missing credentials, or network issue'],
    solution: ['Same steps as ImagePullBackOff — fix the image reference or add imagePullSecret'],
    commands: ['kubectl describe pod {pod} -n {ns}'],
  },
  OOMKilled: {
    severity: 'critical',
    title:    'Container killed by Out-Of-Memory',
    what:     'The Linux kernel OOM killer terminated the container because it exceeded its memory limit. Kubernetes will restart it, often leading to CrashLoopBackOff.',
    causes:   ['Memory limit set too low for the workload','Memory leak in the application','Sudden traffic spike causing memory spike'],
    solution: ['Increase the memory limit in the Deployment resources spec','Profile the application for memory leaks','Set a Horizontal Pod Autoscaler so more replicas share the load','Add memory-based alerting to catch this before it becomes critical'],
    commands: ['kubectl describe pod {pod} -n {ns}','kubectl top pod {pod} -n {ns}'],
  },
  Pending: {
    severity: 'warning',
    title:    'Pod is stuck in Pending',
    what:     'The pod has been accepted by the scheduler but no node has been assigned yet. It is waiting for resources or a scheduling condition to be met.',
    causes:   ['Insufficient CPU or memory on all nodes','Node selector or affinity rules cannot be satisfied','PersistentVolumeClaim not bound','Taint on all nodes with no matching toleration'],
    solution: ['Check events for the scheduling reason: kubectl describe pod <pod> -n <ns>','Check node capacity: kubectl describe nodes | grep -A5 Allocated','Relax resource requests if they are too high','Check if a PVC is unbound: kubectl get pvc -n <ns>','Add tolerations if nodes are tainted'],
    commands: ['kubectl describe pod {pod} -n {ns}','kubectl get nodes -o wide','kubectl get pvc -n {ns}'],
  },
  Error: {
    severity: 'high',
    title:    'Container exited with error',
    what:     'The container process exited with a non-zero exit code, indicating failure. This is different from CrashLoopBackOff — the container ran and then failed rather than failing immediately on startup.',
    causes:   ['Application runtime error','Script or job returned a failure exit code','Dependency (database, API) not reachable'],
    solution: ['Read the pod logs to find the error: kubectl logs <pod> -n <ns>','Check exit code in kubectl describe pod output','Verify all downstream dependencies are reachable from inside the pod'],
    commands: ['kubectl logs {pod} -n {ns}','kubectl describe pod {pod} -n {ns}'],
  },
  Terminating: {
    severity: 'info',
    title:    'Pod is being terminated',
    what:     'The pod has received a deletion signal and is in the process of shutting down. This is usually normal unless it has been stuck in Terminating for a long time.',
    causes:   ['Normal rolling update or scale-down','Stuck finalizer preventing deletion','Node went offline while pod was running'],
    solution: ['If stuck for >5 minutes, force-delete: kubectl delete pod <pod> -n <ns> --grace-period=0 --force','Check for finalizers: kubectl get pod <pod> -n <ns> -o jsonpath=\'{.metadata.finalizers}\''],
    commands: ['kubectl delete pod {pod} -n {ns} --grace-period=0 --force'],
  },
  // Deployment problems
  UnavailableReplicas: {
    severity: 'high',
    title:    'Deployment has unavailable replicas',
    what:     'One or more desired replicas are not ready. The deployment is degraded — traffic may be served by fewer instances than expected, or not at all.',
    causes:   ['Pod crash-looping (see pod issues above)','Image pull failure','Insufficient cluster resources to schedule new pods','Readiness probe failing'],
    solution: ['Check pods in the deployment: kubectl get pods -n <ns> -l app=<name>','Look at pod logs and events for the root cause','Verify readiness probe path and port in the Deployment spec','Scale up nodes if resource pressure is the cause'],
    commands: ['kubectl get pods -n {ns}','kubectl describe deployment {dep} -n {ns}','kubectl rollout status deployment/{dep} -n {ns}'],
  },
  ProgressDeadlineExceeded: {
    severity: 'critical',
    title:    'Rollout deadline exceeded',
    what:     'A rolling update started but did not complete within the progressDeadlineSeconds window (default 600s). The new version pods are not becoming ready.',
    causes:   ['New image fails to start (crash or pull error)','Readiness probe never passing on new pods','Insufficient resources to run old and new pods simultaneously'],
    solution: ['Roll back immediately: kubectl rollout undo deployment/<name> -n <ns>','Check the new pods: kubectl describe pod <new-pod> -n <ns>','Fix the root cause then re-deploy','Consider reducing maxUnavailable / maxSurge in the rollout strategy'],
    commands: ['kubectl rollout undo deployment/{dep} -n {ns}','kubectl rollout status deployment/{dep} -n {ns}'],
  },
  // Network problems
  'empty-endpoints': {
    severity: 'high',
    title:    'Service has no matching pod endpoints',
    what:     'The Service exists and has a selector, but no running pods match that selector. Traffic sent to this Service will get no response (connection refused or timeout).',
    causes:   ['All pods for this service are down or not Ready','Label selector in the Service does not match pod labels','Pods are in a different namespace than the Service'],
    solution: ['Check the pods the service should select: kubectl get pods -n <ns> -l <selector>','Compare service selector with pod labels: kubectl describe svc <name> -n <ns>','Ensure pods are Running and Ready','Fix label mismatch in Service spec or Deployment template'],
    commands: ['kubectl describe svc {svc} -n {ns}','kubectl get pods -n {ns} --show-labels','kubectl get endpoints {svc} -n {ns}'],
  },
  'missing-endpoints': {
    severity: 'high',
    title:    'Service has no endpoints object',
    what:     'There is no Endpoints resource for this Service at all. This usually means the Service was just created or the pods were never created.',
    causes:   ['Deployment for this service was never created','Namespace mismatch between Service and pods','Endpoints controller not reconciling'],
    solution: ['Create the backing Deployment/pods for this Service','Verify namespace: kubectl get all -n <ns>','Check if the endpoints object exists: kubectl get endpoints -n <ns>'],
    commands: ['kubectl get endpoints -n {ns}','kubectl get pods -n {ns} --show-labels'],
  },
  // Events
  FailedScheduling: {
    severity: 'high',
    title:    'Pod cannot be scheduled onto any node',
    what:     'The Kubernetes scheduler tried all nodes and could not find one that satisfies the pod\'s requirements. The pod will remain Pending.',
    causes:   ['All nodes lack sufficient CPU or memory','Node selector / affinity rules not satisfied','All nodes are tainted and pod has no matching toleration','Pod disruption budget blocking eviction'],
    solution: ['kubectl describe pod <pod> -n <ns> — look at Events section for exact reason','Add more nodes or increase node size','Reduce resource requests in the pod spec','Add node tolerations if nodes are tainted'],
    commands: ['kubectl describe pod {pod} -n {ns}','kubectl get nodes -o custom-columns=NAME:.metadata.name,CPU:.status.allocatable.cpu,MEM:.status.allocatable.memory'],
  },
  BackOff: {
    severity: 'warning',
    title:    'Kubernetes is backing off restarting container',
    what:     'The container keeps crashing and Kubernetes is applying exponential back-off before each restart attempt. Restart intervals grow: 10s → 20s → 40s → 80s → 160s → 300s (max).',
    causes:   ['Application panics or exits immediately on startup','Missing config or environment variable','See CrashLoopBackOff causes'],
    solution: ['Same as CrashLoopBackOff — fix the application startup error','Check kubectl logs <pod> --previous -n <ns>'],
    commands: ['kubectl logs {pod} --previous -n {ns}','kubectl describe pod {pod} -n {ns}'],
  },
  Unhealthy: {
    severity: 'warning',
    title:    'Liveness or readiness probe failing',
    what:     'The kubelet ran the probe (HTTP, TCP, or exec) and it failed. A failing liveness probe causes the container to be restarted; a failing readiness probe removes the pod from Service endpoints.',
    causes:   ['Application not yet started when probe fires (startupProbe missing)','Wrong probe port or path configured','Application is slow and probe times out','Application has a health-check bug'],
    solution: ['Check probe configuration: kubectl describe pod <pod> -n <ns>','Add initialDelaySeconds to give the app time to start','Increase timeoutSeconds and failureThreshold for slow apps','Test the probe endpoint manually from inside the pod: kubectl exec <pod> -n <ns> -- curl localhost:<port>/health'],
    commands: ['kubectl describe pod {pod} -n {ns}','kubectl exec {pod} -n {ns} -- wget -qO- localhost:8080/health'],
  },
};

function resolveInvestigation(investigation) {
  const findings = [];
  const inv = investigation || {};
  const pods        = inv.pods        || {};
  const events      = inv.events      || {};
  const deployments = inv.deployments || {};
  const network     = inv.network     || {};
  const logs        = inv.logs        || {};

  // ── Pod problems ──────────────────────────────────────────────────────────
  for (const pod of (pods.problematic_pods || [])) {
    const kb = RESOLUTION_KB[pod.status] || {
      severity: 'warning',
      title:    `Pod status: ${pod.status}`,
      what:     `The pod is in an unexpected state: ${pod.status}.`,
      causes:   ['Check pod events and logs for more details'],
      solution: ['kubectl describe pod ' + pod.name + ' -n ' + pod.namespace],
      commands: ['kubectl describe pod {pod} -n {ns}','kubectl logs {pod} -n {ns}'],
    };
    const podLogs = logs[`${pod.namespace}/${pod.name}`];
    const errorLines = podLogs ? podLogs.error_lines || [] : [];
    findings.push({
      id:         `pod-${pod.namespace}-${pod.name}`,
      type:       'pod',
      severity:   kb.severity,
      subject:    `${pod.namespace} / ${pod.name}`,
      status:     pod.status,
      restarts:   pod.restarts,
      title:      kb.title,
      what:       kb.what,
      causes:     kb.causes,
      solution:   kb.solution,
      commands:   kb.commands.map(c => c.replace('{pod}', pod.name).replace('{ns}', pod.namespace)),
      evidence:   errorLines.slice(0, 10),
    });
  }

  // ── Deployment problems ───────────────────────────────────────────────────
  for (const dep of (deployments.unhealthy_deployments || [])) {
    const deadlineCondition = (dep.conditions || []).find(c => c.reason === 'ProgressDeadlineExceeded');
    const key = deadlineCondition ? 'ProgressDeadlineExceeded' : 'UnavailableReplicas';
    const kb  = RESOLUTION_KB[key];
    findings.push({
      id:       `dep-${dep.namespace}-${dep.name}`,
      type:     'deployment',
      severity: kb.severity,
      subject:  `${dep.namespace} / ${dep.name}`,
      status:   `${dep.ready}/${dep.desired} ready`,
      title:    kb.title,
      what:     kb.what,
      causes:   kb.causes,
      solution: kb.solution,
      commands: kb.commands.map(c => c.replace(/{dep}/g, dep.name).replace(/{ns}/g, dep.namespace)),
      evidence: [],
    });
  }

  // ── Network problems ──────────────────────────────────────────────────────
  for (const issue of (network.issues || [])) {
    const kb = RESOLUTION_KB[issue.issue] || { severity:'warning', title: issue.issue, what:'', causes:[], solution:[], commands:[] };
    findings.push({
      id:       `net-${issue.namespace}-${issue.name}`,
      type:     'network',
      severity: kb.severity,
      subject:  `${issue.namespace} / ${issue.name}`,
      status:   issue.issue,
      title:    kb.title,
      what:     kb.what,
      causes:   kb.causes,
      solution: kb.solution,
      commands: kb.commands.map(c => c.replace(/{svc}/g, issue.name).replace(/{ns}/g, issue.namespace)),
      evidence: [],
    });
  }

  // ── Warning events (top 5 unique reasons) ────────────────────────────────
  const seenReasons = new Set();
  for (const ev of (events.warning_events || []).slice(0, 10)) {
    if (seenReasons.has(ev.reason)) continue;
    seenReasons.add(ev.reason);
    const kb = RESOLUTION_KB[ev.reason];
    if (!kb) continue;
    findings.push({
      id:       `ev-${ev.reason}-${ev.object_name}`,
      type:     'event',
      severity: kb.severity,
      subject:  `${ev.namespace} / ${ev.object_name} (${ev.object_kind})`,
      status:   ev.reason,
      title:    kb.title,
      what:     kb.what,
      causes:   kb.causes,
      solution: kb.solution,
      commands: kb.commands.map(c => c.replace(/{pod}/g, ev.object_name).replace(/{ns}/g, ev.namespace || 'default')),
      evidence: [ev.message],
    });
  }

  // Sort: critical first
  const order = { critical:0, high:1, warning:2, info:3 };
  findings.sort((a,b) => (order[a.severity]||9) - (order[b.severity]||9));

  return { total: findings.length, findings };
}

// ── AI Reasoning Engine ────────────────────────────────────────────────────────
function reasonAboutInvestigation(inv) {
  const pods    = inv.pods    || {};
  const logs    = inv.logs    || {};
  const events  = inv.events  || {};
  const deps    = inv.deployments || {};
  const net     = inv.network || {};

  const badPods   = pods.problematic_pods      || [];
  const badDeps   = deps.unhealthy_deployments || [];
  const warnEvts  = events.warning_events      || [];
  const netIssues = net.issues                 || [];
  const logKeys   = Object.keys(logs);

  const steps = [];
  const root_causes = [];
  let confidence = 50;
  let severity   = 'info';

  // Penalise for errored stages
  const erroredStages = [pods.error && !badPods.length, deps.error, events.error, net.error]
    .filter(Boolean).length;
  if (erroredStages > 0) {
    confidence -= erroredStages * 12;
    steps.push(`⚠ ${erroredStages} stage(s) had errors — evidence may be incomplete`);
  }

  const totalIssues = badPods.length + badDeps.length +
                      (events.warning_count || 0) + (net.issue_count || 0);

  // Healthy cluster
  if (totalIssues === 0 && erroredStages === 0) {
    return {
      summary: 'Cluster appears healthy — no issues detected.',
      confidence: 95,
      severity: 'healthy',
      root_causes: [],
      reasoning_steps: [
        '✓ All 5 investigation stages completed without errors',
        '✓ No unhealthy pods, deployments, warning events, or network issues found',
        '→ Cluster is operating normally'
      ]
    };
  }

  // Analyse pods
  if (badPods.length > 0) {
    confidence += 10;
    steps.push(`✓ Pod analysis: ${badPods.length} unhealthy pod(s) found`);

    const clb  = badPods.filter(p => p.reason === 'CrashLoopBackOff');
    const ipb  = badPods.filter(p => p.reason === 'ImagePullBackOff' || p.reason === 'ErrImagePull');
    const oom  = badPods.filter(p => p.reason === 'OOMKilled');
    const pend = badPods.filter(p => p.reason === 'Pending');
    const err  = badPods.filter(p => p.reason === 'Error');

    if (clb.length > 0) {
      confidence += 20; severity = 'critical';
      const names = clb.map(p => p.name).join(', ');
      root_causes.push({ type:'CrashLoopBackOff', pods:names, severity:'critical' });
      steps.push(`→ CrashLoopBackOff in ${clb.length} pod(s): ${names}`);
      steps.push('→ Pod is repeatedly crashing — likely startup error, bad config, or missing dependency');
    }
    if (ipb.length > 0) {
      confidence += 18; if (severity !== 'critical') severity = 'high';
      const names = ipb.map(p => p.name).join(', ');
      root_causes.push({ type:'ImagePullBackOff', pods:names, severity:'high' });
      steps.push(`→ ImagePullBackOff in ${ipb.length} pod(s): ${names}`);
      steps.push('→ Cannot pull container image — check image tag, registry URL, and pull secrets');
    }
    if (oom.length > 0) {
      confidence += 15; if (severity !== 'critical') severity = 'high';
      const names = oom.map(p => p.name).join(', ');
      root_causes.push({ type:'OOMKilled', pods:names, severity:'high' });
      steps.push(`→ OOMKilled in ${oom.length} pod(s): ${names}`);
      steps.push('→ Container exceeded memory limit — increase memory requests/limits');
    }
    if (pend.length > 0) {
      if (severity === 'info') severity = 'warning';
      const names = pend.map(p => p.name).join(', ');
      root_causes.push({ type:'Pending', pods:names, severity:'warning' });
      steps.push(`→ ${pend.length} pod(s) stuck Pending: ${names} — check node resources or taints`);
    }
    if (err.length > 0) {
      if (severity === 'info') severity = 'warning';
      const names = err.map(p => p.name).join(', ');
      root_causes.push({ type:'Error', pods:names, severity:'warning' });
      steps.push(`→ ${err.length} pod(s) in Error state: ${names}`);
    }
  } else {
    steps.push('✓ Pod analysis: all pods healthy');
    confidence += 5;
  }

  // Correlate with logs
  const logsWithErrors = logKeys.filter(k => (logs[k].error_count || 0) > 0);
  if (logsWithErrors.length > 0) {
    confidence += 15;
    steps.push(`✓ Log correlation: error lines found in ${logsWithErrors.length} pod(s) — corroborates findings`);
    let hasOOM = false, hasMissingEnv = false, hasConnRefused = false;
    logsWithErrors.forEach(k => {
      const text = (logs[k].error_lines || []).join(' ').toLowerCase();
      if (text.includes('oom') || text.includes('memory'))                       hasOOM = true;
      if (text.includes('environment') || text.includes('env var') ||
          text.includes('configmap') || text.includes('secret'))                 hasMissingEnv = true;
      if (text.includes('connection refused') || text.includes('dial tcp') ||
          text.includes('timeout'))                                               hasConnRefused = true;
    });
    if (hasOOM)          { confidence += 5; steps.push('→ Log evidence: OOM/memory errors reinforce OOMKilled diagnosis'); }
    if (hasMissingEnv)   { confidence += 5; steps.push('→ Log evidence: missing env/config errors suggest bad ConfigMap or Secret'); }
    if (hasConnRefused)  { confidence += 3; steps.push('→ Log evidence: connection errors suggest service or DNS issue'); }
  } else if (badPods.length > 0 && logKeys.length === 0) {
    confidence -= 5;
    steps.push('⚠ Log collection skipped — confidence slightly reduced');
  }

  // Deployments
  if (badDeps.length > 0) {
    confidence += 8; if (severity === 'info') severity = 'warning';
    const names = badDeps.map(d => d.name).slice(0, 3).join(', ');
    root_causes.push({ type:'UnhealthyDeployment', deployments:names, severity:'high' });
    steps.push(`✓ Deployment analysis: ${badDeps.length} unhealthy — ${names}${badDeps.length > 3 ? ' …' : ''}`);
    steps.push('→ Replica count below desired — pods may be failing to start');
  } else {
    steps.push('✓ Deployment analysis: all deployments healthy');
    confidence += 3;
  }

  // Events
  if (warnEvts.length > 0) {
    confidence += 8;
    const reasons = [...new Set(warnEvts.map(e => e.reason))].slice(0, 3).join(', ');
    steps.push(`✓ Events: ${warnEvts.length} warning event(s) — reasons: ${reasons}`);
    if (reasons.includes('FailedScheduling')) {
      steps.push('→ FailedScheduling: cluster may lack resources (CPU/memory/nodes)');
      root_causes.push({ type:'FailedScheduling', severity:'warning' });
    }
    if (reasons.includes('BackOff')) {
      confidence += 5;
      steps.push('→ BackOff events corroborate pod restart issues');
    }
    if (reasons.includes('FailedMount')) {
      steps.push('→ FailedMount: PersistentVolume or ConfigMap/Secret may be missing');
      root_causes.push({ type:'FailedMount', severity:'high' });
    }
  }

  // Network
  const emptyEp  = netIssues.filter(i => i.issue === 'empty-endpoints');
  const missingEp = netIssues.filter(i => i.issue === 'missing-endpoints');
  if (emptyEp.length > 0) {
    confidence += 8; if (severity === 'info') severity = 'warning';
    const names = emptyEp.map(i => i.name).join(', ');
    root_causes.push({ type:'EmptyEndpoints', services:names, severity:'warning' });
    steps.push(`✓ Network: ${emptyEp.length} service(s) with no endpoints — ${names}`);
    steps.push('→ Selector mismatch or all backing pods are unhealthy/not ready');
  }
  if (missingEp.length > 0) {
    const names = missingEp.map(i => i.name).join(', ');
    root_causes.push({ type:'MissingEndpoints', services:names, severity:'warning' });
    steps.push(`✓ Network: ${missingEp.length} service(s) missing endpoint objects — ${names}`);
  }

  // Final confidence cap and summary
  confidence = Math.max(10, Math.min(97, confidence));
  if (root_causes.length === 0 && totalIssues > 0) {
    confidence = Math.min(confidence, 50);
    steps.push('→ Issues detected but specific root cause unclear — manual inspection recommended');
  }

  const top = root_causes[0];
  let summary;
  if (!top) {
    summary = `${totalIssues} issue(s) detected — root cause unclear, manual review needed`;
  } else if (top.type === 'CrashLoopBackOff') {
    summary = `CrashLoopBackOff in ${top.pods} — container crashing repeatedly`;
  } else if (top.type === 'ImagePullBackOff') {
    summary = `ImagePullBackOff in ${top.pods} — invalid image or registry access denied`;
  } else if (top.type === 'OOMKilled') {
    summary = `OOMKilled in ${top.pods} — container exceeded memory limits`;
  } else if (top.type === 'EmptyEndpoints') {
    summary = `Service endpoints empty for ${top.services} — likely selector mismatch`;
  } else if (top.type === 'UnhealthyDeployment') {
    summary = `Unhealthy deployment(s): ${top.deployments} — replicas below desired count`;
  } else if (top.type === 'Pending') {
    summary = `Pods stuck Pending: ${top.pods} — insufficient resources or scheduling constraint`;
  } else {
    summary = `${root_causes.length} issue(s) identified — see root causes for details`;
  }

  if (severity === 'info' && totalIssues > 0) severity = 'warning';

  return { summary, confidence, severity, root_causes, reasoning_steps: steps };
}

// GET /api/investigate/stream  — Server-Sent Events, one event per stage
app.get('/api/investigate/stream', async (req, res) => {
  const ctx = req.query.context;
  const ns  = req.query.namespace || null;
  if (!ctx) { res.status(400).end(); return; }

  res.setHeader('Content-Type',  'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection',    'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();

  const emit = (obj) => res.write(`data: ${JSON.stringify(obj)}\n\n`);
  const start = Date.now();
  const elapsed = () => ((Date.now() - start) / 1000).toFixed(1) + 's';

  const pause = (ms) => new Promise(r => setTimeout(r, ms));
  let podResult, logsResult, evResult, depResult, netResult;

  // ── Stage 1: Pods ──────────────────────────────────────────────────────────
  emit({ stage:'pods', status:'running', message:'Fetching pod status across all namespaces…' });
  await pause(1200);
  try {
    const podsData = await fetchK8s(ctx, ns ? `/api/v1/namespaces/${ns}/pods` : '/api/v1/pods');
    podResult = inspectPods(podsData, null);
    if (podsData.error) podResult.error = podsData.error;
    emit({ stage:'pods', status:'done', elapsed:elapsed(),
           total: podResult.total, issues: podResult.problematic_pods.length,
           message: `${podResult.total} pods found, ${podResult.problematic_pods.length} unhealthy` });
  } catch(e) {
    podResult = { total:0, problematic_pods:[], all_pods:[], healthy:false, error:e.message };
    emit({ stage:'pods', status:'error', elapsed:elapsed(), message: e.message });
  }

  await pause(1400);

  // ── Stage 2: Logs ─────────────────────────────────────────────────────────
  const badPods = podResult.problematic_pods || [];
  emit({ stage:'logs', status:'running',
         message: badPods.length
           ? `Collecting logs for ${badPods.length} unhealthy pod(s)…`
           : 'No unhealthy pods — skipping log collection' });
  try {
    logsResult = await collectLogs(ctx, badPods);
    const withErrors = Object.values(logsResult).filter(l => l.error_count > 0).length;
    emit({ stage:'logs', status:'done', elapsed:elapsed(),
           total: Object.keys(logsResult).length, issues: withErrors,
           message: `Logs collected for ${Object.keys(logsResult).length} pod(s), ${withErrors} with error lines` });
  } catch(e) {
    logsResult = {};
    emit({ stage:'logs', status:'error', elapsed:elapsed(), message: e.message });
  }

  await pause(1400);

  // ── Stage 3: Events ───────────────────────────────────────────────────────
  emit({ stage:'events', status:'running', message:'Reading Kubernetes warning events…' });
  await pause(1200);
  try {
    const evData = await fetchK8s(ctx, ns ? `/api/v1/namespaces/${ns}/events` : '/api/v1/events');
    evResult = analyzeEvents(evData, null);
    emit({ stage:'events', status:'done', elapsed:elapsed(),
           total: evResult.total, issues: evResult.warning_count,
           message: `${evResult.total} events, ${evResult.warning_count} warnings` });
  } catch(e) {
    evResult = { total:0, warning_count:0, warning_events:[], healthy:true };
    emit({ stage:'events', status:'error', elapsed:elapsed(), message: e.message });
  }

  await pause(1400);

  // ── Stage 4: Deployments ──────────────────────────────────────────────────
  emit({ stage:'deployments', status:'running', message:'Inspecting deployment replica health…' });
  await pause(1200);
  try {
    const depsData = await fetchK8s(ctx, ns ? `/apis/apps/v1/namespaces/${ns}/deployments` : '/apis/apps/v1/deployments');
    depResult = inspectDeployments(depsData, null);
    emit({ stage:'deployments', status:'done', elapsed:elapsed(),
           total: depResult.total, issues: depResult.unhealthy_deployments.length,
           message: `${depResult.total} deployments, ${depResult.unhealthy_deployments.length} unhealthy` });
  } catch(e) {
    depResult = { total:0, unhealthy_deployments:[], all_deployments:[], healthy:true };
    emit({ stage:'deployments', status:'error', elapsed:elapsed(), message: e.message });
  }

  await pause(1400);

  // ── Stage 5: Network ──────────────────────────────────────────────────────
  emit({ stage:'network', status:'running', message:'Checking service endpoints and selectors…' });
  await pause(1200);
  try {
    const [svcData, epData] = await Promise.all([
      fetchK8s(ctx, ns ? `/api/v1/namespaces/${ns}/services`  : '/api/v1/services'),
      fetchK8s(ctx, ns ? `/api/v1/namespaces/${ns}/endpoints` : '/api/v1/endpoints'),
    ]);
    netResult = inspectNetwork(svcData, epData, null);
    emit({ stage:'network', status:'done', elapsed:elapsed(),
           total: netResult.total, issues: netResult.issue_count,
           message: `${netResult.total} services, ${netResult.issue_count} with endpoint issues` });
  } catch(e) {
    netResult = { total:0, issue_count:0, issues:[], all_services:[], healthy:true };
    emit({ stage:'network', status:'error', elapsed:elapsed(), message: e.message });
  }

  await pause(1000);

  // ── Stage 6: AI Reasoning ─────────────────────────────────────────────────
  emit({ stage:'reasoning', status:'running', elapsed:elapsed(),
         message:'Correlating evidence and reasoning about root causes…' });
  await pause(1800);
  const aiResult = reasonAboutInvestigation({
    pods:podResult, logs:logsResult, events:evResult, deployments:depResult, network:netResult
  });
  emit({ stage:'reasoning', status:'done', elapsed:elapsed(),
         confidence: aiResult.confidence, severity: aiResult.severity,
         issues: aiResult.root_causes.length,
         message: aiResult.summary });
  await pause(800);

  // ── Complete ──────────────────────────────────────────────────────────────
  const totalIssues = (podResult.problematic_pods||[]).length +
                      (depResult.unhealthy_deployments||[]).length +
                      (evResult.warning_count||0) +
                      (netResult.issue_count||0);

  emit({
    stage:'complete', status:'done', elapsed:elapsed(), total_issues: totalIssues,
    result: {
      status:'success', context:ctx, namespace:ns||null,
      ai_reasoning: aiResult,
      investigation:{ pods:podResult, logs:logsResult, events:evResult, deployments:depResult, network:netResult }
    }
  });

  res.end();
});

// POST /api/resolve
app.post('/api/resolve', (req, res) => {
  const { investigation } = req.body || {};
  if (!investigation) return res.status(400).json({ error: 'investigation payload required' });
  try {
    res.json(resolveInvestigation(investigation));
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// S3 routes
app.get('/api/s3/buckets', async (req, res) => {
  try {
    const data = await s3.send(new ListBucketsCommand({}));
    const buckets = await Promise.all((data.Buckets || []).map(async b => {
      let region = 'us-east-1';
      try {
        const loc = await s3.send(new GetBucketLocationCommand({ Bucket: b.Name }));
        region = loc.LocationConstraint || 'us-east-1';
      } catch(_) {}
      let objects = 0;
      try {
        const obj = await s3.send(new ListObjectsV2Command({ Bucket: b.Name }));
        objects = obj.KeyCount || 0;
      } catch(_) {}
      return { name: b.Name, created: b.CreationDate, region, objects };
    }));
    res.json(buckets);
  } catch(e) {
    res.status(503).json({ error: e.message });
  }
});

// DynamoDB routes
app.get('/api/dynamo/tables', async (req, res) => {
  try {
    const list = await dynamo.send(new ListTablesCommand({}));
    const tables = await Promise.all((list.TableNames || []).map(async name => {
      try {
        const d = await dynamo.send(new DescribeTableCommand({ TableName: name }));
        const t = d.Table;
        return {
          name,
          status:    t.TableStatus,
          items:     t.ItemCount,
          sizeBytes: t.TableSizeBytes,
          keySchema: t.KeySchema,
          created:   t.CreationDateTime,
        };
      } catch(_) {
        return { name, status: 'UNKNOWN', items: 0, sizeBytes: 0, keySchema: [] };
      }
    }));
    res.json(tables);
  } catch(e) {
    res.status(503).json({ error: e.message });
  }
});

app.listen(PORT, () => console.log(`K8s backend listening on :${PORT}`));
