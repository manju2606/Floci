const express = require('express');
const https   = require('https');
const fs      = require('fs');
const { spawn } = require('child_process');
const yaml    = require('js-yaml');
const { S3Client, ListBucketsCommand, ListObjectsV2Command, GetBucketLocationCommand } = require('@aws-sdk/client-s3');
const { DynamoDBClient, ListTablesCommand, DescribeTableCommand,
        CreateTableCommand, PutItemCommand, ScanCommand, GetItemCommand,
        QueryCommand } = require('@aws-sdk/client-dynamodb');

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

// Write a temp kubeconfig with 127.0.0.1/localhost → host.docker.internal
// so kubectl subprocesses can reach Kind API servers from inside the container.
function makeKubectlConfig(contextName) {
  const kc  = readKubeconfig();
  const out  = JSON.parse(JSON.stringify(kc)); // deep clone
  (out.clusters || []).forEach(c => {
    if (c.cluster && c.cluster.server) {
      c.cluster.server = c.cluster.server
        .replace('https://127.0.0.1', 'https://host.docker.internal')
        .replace('https://localhost',  'https://host.docker.internal');
    }
  });
  out['current-context'] = contextName;
  const tmpPath = `/tmp/kc-exec-${Date.now()}.yaml`;
  fs.writeFileSync(tmpPath, yaml.dump(out));
  return tmpPath;
}

// Return a human-readable resolution tip for a failed command
function resolutionTip(cmd, stderr) {
  if (/connection refused|no route to host/i.test(stderr))
    return 'The Kubernetes API server is unreachable. Verify the cluster is running with: kubectl cluster-info';
  if (/unauthorized|forbidden/i.test(stderr))
    return 'Authentication or RBAC error. Check your kubeconfig credentials and cluster role bindings.';
  if (/not found|no such file/i.test(stderr) || /127$/.test(''+1))
    return `"${cmd.split(' ')[0]}" was not found. It may need to run on a different machine or be installed separately.`;
  if (/timeout/i.test(stderr))
    return 'The command timed out. Check cluster health and network connectivity.';
  return 'Check the error output above and verify your cluster state before retrying.';
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

// ── DynamoDB History Helpers ───────────────────────────────────────────────────
const K8S_TABLE     = 'k8s';
const K8S_RES_TABLE = 'k8s-resolutions';

async function ensureK8sTable() {
  try {
    await dynamo.send(new DescribeTableCommand({ TableName: K8S_TABLE }));
  } catch(e) {
    if (e.name === 'ResourceNotFoundException') {
      await dynamo.send(new CreateTableCommand({
        TableName: K8S_TABLE,
        AttributeDefinitions: [
          { AttributeName: 'cluster', AttributeType: 'S' },
          { AttributeName: 'run_id',  AttributeType: 'S' },
        ],
        KeySchema: [
          { AttributeName: 'cluster', KeyType: 'HASH'  },
          { AttributeName: 'run_id',  KeyType: 'RANGE' },
        ],
        BillingMode: 'PAY_PER_REQUEST',
      }));
      await new Promise(r => setTimeout(r, 600));
    } else {
      throw e;
    }
  }
}

async function ensureK8sResTable() {
  try {
    await dynamo.send(new DescribeTableCommand({ TableName: K8S_RES_TABLE }));
  } catch(e) {
    if (e.name === 'ResourceNotFoundException') {
      await dynamo.send(new CreateTableCommand({
        TableName: K8S_RES_TABLE,
        AttributeDefinitions: [
          { AttributeName: 'cluster', AttributeType: 'S' },
          { AttributeName: 'run_id',  AttributeType: 'S' },
        ],
        KeySchema: [
          { AttributeName: 'cluster', KeyType: 'HASH'  },
          { AttributeName: 'run_id',  KeyType: 'RANGE' },
        ],
        BillingMode: 'PAY_PER_REQUEST',
      }));
      await new Promise(r => setTimeout(r, 600));
    } else {
      throw e;
    }
  }
}

async function saveInvestigationRun(ctx, ns, investigation, aiResult, totalIssues) {
  await ensureK8sTable();
  const run_id = new Date().toISOString();
  await dynamo.send(new PutItemCommand({
    TableName: K8S_TABLE,
    Item: {
      cluster:               { S: ctx },
      run_id:                { S: run_id },
      namespace:             { S: ns || 'all' },
      total_issues:          { N: String(totalIssues) },
      pods_unhealthy:        { N: String((investigation.pods?.problematic_pods||[]).length) },
      deployments_unhealthy: { N: String((investigation.deployments?.unhealthy_deployments||[]).length) },
      events_warnings:       { N: String(investigation.events?.warning_count||0) },
      network_issues:        { N: String(investigation.network?.issue_count||0) },
      ai_severity:           { S: aiResult?.severity || 'info' },
      ai_confidence:         { N: String(aiResult?.confidence || 0) },
      ai_summary:            { S: (aiResult?.summary || '').slice(0, 1000) },
      investigation_json:    { S: JSON.stringify(investigation) },
      ai_reasoning_json:     { S: JSON.stringify(aiResult || null) },
    }
  }));
  return run_id;
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

  const investigation = { pods:podResult, logs:logsResult, events:evResult, deployments:depResult, network:netResult };

  // Save to DynamoDB (fire-and-forget — don't block the SSE response)
  saveInvestigationRun(ctx, ns, investigation, aiResult, totalIssues)
    .catch(e => console.error('[dynamo] save failed:', e.message));

  emit({
    stage:'complete', status:'done', elapsed:elapsed(), total_issues: totalIssues,
    result: {
      status:'success', context:ctx, namespace:ns||null,
      ai_reasoning: aiResult,
      investigation
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

// GET /api/history/:ctx — list last 50 runs for a cluster (query by partition key)
app.get('/api/history/:ctx', async (req, res) => {
  const ctx = decodeURIComponent(req.params.ctx);
  try {
    const data = await dynamo.send(new QueryCommand({
      TableName: K8S_TABLE,
      KeyConditionExpression: 'cluster = :c',
      ExpressionAttributeValues: { ':c': { S: ctx } },
      ScanIndexForward: false,   // newest first (descending sort key)
      Limit: 50,
    }));
    const items = (data.Items || []).map(i => ({
      run_id:                i.run_id.S,
      cluster:               i.cluster.S,
      namespace:             i.namespace?.S || 'all',
      total_issues:          parseInt(i.total_issues?.N || '0'),
      pods_unhealthy:        parseInt(i.pods_unhealthy?.N || '0'),
      deployments_unhealthy: parseInt(i.deployments_unhealthy?.N || '0'),
      events_warnings:       parseInt(i.events_warnings?.N || '0'),
      network_issues:        parseInt(i.network_issues?.N || '0'),
      ai_severity:           i.ai_severity?.S || 'info',
      ai_confidence:         parseInt(i.ai_confidence?.N || '0'),
      ai_summary:            i.ai_summary?.S || '',
    }));
    res.json({ items });
  } catch(e) {
    if (e.name === 'ResourceNotFoundException') return res.json({ items: [] });
    res.status(500).json({ error: e.message });
  }
});

// GET /api/history/:ctx/:runId — fetch full payload for one run
app.get('/api/history/:ctx/:runId', async (req, res) => {
  const ctx    = decodeURIComponent(req.params.ctx);
  const run_id = decodeURIComponent(req.params.runId);
  try {
    const data = await dynamo.send(new GetItemCommand({
      TableName: K8S_TABLE,
      Key: { cluster: { S: ctx }, run_id: { S: run_id } },
    }));
    if (!data.Item) return res.status(404).json({ error: 'Run not found' });
    const i = data.Item;
    res.json({
      run_id:        i.run_id.S,
      cluster:       i.cluster.S,
      namespace:     i.namespace?.S || 'all',
      total_issues:  parseInt(i.total_issues?.N || '0'),
      ai_severity:   i.ai_severity?.S || 'info',
      ai_confidence: parseInt(i.ai_confidence?.N || '0'),
      ai_summary:    i.ai_summary?.S || '',
      investigation: JSON.parse(i.investigation_json?.S || '{}'),
      ai_reasoning:  JSON.parse(i.ai_reasoning_json?.S || 'null'),
    });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/k8s/clusters — list unique clusters stored in the k8s table with stats
app.get('/api/k8s/clusters', async (req, res) => {
  try {
    const data = await dynamo.send(new ScanCommand({ TableName: K8S_TABLE }));
    const map = {};
    (data.Items || []).forEach(item => {
      const c = item.cluster?.S;
      if (!c) return;
      if (!map[c]) map[c] = { cluster: c, run_count: 0, last_run: '', last_severity: 'info', last_issues: 0 };
      map[c].run_count++;
      const rid = item.run_id?.S || '';
      if (rid > map[c].last_run) {
        map[c].last_run      = rid;
        map[c].last_severity = item.ai_severity?.S || 'info';
        map[c].last_issues   = parseInt(item.total_issues?.N || '0');
      }
    });
    const clusters = Object.values(map).sort((a, b) => b.last_run.localeCompare(a.last_run));
    res.json({ clusters });
  } catch(e) {
    if (e.name === 'ResourceNotFoundException') return res.json({ clusters: [] });
    res.status(500).json({ error: e.message });
  }
});

// POST /api/resolution/save — persist a resolution run into k8s-resolutions
app.post('/api/resolution/save', async (req, res) => {
  const { cluster, namespace, findings, ai_reasoning } = req.body || {};
  if (!cluster) return res.status(400).json({ error: 'cluster is required' });
  try {
    await ensureK8sResTable();
    const run_id = new Date().toISOString();
    const sevCounts = { critical: 0, high: 0, warning: 0, info: 0 };
    (findings || []).forEach(f => { if (sevCounts[f.severity] !== undefined) sevCounts[f.severity]++; });
    const topSev = sevCounts.critical > 0 ? 'critical'
                 : sevCounts.high     > 0 ? 'high'
                 : sevCounts.warning  > 0 ? 'warning'
                 : sevCounts.info     > 0 ? 'info'
                 : (ai_reasoning?.severity || 'info');
    await dynamo.send(new PutItemCommand({
      TableName: K8S_RES_TABLE,
      Item: {
        cluster:        { S: cluster },
        run_id:         { S: run_id },
        namespace:      { S: namespace || 'all' },
        total_findings: { N: String((findings || []).length) },
        critical_count: { N: String(sevCounts.critical) },
        high_count:     { N: String(sevCounts.high) },
        warning_count:  { N: String(sevCounts.warning) },
        info_count:     { N: String(sevCounts.info) },
        ai_severity:    { S: topSev },
        ai_confidence:  { N: String(ai_reasoning?.confidence || 0) },
        ai_summary:     { S: (ai_reasoning?.summary || '').slice(0, 1000) },
        findings_json:  { S: JSON.stringify(findings || []) },
      }
    }));
    res.json({ run_id });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/resolution-history/:ctx — list last 50 resolution runs for a cluster
app.get('/api/resolution-history/:ctx', async (req, res) => {
  const ctx = decodeURIComponent(req.params.ctx);
  try {
    const data = await dynamo.send(new QueryCommand({
      TableName: K8S_RES_TABLE,
      KeyConditionExpression: 'cluster = :c',
      ExpressionAttributeValues: { ':c': { S: ctx } },
      ScanIndexForward: false,
      Limit: 50,
    }));
    const items = (data.Items || []).map(i => ({
      run_id:         i.run_id.S,
      cluster:        i.cluster.S,
      namespace:      i.namespace?.S || 'all',
      total_findings: parseInt(i.total_findings?.N || '0'),
      critical_count: parseInt(i.critical_count?.N || '0'),
      high_count:     parseInt(i.high_count?.N || '0'),
      warning_count:  parseInt(i.warning_count?.N || '0'),
      info_count:     parseInt(i.info_count?.N || '0'),
      ai_severity:    i.ai_severity?.S || 'info',
      ai_confidence:  parseInt(i.ai_confidence?.N || '0'),
      ai_summary:     i.ai_summary?.S || '',
    }));
    res.json({ items });
  } catch(e) {
    if (e.name === 'ResourceNotFoundException') return res.json({ items: [] });
    res.status(500).json({ error: e.message });
  }
});

// GET /api/k8s-resolutions/clusters — list unique clusters in the k8s-resolutions table
app.get('/api/k8s-resolutions/clusters', async (req, res) => {
  try {
    const data = await dynamo.send(new ScanCommand({ TableName: K8S_RES_TABLE }));
    const map = {};
    (data.Items || []).forEach(item => {
      const c = item.cluster?.S;
      if (!c) return;
      if (!map[c]) map[c] = { cluster: c, run_count: 0, last_run: '', last_severity: 'info', last_findings: 0 };
      map[c].run_count++;
      const rid = item.run_id?.S || '';
      if (rid > map[c].last_run) {
        map[c].last_run      = rid;
        map[c].last_severity = item.ai_severity?.S || 'info';
        map[c].last_findings = parseInt(item.total_findings?.N || '0');
      }
    });
    const clusters = Object.values(map).sort((a, b) => b.last_run.localeCompare(a.last_run));
    res.json({ clusters });
  } catch(e) {
    if (e.name === 'ResourceNotFoundException') return res.json({ clusters: [] });
    res.status(500).json({ error: e.message });
  }
});

// GET /api/dynamo/table/:name/items — browse table items
// For the k8s table: supports ?cluster= to query a specific partition
app.get('/api/dynamo/table/:name/items', async (req, res) => {
  const tableName = req.params.name;
  const cluster   = req.query.cluster;
  try {
    let data;
    const isClusterTable = tableName === K8S_TABLE || tableName === K8S_RES_TABLE;
    if (isClusterTable && cluster) {
      data = await dynamo.send(new QueryCommand({
        TableName: tableName,
        KeyConditionExpression: 'cluster = :c',
        ExpressionAttributeValues: { ':c': { S: cluster } },
        ScanIndexForward: false,
        Limit: 100,
      }));
    } else {
      data = await dynamo.send(new ScanCommand({ TableName: tableName, Limit: 100 }));
    }
    // Convert DynamoDB typed values to plain scalars (omit large JSON blobs)
    const SKIP = new Set(['investigation_json', 'ai_reasoning_json', 'findings_json']);
    const items = (data.Items || []).map(item => {
      const obj = {};
      for (const [k, v] of Object.entries(item)) {
        if (SKIP.has(k)) continue;
        if (v.S  !== undefined) obj[k] = v.S;
        else if (v.N !== undefined) obj[k] = v.N;
        else if (v.BOOL !== undefined) obj[k] = String(v.BOOL);
        else obj[k] = JSON.stringify(v);
      }
      return obj;
    });
    res.json({ items, count: items.length });
  } catch(e) {
    if (e.name === 'ResourceNotFoundException') return res.json({ items: [], count: 0 });
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

// ── Kubernetes Upgrade Agent ───────────────────────────────────────────────────

function fetchGitHub(path) {
  return new Promise((resolve, reject) => {
    const opts = {
      hostname: 'api.github.com',
      path,
      method: 'GET',
      headers: { 'User-Agent': 'floci-dashboard', 'Accept': 'application/vnd.github.v3+json' },
    };
    const req = https.request(opts, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => { try { resolve(JSON.parse(data)); } catch(e) { reject(new Error('bad json')); } });
    });
    req.setTimeout(8000, () => { req.destroy(); reject(new Error('GitHub API timeout')); });
    req.on('error', reject);
    req.end();
  });
}

// ─── Kubernetes API Removal Database ──────────────────────────────────────────
// Each entry: removedIn (minor int), apiVersion, kinds[], replacement, severity, note
const API_REMOVAL_DB = [
  // v1.16
  { removedIn:16, apiVersion:'extensions/v1beta1',
    kinds:['Deployment','DaemonSet','ReplicaSet','NetworkPolicy','Ingress','PodSecurityPolicy'],
    replacement:'apps/v1 / networking.k8s.io/v1', severity:'CRITICAL',
    note:'All major workload types must use apps/v1; Ingress moves to networking.k8s.io/v1' },
  { removedIn:16, apiVersion:'apps/v1beta1',
    kinds:['Deployment','StatefulSet'], replacement:'apps/v1', severity:'CRITICAL',
    note:'apps/v1 stable since 1.9' },
  { removedIn:16, apiVersion:'apps/v1beta2',
    kinds:['Deployment','DaemonSet','StatefulSet','ReplicaSet'], replacement:'apps/v1', severity:'CRITICAL',
    note:'apps/v1 stable since 1.9' },
  // v1.22
  { removedIn:22, apiVersion:'networking.k8s.io/v1beta1',
    kinds:['Ingress','IngressClass'], replacement:'networking.k8s.io/v1', severity:'CRITICAL',
    note:'Must add pathType field when migrating Ingress resources' },
  { removedIn:22, apiVersion:'admissionregistration.k8s.io/v1beta1',
    kinds:['ValidatingWebhookConfiguration','MutatingWebhookConfiguration'],
    replacement:'admissionregistration.k8s.io/v1', severity:'CRITICAL',
    note:'Webhook controllers must be updated before upgrade' },
  { removedIn:22, apiVersion:'apiextensions.k8s.io/v1beta1',
    kinds:['CustomResourceDefinition'], replacement:'apiextensions.k8s.io/v1', severity:'CRITICAL',
    note:'CRDs must define structural schemas; v1beta1 CRDs blocked on upgrade' },
  { removedIn:22, apiVersion:'rbac.authorization.k8s.io/v1beta1',
    kinds:['ClusterRole','ClusterRoleBinding','Role','RoleBinding'],
    replacement:'rbac.authorization.k8s.io/v1', severity:'HIGH',
    note:'RBAC v1 stable since 1.8' },
  { removedIn:22, apiVersion:'scheduling.k8s.io/v1beta1',
    kinds:['PriorityClass'], replacement:'scheduling.k8s.io/v1', severity:'HIGH',
    note:'PriorityClass v1 stable since 1.14' },
  { removedIn:22, apiVersion:'storage.k8s.io/v1beta1',
    kinds:['CSIDriver','CSINode','StorageClass','VolumeAttachment'],
    replacement:'storage.k8s.io/v1', severity:'HIGH',
    note:'storage.k8s.io/v1 stable since 1.17' },
  { removedIn:22, apiVersion:'coordination.k8s.io/v1beta1',
    kinds:['Lease'], replacement:'coordination.k8s.io/v1', severity:'MEDIUM',
    note:'Lease v1 stable since 1.14' },
  { removedIn:22, apiVersion:'authorization.k8s.io/v1beta1',
    kinds:['SubjectAccessReview','LocalSubjectAccessReview','SelfSubjectAccessReview'],
    replacement:'authorization.k8s.io/v1', severity:'MEDIUM',
    note:'Authorization v1 stable since 1.6' },
  // v1.25
  { removedIn:25, apiVersion:'batch/v1beta1',
    kinds:['CronJob'], replacement:'batch/v1', severity:'CRITICAL',
    note:'CronJob v1 stable since 1.21' },
  { removedIn:25, apiVersion:'policy/v1beta1',
    kinds:['PodSecurityPolicy','PodDisruptionBudget'],
    replacement:'policy/v1 (PDB); Pod Security Admission replaces PSP', severity:'CRITICAL',
    note:'PSP permanently removed — migrate to namespace labels + Pod Security Admission' },
  { removedIn:25, apiVersion:'autoscaling/v2beta1',
    kinds:['HorizontalPodAutoscaler'], replacement:'autoscaling/v2', severity:'HIGH',
    note:'autoscaling/v2 stable since 1.23' },
  { removedIn:25, apiVersion:'discovery.k8s.io/v1beta1',
    kinds:['EndpointSlice'], replacement:'discovery.k8s.io/v1', severity:'HIGH',
    note:'EndpointSlice v1 stable since 1.21' },
  { removedIn:25, apiVersion:'node.k8s.io/v1beta1',
    kinds:['RuntimeClass'], replacement:'node.k8s.io/v1', severity:'MEDIUM',
    note:'RuntimeClass v1 stable since 1.20' },
  { removedIn:25, apiVersion:'events.k8s.io/v1beta1',
    kinds:['Event'], replacement:'events.k8s.io/v1', severity:'LOW',
    note:'Events v1 stable since 1.19' },
  // v1.26
  { removedIn:26, apiVersion:'autoscaling/v2beta2',
    kinds:['HorizontalPodAutoscaler'], replacement:'autoscaling/v2', severity:'HIGH',
    note:'autoscaling/v2 stable since 1.23' },
  { removedIn:26, apiVersion:'flowcontrol.apiserver.k8s.io/v1beta1',
    kinds:['FlowSchema','PriorityLevelConfiguration'],
    replacement:'flowcontrol.apiserver.k8s.io/v1beta3', severity:'MEDIUM',
    note:'API Priority and Fairness — check kube-apiserver flags' },
  // v1.29
  { removedIn:29, apiVersion:'flowcontrol.apiserver.k8s.io/v1beta2',
    kinds:['FlowSchema','PriorityLevelConfiguration'],
    replacement:'flowcontrol.apiserver.k8s.io/v1', severity:'MEDIUM',
    note:'flowcontrol v1 stable since 1.29' },
  // v1.32
  { removedIn:32, apiVersion:'resource.k8s.io/v1alpha2',
    kinds:['ResourceClaim','ResourceClass','PodSchedulingContext'],
    replacement:'resource.k8s.io/v1alpha3', severity:'HIGH',
    note:'Dynamic Resource Allocation API updated; alpha — verify usage' },
];

function parseVersion(v) {
  const m = String(v || '').match(/v?(\d+)\.(\d+)\.(\d+)/);
  if (!m) return null;
  return { major: parseInt(m[1]), minor: parseInt(m[2]), patch: parseInt(m[3]),
           str: `v${m[1]}.${m[2]}.${m[3]}` };
}

function estimateUpgradeTime(isKind, skew) {
  if (skew <= 0) return { label: 'None', note: 'Already on latest', stepTimes: {} };
  if (isKind) {
    return {
      label: '5–10 min',
      note: 'Backup ~2 min · Delete ~30 sec · Recreate cluster ~3–7 min · Verify ~1 min',
      stepTimes: { backup: '~2 min', delete: '~30 sec', create: '~3–7 min', verify: '~1 min' },
    };
  }
  // kubeadm: ~20–35 min per minor version hop
  const minT = skew * 20;
  const maxT = skew * 35;
  const perHop = '~20–35 min per hop';
  const stepTimes = {};
  for (let i = 1; i <= skew; i++) stepTimes[`upgrade-to-v${i}`] = '~20–35 min';
  stepTimes['verify'] = '~2 min';
  return {
    label: skew === 1 ? '20–35 min' : `${minT}–${maxT} min`,
    note: `${perHop} · ${skew} hop${skew !== 1 ? 's' : ''} required · Total ~${minT}–${maxT} min`,
    stepTimes,
  };
}

function generateUpgradePlan(serverVersion, latestVersion, contextName, nodes) {
  const cur = parseVersion(serverVersion);
  const lat = parseVersion(latestVersion);
  if (!cur || !lat) return null;

  const isKind    = /^kind-/i.test(contextName);
  const skew      = (lat.major - cur.major) * 100 + (lat.minor - cur.minor);
  const upToDate  = skew <= 0;
  const clusterType = isKind ? 'kind' : 'kubeadm';
  const timeEst   = estimateUpgradeTime(isKind, skew);

  // Build minor-version path (must upgrade one minor at a time for kubeadm)
  const path = [];
  for (let m = cur.minor; m <= lat.minor; m++) {
    const label = (m === cur.minor) ? cur.str
                : (m === lat.minor) ? lat.str
                : `v${cur.major}.${m}.x`;
    path.push(label);
  }

  const steps = [];
  if (!upToDate) {
    if (isKind) {
      const clusterName = contextName.replace(/^kind-/i, '');
      steps.push({
        id: 'backup', title: 'Backup workloads', estimatedTime: timeEst.stepTimes.backup,
        warning: 'Kind clusters must be recreated to upgrade — existing workloads will be lost unless backed up.',
        commands: [
          `kubectl get all -A -o yaml > cluster-backup-${cur.str}.yaml`,
          `kubectl get configmaps -A -o yaml >> cluster-backup-${cur.str}.yaml`,
          `kubectl get secrets -A -o yaml >> cluster-backup-${cur.str}.yaml`,
        ],
      });
      steps.push({
        id: 'delete', title: `Delete cluster "${clusterName}"`, estimatedTime: timeEst.stepTimes.delete,
        warning: 'This permanently removes the cluster and all its data.',
        commands: [`kind delete cluster --name ${clusterName}`],
      });
      steps.push({
        id: 'create', title: `Create cluster with ${lat.str}`, estimatedTime: timeEst.stepTimes.create,
        commands: [
          `kind create cluster --name ${clusterName} --image kindest/node:${lat.str}`,
          `# Or with your existing config:`,
          `kind create cluster --name ${clusterName} --image kindest/node:${lat.str} --config kind-cluster.yaml`,
        ],
      });
    } else {
      // kubeadm — one minor version at a time
      for (let m = cur.minor + 1; m <= lat.minor; m++) {
        const target = (m === lat.minor) ? lat.str : `v${cur.major}.${m}.0`;
        const ver    = target.replace(/^v/, '');
        steps.push({
          id: `upgrade-to-${target}`, title: `Upgrade control plane to ${target}`,
          estimatedTime: '~20–35 min',
          commands: [
            `# Upgrade kubeadm on control-plane node`,
            `apt-get update && apt-get install -y kubeadm=${ver}-*`,
            `kubeadm upgrade plan`,
            `kubeadm upgrade apply ${target}`,
            `# Then upgrade kubelet & kubectl on each node`,
            `apt-get install -y kubelet=${ver}-* kubectl=${ver}-*`,
            `systemctl daemon-reload && systemctl restart kubelet`,
          ],
        });
      }
    }
    steps.push({
      id: 'verify', title: 'Verify the upgrade', estimatedTime: '~1–2 min',
      commands: [
        `kubectl version`,
        `kubectl get nodes -o wide`,
        `kubectl get componentstatuses`,
      ],
    });
  }

  return { current: cur.str, latest: lat.str, upToDate, skew, clusterType, path, steps, nodes, estimatedTime: timeEst };
}

// GET /api/upgrade/stream?context= — SSE upgrade check stream
app.get('/api/upgrade/stream', async (req, res) => {
  const ctx = req.query.context;
  if (!ctx) { res.status(400).end(); return; }

  res.setHeader('Content-Type',  'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection',    'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();

  const emit  = obj => res.write(`data: ${JSON.stringify(obj)}\n\n`);
  const pause = ms  => new Promise(r => setTimeout(r, ms));

  let serverVersion = null, nodeList = [], latestVersion = null;

  // Phase 1 — cluster connect
  emit({ phase: 'connect', status: 'running', message: 'Connecting to cluster and reading server version…' });
  await pause(800);
  try {
    const ver = await fetchK8s(ctx, '/version');
    serverVersion = ver.gitVersion || `v${ver.major}.${ver.minor}`;
    emit({ phase: 'connect', status: 'done', version: serverVersion,
           message: `Connected — Kubernetes ${serverVersion}` });
  } catch(e) {
    emit({ phase: 'connect', status: 'error', message: e.message });
    emit({ phase: 'complete', status: 'error', error: e.message });
    res.end(); return;
  }

  await pause(700);

  // Phase 2 — node versions
  emit({ phase: 'nodes', status: 'running', message: 'Reading node kubelet versions…' });
  await pause(800);
  try {
    const data = await fetchK8s(ctx, '/api/v1/nodes');
    nodeList = (data.items || []).map(n => ({
      name:           n.metadata.name,
      kubeletVersion: n.status?.nodeInfo?.kubeletVersion || '—',
      role:           (n.metadata?.labels?.['node-role.kubernetes.io/control-plane'] !== undefined ||
                       n.metadata?.labels?.['node-role.kubernetes.io/master'] !== undefined)
                       ? 'control-plane' : 'worker',
      status: (n.status?.conditions || []).find(c => c.type === 'Ready')?.status === 'True' ? 'Ready' : 'NotReady',
    }));
    emit({ phase: 'nodes', status: 'done', nodes: nodeList,
           message: `${nodeList.length} node(s) found` });
  } catch(e) {
    nodeList = [];
    emit({ phase: 'nodes', status: 'error', message: e.message });
  }

  await pause(700);

  // Phase 3 — fetch latest K8s version from GitHub
  emit({ phase: 'latest', status: 'running', message: 'Fetching latest Kubernetes release from GitHub…' });
  await pause(600);
  try {
    const release = await fetchGitHub('/repos/kubernetes/kubernetes/releases/latest');
    latestVersion = release.tag_name;
    emit({ phase: 'latest', status: 'done', version: latestVersion,
           message: `Latest stable: ${latestVersion}` });
  } catch(e) {
    latestVersion = serverVersion; // fallback — can't determine latest
    emit({ phase: 'latest', status: 'error',
           message: `Could not reach GitHub (${e.message}) — showing current version only` });
  }

  await pause(600);

  // Phase 4 — analyze
  emit({ phase: 'analyze', status: 'running', message: 'Computing upgrade path and generating plan…' });
  await pause(900);
  const plan = generateUpgradePlan(serverVersion, latestVersion, ctx, nodeList);
  if (plan.upToDate) {
    emit({ phase: 'analyze', status: 'done', message: `Already on latest (${serverVersion}) — no upgrade needed` });
  } else {
    emit({ phase: 'analyze', status: 'done',
           message: `${plan.skew} minor version(s) behind — upgrade path: ${plan.path.join(' → ')}` });
  }

  await pause(500);

  // Complete
  emit({ phase: 'complete', status: 'done', plan });
  res.end();
});

// GET /api/upgrade/setup?context= — SSE environment pre-flight checklist
app.get('/api/upgrade/setup', async (req, res) => {
  const ctx = req.query.context;
  if (!ctx) { res.status(400).end(); return; }

  res.setHeader('Content-Type',  'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection',    'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();

  const emit  = obj => res.write(`data: ${JSON.stringify(obj)}\n\n`);
  const pause = ms  => new Promise(r => setTimeout(r, ms));

  let kcPath = null;
  try { kcPath = makeKubectlConfig(ctx); } catch(e) {
    emit({ type: 'complete', allPass: false, fatalError: e.message });
    return res.end();
  }

  const env = { ...process.env, KUBECONFIG: kcPath };

  function runCheck(cmd) {
    return new Promise(resolve => {
      let stdout = '', stderr = '';
      const proc = spawn(cmd, [], { env, shell: true, cwd: '/tmp' });
      proc.stdout.on('data', d => { stdout += d.toString(); });
      proc.stderr.on('data', d => { stderr += d.toString(); });
      proc.on('error', err => { stderr += err.message; resolve({ exitCode: 1, stdout, stderr }); });
      proc.on('close', code => resolve({ exitCode: code || 0, stdout, stderr }));
    });
  }

  const checks = [
    { id: 'kubectl',  label: 'kubectl installed',
      cmd: 'kubectl version --client',
      hint: 'kubectl is not in PATH — the backend container may not have it installed' },
    { id: 'kind',     label: 'kind CLI installed',
      cmd: 'kind version',
      hint: 'kind is not in PATH — Kind cluster operations (delete/create) will not work' },
    { id: 'docker',   label: 'Docker socket accessible',
      cmd: 'docker info --format "Server Version: {{.ServerVersion}} | Containers: {{.Containers}}"',
      hint: 'Docker socket is not mounted — kind commands cannot manage clusters from inside the container' },
    { id: 'connect',  label: `Cluster reachable — ${ctx}`,
      cmd: `kubectl --context="${ctx}" cluster-info`,
      hint: 'Cannot reach the Kubernetes API server — check the cluster is running and the context is correct' },
    { id: 'auth',     label: 'Admin permissions verified',
      cmd: `kubectl --context="${ctx}" auth can-i '*' '*' --all-namespaces`,
      hint: 'Insufficient permissions — upgrade requires cluster-admin access' },
    { id: 'nodes',    label: 'Node health',
      cmd: `kubectl --context="${ctx}" get nodes -o wide`,
      hint: 'Could not retrieve node list — cluster may be partially unavailable' },
    { id: 'version',  label: 'Server version confirmed',
      cmd: `kubectl --context="${ctx}" version`,
      hint: 'Could not confirm server version' },
  ];

  const results = [];
  let allPass = true;

  for (const check of checks) {
    emit({ type: 'check-start', id: check.id, label: check.label });
    await pause(700);

    const { exitCode, stdout, stderr } = await runCheck(check.cmd);
    const pass = exitCode === 0;
    if (!pass) allPass = false;

    // Keep output brief — first 6 meaningful lines
    const rawOut = (stdout + (stdout && stderr ? '\n' : '') + stderr).trim();
    const output = rawOut.split('\n').filter(l => l.trim()).slice(0, 6).join('\n');

    results.push({ id: check.id, label: check.label, pass, output, hint: check.hint });
    emit({ type: 'check-done', id: check.id, label: check.label, pass, output,
           hint: pass ? null : check.hint });
    await pause(400);
  }

  try { fs.unlinkSync(kcPath); } catch { /* ignore */ }

  emit({ type: 'complete', allPass, results });
  res.end();
});

// GET /api/upgrade/execute?context= — SSE real-time upgrade execution
app.get('/api/upgrade/execute', async (req, res) => {
  const ctx = req.query.context;
  if (!ctx) { res.status(400).end(); return; }

  res.setHeader('Content-Type',  'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection',    'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();

  const emit = obj => res.write(`data: ${JSON.stringify(obj)}\n\n`);
  let aborted = false;
  req.on('close', () => { aborted = true; });

  try {
    emit({ type: 'status', message: 'Generating upgrade plan…' });

    const ver      = await fetchK8s(ctx, '/version');
    const srvVer   = ver.gitVersion || `v${ver.major}.${ver.minor}`;
    const nodeData = await fetchK8s(ctx, '/api/v1/nodes');
    const nodeList = (nodeData.items || []).map(n => ({
      name:           n.metadata.name,
      kubeletVersion: n.status?.nodeInfo?.kubeletVersion || '—',
      role:           (n.metadata?.labels?.['node-role.kubernetes.io/control-plane'] !== undefined ||
                       n.metadata?.labels?.['node-role.kubernetes.io/master']        !== undefined)
                       ? 'control-plane' : 'worker',
      status: (n.status?.conditions || []).find(c => c.type === 'Ready')?.status === 'True' ? 'Ready' : 'NotReady',
    }));

    let latestVer = srvVer;
    try {
      const rel = await fetchGitHub('/repos/kubernetes/kubernetes/releases/latest');
      latestVer = rel.tag_name;
    } catch { /* fallback to current */ }

    const plan = generateUpgradePlan(srvVer, latestVer, ctx, nodeList);

    if (plan.upToDate) {
      emit({ type: 'complete', upToDate: true });
      return res.end();
    }

    emit({ type: 'plan', totalSteps: plan.steps.length, current: plan.current, latest: plan.latest,
           steps: plan.steps.map(s => ({ id: s.id, title: s.title })) });

    // Commands that must run on the actual Kubernetes node — cannot execute from container
    const NODE_ONLY_PREFIXES = ['apt-get', 'apt ', 'yum ', 'dnf ', 'systemctl', 'kubeadm'];
    function isNodeOnly(cmd) {
      const t = cmd.trim().toLowerCase();
      return NODE_ONLY_PREFIXES.some(p => t.startsWith(p));
    }
    function nodeOnlyTip(cmd) {
      if (/^apt(-get)?/i.test(cmd.trim())) return 'Package manager — run on each Kubernetes node via SSH.';
      if (/^systemctl/i.test(cmd.trim()))  return 'Systemd — run on each Kubernetes node via SSH.';
      if (/^kubeadm/i.test(cmd.trim()))    return 'kubeadm — run on the control-plane node via SSH.';
      return 'This command must run directly on the Kubernetes node via SSH.';
    }

    // Build a temp kubeconfig with host.docker.internal so kubectl can reach the API server
    const kcPath = makeKubectlConfig(ctx);
    const env = { ...process.env, KUBECONFIG: kcPath };

    const pause = ms => new Promise(r => setTimeout(r, ms));

    try {
      for (let si = 0; si < plan.steps.length; si++) {
        if (aborted) break;
        const step = plan.steps[si];

        // Pause between steps so the output is easy to follow
        if (si > 0) await pause(2000);

        emit({ type: 'step-start', idx: si, stepId: step.id, title: step.title, total: plan.steps.length });
        await pause(600);

        let stepFailed = false;

        for (const rawCmd of step.commands) {
          if (aborted) break;
          const trimmed = rawCmd.trim();

          if (!trimmed || trimmed.startsWith('#')) {
            emit({ type: 'cmd-skip', cmd: rawCmd });
            await pause(300);
            continue;
          }

          // Node-only commands — show SSH tip and continue
          if (isNodeOnly(trimmed)) {
            emit({ type: 'cmd-node-only', cmd: trimmed, tip: nodeOnlyTip(trimmed) });
            await pause(1200);
            continue;
          }

          // Inject --context into kubectl commands
          let execCmd = trimmed;
          if (execCmd.startsWith('kubectl ') && !execCmd.includes('--context')) {
            execCmd = `kubectl --context="${ctx}" ${execCmd.slice(8)}`;
          }

          await pause(500);
          emit({ type: 'cmd-start', cmd: trimmed });

          let stderrBuf = '';
          const exitCode = await new Promise(resolve => {
            const proc = spawn(execCmd, [], { env, shell: true, cwd: '/tmp' });
            proc.stdout.on('data', d => emit({ type: 'output', stream: 'stdout', text: d.toString() }));
            proc.stderr.on('data', d => {
              stderrBuf += d.toString();
              emit({ type: 'output', stream: 'stderr', text: d.toString() });
            });
            proc.on('error', err => {
              stderrBuf += err.message;
              emit({ type: 'output', stream: 'stderr', text: `Error: ${err.message}\n` });
              resolve(1);
            });
            proc.on('close', resolve);
          });

          await pause(400);
          emit({ type: 'cmd-done', exitCode });

          if (exitCode !== 0) {
            await pause(600);
            emit({ type: 'cmd-resolution', cmd: trimmed, tip: resolutionTip(trimmed, stderrBuf) });
            stepFailed = true;
            break;
          }

          await pause(600);
        }

        if (stepFailed) {
          await pause(500);
          emit({ type: 'step-error', stepId: step.id });
          break;
        }
        await pause(500);
        emit({ type: 'step-done', stepId: step.id });
      }
    } finally {
      try { fs.unlinkSync(kcPath); } catch { /* ignore */ }
    }

    emit({ type: 'complete', aborted });
  } catch (err) {
    emit({ type: 'error', message: err.message });
  }

  res.end();
});

// GET /api/s3/bucket/:name/objects — list objects at a given prefix (folder nav)
app.get('/api/s3/bucket/:name/objects', async (req, res) => {
  const bucket = req.params.name;
  const prefix = req.query.prefix || '';
  const token  = req.query.continuationToken;
  try {
    const params = { Bucket: bucket, Delimiter: '/', Prefix: prefix, MaxKeys: 500 };
    if (token) params.ContinuationToken = token;
    const data = await s3.send(new ListObjectsV2Command(params));
    const prefixes = (data.CommonPrefixes || []).map(p => ({ prefix: p.Prefix }));
    const objects  = (data.Contents || [])
      .filter(o => o.Key !== prefix)
      .map(o => ({
        key:          o.Key,
        size:         o.Size,
        lastModified: o.LastModified,
        etag:         (o.ETag || '').replace(/"/g, ''),
        storageClass: o.StorageClass || 'STANDARD',
      }));
    res.json({ bucket, prefix, prefixes, objects, isTruncated: data.IsTruncated,
               continuationToken: data.NextContinuationToken, keyCount: data.KeyCount });
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

// ─── Feasibility Assessment Stream ────────────────────────────────────────────
// GET /api/feasibility/stream?context=
app.get('/api/feasibility/stream', async (req, res) => {
  const ctx = req.query.context;
  if (!ctx) { res.status(400).end(); return; }

  res.setHeader('Content-Type',  'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection',    'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();

  const emit  = obj => res.write(`data: ${JSON.stringify(obj)}\n\n`);
  const pause = ms  => new Promise(r => setTimeout(r, ms));

  const findings   = { CRITICAL:[], HIGH:[], MEDIUM:[], LOW:[] };
  const phaseErrors = {};
  const report     = {};

  function addFinding(severity, area, title, detail) {
    findings[severity] = findings[severity] || [];
    findings[severity].push({ severity, area, title, ...detail });
  }

  // ── Phase 1: Cluster Info ──────────────────────────────────────────────────
  emit({ phase:'cluster-info', status:'running', message:'Reading cluster version, nodes and namespaces…' });
  await pause(600);
  try {
    const [ver, nodeData, nsData] = await Promise.all([
      fetchK8s(ctx, '/version'),
      fetchK8s(ctx, '/api/v1/nodes'),
      fetchK8s(ctx, '/api/v1/namespaces'),
    ]);

    const serverVersion = ver.gitVersion || `v${ver.major}.${ver.minor}`;
    const nodes = (nodeData.items || []).map(n => ({
      name:    n.metadata.name,
      os:      n.status?.nodeInfo?.osImage      || '—',
      runtime: n.status?.nodeInfo?.containerRuntimeVersion || '—',
      kubelet: n.status?.nodeInfo?.kubeletVersion || '—',
      role:    (n.metadata?.labels?.['node-role.kubernetes.io/control-plane'] !== undefined ||
                n.metadata?.labels?.['node-role.kubernetes.io/master']        !== undefined)
               ? 'control-plane' : 'worker',
      ready:   (n.status?.conditions || []).find(c => c.type==='Ready')?.status === 'True',
      memPressure: (n.status?.conditions || []).find(c => c.type==='MemoryPressure')?.status === 'True',
      diskPressure:(n.status?.conditions || []).find(c => c.type==='DiskPressure')?.status  === 'True',
    }));
    const namespaces = (nsData.items || []).map(n => n.metadata.name);

    report.clusterVersion = serverVersion;
    report.nodes          = nodes;
    report.namespaces     = namespaces;

    // Check node conditions for pressure flags
    nodes.forEach(n => {
      if (n.memPressure) addFinding('HIGH', 'Nodes',
        `Memory pressure on node ${n.name}`,
        { whatWillBreak:'Pods may be evicted during upgrade, causing downtime',
          whenItBreaks:'During node upgrade', impact:'Pod eviction',
          remediation:'Free memory or add node capacity before upgrading' });
      if (n.diskPressure) addFinding('HIGH', 'Nodes',
        `Disk pressure on node ${n.name}`,
        { whatWillBreak:'Image pulls may fail during upgrade',
          whenItBreaks:'During node upgrade', impact:'Image pull failures',
          remediation:'Free disk space on the node before upgrading' });
    });

    emit({ phase:'cluster-info', status:'done',
           message:`${serverVersion} · ${nodes.length} node(s) · ${namespaces.length} namespace(s)`,
           version: serverVersion });
  } catch(e) {
    phaseErrors['cluster-info'] = e.message;
    emit({ phase:'cluster-info', status:'error', message: e.message });
  }

  await pause(800);

  // ── Phase 2: Workload Inventory ────────────────────────────────────────────
  emit({ phase:'workloads', status:'running', message:'Inventorying deployments, statefulsets, daemonsets, jobs and cronjobs…' });
  await pause(600);
  try {
    const [deps, sts, ds, jobs, cjs] = await Promise.allSettled([
      fetchK8s(ctx, '/apis/apps/v1/deployments'),
      fetchK8s(ctx, '/apis/apps/v1/statefulsets'),
      fetchK8s(ctx, '/apis/apps/v1/daemonsets'),
      fetchK8s(ctx, '/apis/batch/v1/jobs'),
      fetchK8s(ctx, '/apis/batch/v1/cronjobs'),
    ]);

    const workloads = {
      deployments:  deps.status==='fulfilled' ? (deps.value.items||[])   : [],
      statefulsets: sts.status ==='fulfilled' ? (sts.value.items||[])    : [],
      daemonsets:   ds.status  ==='fulfilled' ? (ds.value.items||[])     : [],
      jobs:         jobs.status==='fulfilled' ? (jobs.value.items||[])   : [],
      cronjobs:     cjs.status ==='fulfilled' ? (cjs.value.items||[])    : [],
    };

    report.workloads = {
      deployments:  workloads.deployments.length,
      statefulsets: workloads.statefulsets.length,
      daemonsets:   workloads.daemonsets.length,
      jobs:         workloads.jobs.length,
      cronjobs:     workloads.cronjobs.length,
      total: workloads.deployments.length + workloads.statefulsets.length +
             workloads.daemonsets.length  + workloads.cronjobs.length,
    };

    const total = report.workloads.total;
    emit({ phase:'workloads', status:'done',
           message:`${total} workload(s) — ${workloads.deployments.length} Deployments · ${workloads.statefulsets.length} StatefulSets · ${workloads.daemonsets.length} DaemonSets · ${workloads.cronjobs.length} CronJobs`,
           workloads: report.workloads });
  } catch(e) {
    phaseErrors['workloads'] = e.message;
    report.workloads = { deployments:0, statefulsets:0, daemonsets:0, jobs:0, cronjobs:0, total:0 };
    emit({ phase:'workloads', status:'error', message: e.message });
  }

  await pause(800);

  // ── Phase 3: CRD Discovery ─────────────────────────────────────────────────
  emit({ phase:'crds', status:'running', message:'Discovering Custom Resource Definitions…' });
  await pause(600);
  try {
    const crdData = await fetchK8s(ctx, '/apis/apiextensions.k8s.io/v1/customresourcedefinitions');
    const crds = (crdData.items || []).map(c => ({
      name:           c.metadata.name,
      group:          c.spec.group,
      scope:          c.spec.scope,
      versions:       (c.spec.versions || []).map(v => v.name),
      storageVersion: (c.spec.versions || []).find(v => v.storage)?.name || '—',
      hasWebhook:     !!(c.spec.conversion && c.spec.conversion.strategy === 'Webhook'),
    }));
    report.crds = crds;
    emit({ phase:'crds', status:'done',
           message:`${crds.length} CRD(s) found — ${crds.filter(c=>c.hasWebhook).length} with conversion webhooks`,
           crds });
  } catch(e) {
    phaseErrors['crds'] = e.message;
    report.crds = [];
    emit({ phase:'crds', status:'error', message: e.message });
  }

  await pause(800);

  // ── Phase 4: API Removal Scan ──────────────────────────────────────────────
  emit({ phase:'api-scan', status:'running', message:'Fetching available API groups from cluster…' });
  await pause(600);
  try {
    // Get all served API groups
    const apiGroupsData = await fetchK8s(ctx, '/apis');
    const servedVersions = new Set();
    for (const grp of (apiGroupsData.groups || [])) {
      for (const v of (grp.versions || [])) {
        servedVersions.add(v.groupVersion);
      }
    }

    // Determine target minor version
    let targetMinor = 99;
    let sourceMinor = 0;
    if (report.clusterVersion) {
      const pv = parseVersion(report.clusterVersion);
      sourceMinor = pv.minor;
    }
    try {
      const rel = await fetchGitHub('/repos/kubernetes/kubernetes/releases/latest');
      const pv  = parseVersion(rel.tag_name);
      targetMinor = pv.minor;
      report.targetVersion = rel.tag_name;
    } catch { targetMinor = sourceMinor + 3; }

    report.sourceMinor = sourceMinor;
    report.targetMinor = targetMinor;

    // Filter removals that apply to this upgrade
    const applicableRemovals = API_REMOVAL_DB.filter(
      e => e.removedIn > sourceMinor && e.removedIn <= targetMinor
    );

    emit({ phase:'api-scan', status:'running',
           message:`Scanning ${applicableRemovals.length} deprecated API(s) for live resources…` });

    const apiFindings = [];

    for (const entry of applicableRemovals) {
      const [group, version] = entry.apiVersion.split('/');
      const groupVersion     = entry.apiVersion;

      // Check if this API group/version is still served on the current cluster
      const isServed = servedVersions.has(groupVersion);

      if (!isServed) {
        // Already removed or never present — no risk from live resources
        apiFindings.push({ ...entry, status:'NOT_SERVED', resources:[], resourceCount:0 });
        continue;
      }

      // API is still served — check for live resources
      let totalCount = 0;
      const liveResources = [];

      try {
        const resourceList = await fetchK8s(ctx, `/apis/${groupVersion}`);
        for (const resource of (resourceList.resources || [])) {
          if (resource.name.includes('/')) continue; // skip sub-resources
          try {
            const listPath = resource.namespaced
              ? `/apis/${groupVersion}/${resource.name}`
              : `/apis/${groupVersion}/${resource.name}`;
            const list = await fetchK8s(ctx, listPath);
            const items = list.items || [];
            if (items.length > 0) {
              totalCount += items.length;
              liveResources.push({
                kind:      resource.kind,
                count:     items.length,
                namespaces:[...new Set(items.map(i => i.metadata?.namespace).filter(Boolean))],
                names:     items.slice(0,5).map(i => i.metadata?.name).filter(Boolean),
              });
              addFinding(entry.severity, 'APIs',
                `${resource.kind} using removed ${groupVersion}`,
                { api: groupVersion, kind: resource.kind, count: items.length,
                  replacement: entry.replacement, removedIn: `v1.${entry.removedIn}`,
                  whatWillBreak:`${resource.kind} resources will stop working after upgrade to v1.${entry.removedIn}`,
                  whenItBreaks:'Control Plane Upgrade',
                  impact: entry.severity === 'CRITICAL' ? 'Outage' : 'Partial Outage / Reconciliation Failure',
                  remediation:`Update apiVersion to ${entry.replacement}. ${entry.note}`,
                  resources: items.slice(0,10).map(i => ({ name:i.metadata?.name, namespace:i.metadata?.namespace })) });
            }
          } catch { /* ignore individual resource list errors */ }
        }
      } catch { /* API group list failed */ }

      apiFindings.push({ ...entry, status: totalCount > 0 ? 'FOUND' : 'SERVED_EMPTY',
                         resources: liveResources, resourceCount: totalCount });
    }

    report.apiFindings   = apiFindings;
    report.servedVersions = [...servedVersions];

    const criticalApiCount = apiFindings.filter(f => f.status === 'FOUND' && f.severity === 'CRITICAL').length;
    const highApiCount     = apiFindings.filter(f => f.status === 'FOUND' && f.severity === 'HIGH').length;

    emit({ phase:'api-scan', status:'done',
           message:`${applicableRemovals.length} API(s) checked — ${criticalApiCount} CRITICAL · ${highApiCount} HIGH removals with live resources`,
           apiFindings });
  } catch(e) {
    phaseErrors['api-scan'] = e.message;
    report.apiFindings = [];
    emit({ phase:'api-scan', status:'error', message: e.message });
  }

  await pause(800);

  // ── Phase 5: Webhook Analysis ──────────────────────────────────────────────
  emit({ phase:'webhooks', status:'running', message:'Analysing admission webhooks…' });
  await pause(500);
  try {
    const [valData, mutData] = await Promise.allSettled([
      fetchK8s(ctx, '/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations'),
      fetchK8s(ctx, '/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations'),
    ]);

    const validating = valData.status==='fulfilled' ? (valData.value.items||[]) : [];
    const mutating   = mutData.status==='fulfilled' ? (mutData.value.items||[]) : [];

    const summarise = wh => ({
      name:         wh.metadata.name,
      webhookCount: (wh.webhooks || []).length,
      failurePolicies: [...new Set((wh.webhooks||[]).map(w => w.failurePolicy))],
      hasFailClosed:   (wh.webhooks||[]).some(w => w.failurePolicy === 'Fail'),
    });

    const vSummary = validating.map(summarise);
    const mSummary = mutating.map(summarise);

    vSummary.filter(w=>w.hasFailClosed).forEach(w =>
      addFinding('HIGH', 'Webhooks',
        `ValidatingWebhook "${w.name}" has Fail-closed policy`,
        { whatWillBreak:'If the webhook backend is unavailable after upgrade, all resource requests in scope will be blocked',
          whenItBreaks:'Immediately after Control Plane Upgrade if webhook pod is restarted',
          impact:'Deployment Failure / Partial Outage',
          remediation:'Verify webhook pod compatibility with target K8s version; consider setting failurePolicy: Ignore temporarily during upgrade' }));

    mSummary.filter(w=>w.hasFailClosed).forEach(w =>
      addFinding('HIGH', 'Webhooks',
        `MutatingWebhook "${w.name}" has Fail-closed policy`,
        { whatWillBreak:'Mutations may be skipped or blocked if the webhook pod fails during upgrade',
          whenItBreaks:'Immediately after Control Plane Upgrade',
          impact:'Deployment Failure',
          remediation:'Verify webhook pod compatibility with target K8s version' }));

    report.webhooks = { validating: vSummary, mutating: mSummary };
    emit({ phase:'webhooks', status:'done',
           message:`${validating.length} validating · ${mutating.length} mutating webhook config(s)`,
           webhooks: report.webhooks });
  } catch(e) {
    phaseErrors['webhooks'] = e.message;
    report.webhooks = { validating:[], mutating:[] };
    emit({ phase:'webhooks', status:'error', message: e.message });
  }

  await pause(800);

  // ── Phase 6: Storage Check ─────────────────────────────────────────────────
  emit({ phase:'storage', status:'running', message:'Checking storage classes and persistent volumes…' });
  await pause(500);
  try {
    const [scData, pvData] = await Promise.allSettled([
      fetchK8s(ctx, '/apis/storage.k8s.io/v1/storageclasses'),
      fetchK8s(ctx, '/api/v1/persistentvolumes'),
    ]);

    const storageClasses = scData.status==='fulfilled'
      ? (scData.value.items||[]).map(sc => ({
          name:        sc.metadata.name,
          provisioner: sc.provisioner,
          reclaimPolicy: sc.reclaimPolicy || 'Delete',
          isDefault:   sc.metadata?.annotations?.['storageclass.kubernetes.io/is-default-class'] === 'true',
        })) : [];

    const pvCount = pvData.status==='fulfilled' ? (pvData.value.items||[]).length : 0;

    report.storage = { storageClasses, pvCount };
    emit({ phase:'storage', status:'done',
           message:`${storageClasses.length} StorageClass(es) · ${pvCount} PersistentVolume(s)`,
           storage: report.storage });
  } catch(e) {
    phaseErrors['storage'] = e.message;
    report.storage = { storageClasses:[], pvCount:0 };
    emit({ phase:'storage', status:'error', message: e.message });
  }

  await pause(800);

  // ── Phase 7: Security Check ────────────────────────────────────────────────
  emit({ phase:'security', status:'running', message:'Scanning for privileged workloads and host-network usage…' });
  await pause(500);
  try {
    const podData = await fetchK8s(ctx, '/api/v1/pods');
    const pods    = podData.items || [];

    const privileged  = [];
    const hostNetwork = [];
    const hostPID     = [];

    pods.forEach(pod => {
      const meta = pod.metadata || {};
      const spec = pod.spec     || {};
      if (spec.hostNetwork) hostNetwork.push(`${meta.namespace}/${meta.name}`);
      if (spec.hostPID)     hostPID.push(`${meta.namespace}/${meta.name}`);
      (spec.containers || []).forEach(c => {
        if (c.securityContext?.privileged) privileged.push(`${meta.namespace}/${meta.name}/${c.name}`);
      });
    });

    if (privileged.length)
      addFinding('MEDIUM', 'Security',
        `${privileged.length} privileged container(s) detected`,
        { whatWillBreak:'Privileged containers may be blocked by Pod Security Admission in target version',
          whenItBreaks:'First Deployment after upgrade if PSA is enforced',
          impact:'Partial Outage',
          remediation:'Review if privileged mode is required; apply appropriate PSA labels to namespaces',
          resources: privileged.slice(0,10).map(n=>({ name:n })) });

    if (hostNetwork.length)
      addFinding('MEDIUM', 'Security',
        `${hostNetwork.length} pod(s) using hostNetwork`,
        { whatWillBreak:'Potential CNI/networking conflicts after upgrade',
          whenItBreaks:'Node Upgrade', impact:'Networking disruption',
          remediation:'Validate CNI plugin compatibility with target K8s version',
          resources: hostNetwork.slice(0,10).map(n=>({ name:n })) });

    report.security = { privileged, hostNetwork, hostPID, podCount: pods.length };
    emit({ phase:'security', status:'done',
           message:`${pods.length} pod(s) scanned — ${privileged.length} privileged · ${hostNetwork.length} hostNetwork · ${hostPID.length} hostPID`,
           security: report.security });
  } catch(e) {
    phaseErrors['security'] = e.message;
    report.security = { privileged:[], hostNetwork:[], hostPID:[], podCount:0 };
    emit({ phase:'security', status:'error', message: e.message });
  }

  await pause(800);

  // ── Phase 8: Scoring ───────────────────────────────────────────────────────
  emit({ phase:'score', status:'running', message:'Computing readiness score and generating executive summary…' });
  await pause(800);

  const critCount = (findings.CRITICAL||[]).length;
  const highCount = (findings.HIGH||[]).length;
  const medCount  = (findings.MEDIUM||[]).length;
  const lowCount  = (findings.LOW||[]).length;

  let readiness = 100;
  readiness -= critCount * 25;
  readiness -= highCount * 12;
  readiness -= medCount  *  6;
  readiness -= lowCount  *  2;
  readiness  = Math.max(0, readiness);

  // Confidence: reduces for phase errors and unknown cluster state
  let confidence = 95;
  confidence -= Object.keys(phaseErrors).length * 10;
  confidence  = Math.max(20, Math.min(95, confidence));

  const verdict = readiness >= 90 ? 'APPROVED'
                : readiness >= 75 ? 'CONDITIONAL'
                : readiness >= 50 ? 'HIGH RISK'
                : 'NOT RECOMMENDED';

  const riskMatrix = [
    { area:'APIs',          status: critCount > 0 || highCount > 0 ? (critCount>0?'CRITICAL':'HIGH') : 'PASS', findings:(findings.CRITICAL||[]).filter(f=>f.area==='APIs').length + (findings.HIGH||[]).filter(f=>f.area==='APIs').length },
    { area:'CRDs',          status: report.crds?.some(c=>c.hasWebhook) ? 'WARNING' : 'PASS', findings:0 },
    { area:'Webhooks',      status: (findings.HIGH||[]).some(f=>f.area==='Webhooks') ? 'HIGH' : 'PASS', findings:(findings.HIGH||[]).filter(f=>f.area==='Webhooks').length },
    { area:'Networking',    status: 'PASS', findings:0 },
    { area:'Storage',       status: 'PASS', findings:0 },
    { area:'Security',      status: (findings.MEDIUM||[]).some(f=>f.area==='Security') ? 'WARNING' : 'PASS', findings:(findings.MEDIUM||[]).filter(f=>f.area==='Security').length },
    { area:'Runtime',       status: 'UNKNOWN', findings:0 },
    { area:'Nodes',         status: (findings.HIGH||[]).some(f=>f.area==='Nodes') ? 'HIGH' : 'PASS', findings:(findings.HIGH||[]).filter(f=>f.area==='Nodes').length },
    { area:'Control Plane', status: critCount>0 ? 'HIGH RISK' : 'PASS', findings: critCount },
    { area:'Controllers',   status: 'UNKNOWN', findings:0 },
  ];

  report.findings    = findings;
  report.readiness   = readiness;
  report.confidence  = confidence;
  report.verdict     = verdict;
  report.riskMatrix  = riskMatrix;
  report.phaseErrors = phaseErrors;
  report.summary = {
    criticalIssues: (findings.CRITICAL||[]).map(f=>f.title),
    highRisks:      (findings.HIGH||[]).map(f=>f.title),
    warnings:       (findings.MEDIUM||[]).map(f=>f.title),
    requiredActions: [
      ...( (findings.CRITICAL||[]).map(f=>`[CRITICAL] ${f.title}: ${f.remediation||''}`) ),
      ...( (findings.HIGH||[]).map(f=>`[HIGH] ${f.title}: ${f.remediation||''}`) ),
    ].slice(0,10),
  };

  emit({ phase:'score', status:'done',
         message:`Readiness ${readiness}/100 · Confidence ${confidence}% · Verdict: ${verdict}`,
         readiness, confidence, verdict });

  await pause(500);
  emit({ phase:'complete', status:'done', report });
  res.end();
});

app.listen(PORT, () => console.log(`K8s backend listening on :${PORT}`));
