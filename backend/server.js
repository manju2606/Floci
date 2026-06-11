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
