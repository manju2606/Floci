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

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
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
