/**
 * Amazon MSK (Managed Streaming for Apache Kafka) Backend Server
 * Express REST API + WebSocket Real-Time Event Streaming Engine
 */

const express = require('express');
const cors = require('cors');
const http = require('http');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.static(path.join(__dirname)));

// ==========================================
// 1. IN-MEMORY KAFKA CLUSTER STATE
// ==========================================

const CLUSTER_STATE = {
  name: 'msk-prod-eventstream-01',
  arn: 'arn:aws:kafka:us-east-1:123456789012:cluster/msk-prod-eventstream-01/a1b2c3d4-e5f6',
  kafkaVersion: '3.6.0',
  mode: 'Provisioned KRaft',
  region: 'us-east-1',
  status: 'ACTIVE',
  simulatorRunning: true,
  brokers: [
    { id: 1, az: 'us-east-1a', subnet: 'subnet-081af29b', endpoint: 'b-1.msk-prod.kafka.us-east-1.amazonaws.com:9098', cpu: 26, diskUsedGb: 242, diskTotalGb: 1000, leaderPartitions: 8 },
    { id: 2, az: 'us-east-1b', subnet: 'subnet-09f14b62', endpoint: 'b-2.msk-prod.kafka.us-east-1.amazonaws.com:9098', cpu: 29, diskUsedGb: 238, diskTotalGb: 1000, leaderPartitions: 8 },
    { id: 3, az: 'us-east-1c', subnet: 'subnet-0aa4e710', endpoint: 'b-3.msk-prod.kafka.us-east-1.amazonaws.com:9098', cpu: 22, diskUsedGb: 250, diskTotalGb: 1000, leaderPartitions: 8 }
  ],
  telemetry: {
    ingressRate: 1420,
    egressRate: 2840,
    latencyMs: 3.2,
    underReplicatedPartitions: 0,
    totalLag: 18
  }
};

const TOPICS = [
  { name: 'orders.v1', partitions: 3, replication: 3, retentionHours: 168, cleanupPolicy: 'delete', messageCount: 34920, sizeMb: 184, status: 'Active' },
  { name: 'customer.complaints.v1', partitions: 3, replication: 3, retentionHours: 720, cleanupPolicy: 'delete', messageCount: 1420, sizeMb: 24, status: 'Active' },
  { name: 'complaints.resolved.v1', partitions: 3, replication: 3, retentionHours: 720, cleanupPolicy: 'compact', messageCount: 1395, sizeMb: 22, status: 'Active' },
  { name: 'telemetry.iot.v2', partitions: 6, replication: 3, retentionHours: 72, cleanupPolicy: 'delete', messageCount: 182410, sizeMb: 512, status: 'Active' },
  { name: 'fraud.detection.events', partitions: 3, replication: 3, retentionHours: 336, cleanupPolicy: 'compact', messageCount: 12300, sizeMb: 45, status: 'Active' },
  { name: 'payment.transactions', partitions: 3, replication: 3, retentionHours: 720, cleanupPolicy: 'compact', messageCount: 89140, sizeMb: 320, status: 'Active' },
  { name: 'user.clickstream', partitions: 3, replication: 3, retentionHours: 48, cleanupPolicy: 'delete', messageCount: 412890, sizeMb: 890, status: 'Active' }
];

// In-Memory Consumer Complaints Store
const COMPLAINTS = [
  {
    id: 'CMP-89211',
    customerName: 'Sarah Jenkins',
    customerEmail: 'sarah.j@example.com',
    category: 'BILLING_DOUBLE_CHARGE',
    orderId: 'ORD-98412',
    description: 'Account was debited $289.99 twice during checkout for high-performance SSD volume.',
    severity: 'CRITICAL',
    status: 'INGESTED',
    timestamp: new Date(Date.now() - 1000 * 60 * 12).toISOString(),
    sentimentScore: 0.92,
    aiRecommendation: 'INSTANT AUTO-REFUND APPROVED ($289.99) + 15% APOLOGY VOUCHER',
    partition: 1,
    offset: 11680,
    brokerId: 2
  },
  {
    id: 'CMP-89204',
    customerName: 'Alex Mercer',
    customerEmail: 'alex.m@example.com',
    category: 'DAMAGED_SHIPMENT',
    orderId: 'ORD-94110',
    description: 'Hardware cooling module arrived cracked with broken seal.',
    severity: 'HIGH',
    status: 'RESOLVED',
    timestamp: new Date(Date.now() - 1000 * 60 * 45).toISOString(),
    sentimentScore: 0.78,
    aiRecommendation: 'PRIORITY REPLACEMENT DISPATCHED + RETURN LABEL ISSUED',
    partition: 0,
    offset: 11674,
    brokerId: 1,
    resolution: 'Replacement unit expedited via FedEx Priority. Customer notified.'
  }
];

const CONSUMER_GROUPS = [
  {
    groupId: 'ecommerce-analytics-group',
    topic: 'orders.v1',
    state: 'Stable',
    protocol: 'RangeAssignor',
    members: 3,
    partitionsLag: [1, 2, 1],
    totalLag: 4,
    latencyMs: 3.2
  },
  {
    groupId: 'fraud-detection-worker',
    topic: 'payment.transactions',
    state: 'Stable',
    protocol: 'CooperativeSticky',
    members: 3,
    partitionsLag: [0, 1, 0],
    totalLag: 1,
    latencyMs: 1.8
  },
  {
    groupId: 's3-data-lake-sink',
    topic: 'user.clickstream',
    state: 'Stable',
    protocol: 'RangeAssignor',
    members: 2,
    partitionsLag: [4, 5, 2],
    totalLag: 11,
    latencyMs: 14.2
  },
  {
    groupId: 'iot-anomaly-tracker',
    topic: 'telemetry.iot.v2',
    state: 'Stable',
    protocol: 'RoundRobin',
    members: 4,
    partitionsLag: [1, 0, 0, 1, 0, 0],
    totalLag: 2,
    latencyMs: 2.1
  }
];

// Ring buffer of recent messages
const RECENT_MESSAGES = [];
const MAX_MESSAGES = 200;

// Topic partition offsets map
const PARTITION_OFFSETS = {
  'orders.v1': { 0: 11640, 1: 11640, 2: 11640 },
  'telemetry.iot.v2': { 0: 30400, 1: 30400, 2: 30400, 3: 30405, 4: 30402, 5: 30403 },
  'fraud.detection.events': { 0: 4100, 1: 4100, 2: 4100 },
  'payment.transactions': { 0: 29713, 1: 29713, 2: 29714 },
  'user.clickstream': { 0: 137630, 1: 137630, 2: 137630 }
};

// ==========================================
// 2. HELPER FUNCTIONS & PRODUCER LOGIC
// ==========================================

function hashString(str) {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) - hash) + str.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash);
}

function produceKafkaRecord(topicName, key, payload, headers = {}, customPartition = null) {
  const topic = TOPICS.find(t => t.name === topicName);
  if (!topic) {
    throw new Error(`Topic '${topicName}' not found`);
  }

  // Calculate Partition
  const partition = customPartition !== null 
    ? Number(customPartition) 
    : (hashString(key || '') % topic.partitions);

  // Advance Offset
  if (!PARTITION_OFFSETS[topicName]) PARTITION_OFFSETS[topicName] = {};
  if (!PARTITION_OFFSETS[topicName][partition]) PARTITION_OFFSETS[topicName][partition] = 1000;
  const offset = ++PARTITION_OFFSETS[topicName][partition];

  // Increment total topic message count & size
  topic.messageCount = (topic.messageCount || 0) + 1;
  const payloadBytes = Buffer.byteLength(JSON.stringify(payload));
  topic.sizeMb = +(topic.sizeMb + (payloadBytes / (1024 * 1024))).toFixed(2);

  // Realistic broker response calculation
  const brokerId = (partition % 3) + 1;
  const latencyMs = +(1.2 + Math.random() * 2.8).toFixed(2);

  const record = {
    id: 'rec_' + Math.random().toString(36).substring(2, 9),
    topic: topicName,
    partition,
    offset,
    key: key || `key_${Math.random().toString(36).substring(2, 8)}`,
    headers: {
      'trace-id': 'tr-' + Math.random().toString(36).substring(2, 8),
      'aws-region': 'us-east-1',
      ...headers
    },
    payload,
    brokerId,
    latencyMs,
    timestamp: new Date().toISOString()
  };

  // Add to ring buffer
  RECENT_MESSAGES.unshift(record);
  if (RECENT_MESSAGES.length > MAX_MESSAGES) {
    RECENT_MESSAGES.pop();
  }

  // Broadcast to all connected WebSocket clients
  broadcastWebSocket({
    type: 'EVENT_INGESTED',
    record,
    topicStats: {
      name: topic.name,
      messageCount: topic.messageCount,
      sizeMb: topic.sizeMb
    }
  });

  return record;
}

// Event sample generators for background simulator
const SAMPLE_GENERATORS = [
  // E-Commerce Order
  () => ({
    topic: 'orders.v1',
    key: 'ord_' + Math.floor(100000 + Math.random() * 900000),
    payload: {
      event_type: 'ORDER_PLACED',
      order_id: 'ord_' + Math.floor(100000 + Math.random() * 900000),
      customer: {
        id: 'cust_' + Math.floor(1000 + Math.random() * 9000),
        tier: ['GOLD', 'PLATINUM', 'STANDARD'][Math.floor(Math.random() * 3)]
      },
      items: [{ sku: 'AWS-EBS-1TB', qty: 1, price: 100 }],
      total_amount: +(49.99 + Math.random() * 250).toFixed(2),
      currency: 'USD',
      status: 'CONFIRMED'
    }
  }),
  // IoT Sensor Telemetry
  () => ({
    topic: 'telemetry.iot.v2',
    key: 'sensor_' + ['us_east', 'eu_central', 'ap_south'][Math.floor(Math.random() * 3)] + '_' + Math.floor(10 + Math.random() * 90),
    payload: {
      event_type: 'TELEMETRY_HEARTBEAT',
      device_id: 'sensor_node_' + Math.floor(100 + Math.random() * 900),
      metrics: {
        temp_c: +(21.0 + Math.random() * 11.5).toFixed(2),
        humidity_pct: +(40.0 + Math.random() * 35).toFixed(1),
        battery_pct: Math.floor(70 + Math.random() * 30)
      },
      status: 'OPTIMAL'
    }
  }),
  // Payment Transaction
  () => ({
    topic: 'payment.transactions',
    key: 'pay_' + Math.random().toString(36).substring(2, 10),
    payload: {
      event_type: 'PAYMENT_AUTHORIZED',
      payment_id: 'pi_' + Math.random().toString(36).substring(2, 12),
      amount: +(10.0 + Math.random() * 500).toFixed(2),
      currency: 'USD',
      processor: 'AWS_PAYMENT_GATEWAY',
      status: 'SETTLED'
    }
  }),
  // Fraud Anomaly
  () => ({
    topic: 'fraud.detection.events',
    key: 'tx_' + Math.floor(10000000 + Math.random() * 90000000),
    payload: {
      event_type: 'FRAUD_RISK_EVALUATION',
      risk_score: +(0.15 + Math.random() * 0.8).toFixed(2),
      anomaly_detected: Math.random() > 0.85,
      evaluated_by: 'sagemaker-risk-pipeline'
    }
  })
];

// Pre-populate initial records
for (let i = 0; i < 20; i++) {
  const gen = SAMPLE_GENERATORS[Math.floor(Math.random() * SAMPLE_GENERATORS.length)]();
  try {
    produceKafkaRecord(gen.topic, gen.key, gen.payload);
  } catch (e) {}
}

// ==========================================
// 3. REST API ENDPOINTS
// ==========================================

// Health Check
app.get('/api/health', (req, res) => {
  res.json({
    status: 'UP',
    service: 'Amazon MSK Event Streaming Backend',
    version: '2.4.0',
    timestamp: new Date().toISOString()
  });
});

// Cluster Overview & Telemetry
app.get('/api/cluster', (req, res) => {
  res.json({
    cluster: CLUSTER_STATE,
    topicCount: TOPICS.length,
    consumerGroupCount: CONSUMER_GROUPS.length,
    recentMessageCount: RECENT_MESSAGES.length
  });
});

// List Topics
app.get('/api/topics', (req, res) => {
  res.json({
    topics: TOPICS,
    totalCount: TOPICS.length
  });
});

// Create Topic
app.post('/api/topics', (req, res) => {
  const { name, partitions, replication, retentionHours, cleanupPolicy } = req.body;

  if (!name || typeof name !== 'string') {
    return res.status(400).json({ error: 'Topic name is required and must be a string' });
  }

  if (TOPICS.some(t => t.name.toLowerCase() === name.toLowerCase())) {
    return res.status(409).json({ error: `Topic '${name}' already exists` });
  }

  const newTopic = {
    name,
    partitions: Math.max(1, Math.min(24, parseInt(partitions, 10) || 3)),
    replication: Math.max(1, Math.min(3, parseInt(replication, 10) || 3)),
    retentionHours: Math.max(1, parseInt(retentionHours, 10) || 168),
    cleanupPolicy: cleanupPolicy === 'compact' ? 'compact' : 'delete',
    messageCount: 0,
    sizeMb: 0,
    status: 'Active'
  };

  TOPICS.push(newTopic);

  // Initialize partition offsets
  PARTITION_OFFSETS[name] = {};
  for (let p = 0; p < newTopic.partitions; p++) {
    PARTITION_OFFSETS[name][p] = 0;
  }

  broadcastWebSocket({
    type: 'TOPIC_CREATED',
    topic: newTopic
  });

  res.status(201).json({
    message: `Topic '${name}' created successfully on Amazon MSK`,
    topic: newTopic
  });
});

// Delete Topic
app.delete('/api/topics/:name', (req, res) => {
  const { name } = req.params;
  const index = TOPICS.findIndex(t => t.name === name);

  if (index === -1) {
    return res.status(404).json({ error: `Topic '${name}' not found` });
  }

  const deleted = TOPICS.splice(index, 1)[0];
  delete PARTITION_OFFSETS[name];

  broadcastWebSocket({
    type: 'TOPIC_DELETED',
    topicName: name
  });

  res.json({
    message: `Topic '${name}' deleted successfully`,
    topic: deleted
  });
});

// Produce Single Event
app.post('/api/produce', (req, res) => {
  const { topic, key, payload, headers, partition } = req.body;

  if (!topic || !payload) {
    return res.status(400).json({ error: 'Both "topic" and "payload" are required fields' });
  }

  try {
    const record = produceKafkaRecord(topic, key, payload, headers, partition);
    res.status(200).json({
      status: 'ACKNOWLEDGED',
      record,
      ackTimeMs: record.latencyMs
    });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

// Burst Produce Multiple Events
app.post('/api/produce/burst', (req, res) => {
  const count = Math.min(100, Math.max(1, parseInt(req.body.count, 10) || 25));
  const generatedRecords = [];

  for (let i = 0; i < count; i++) {
    const sample = SAMPLE_GENERATORS[Math.floor(Math.random() * SAMPLE_GENERATORS.length)]();
    try {
      const rec = produceKafkaRecord(sample.topic, sample.key, sample.payload, { 'batch-burst': 'true' });
      generatedRecords.push(rec);
    } catch (e) {}
  }

  res.json({
    status: 'BURST_COMPLETED',
    count: generatedRecords.length,
    recordsSample: generatedRecords.slice(0, 5)
  });
});

// Get Live Stream Messages (Filterable)
app.get('/api/messages', (req, res) => {
  const { topic, partition, limit, search } = req.query;
  let results = [...RECENT_MESSAGES];

  if (topic && topic !== 'ALL') {
    results = results.filter(m => m.topic === topic);
  }

  if (partition && partition !== 'ALL') {
    results = results.filter(m => String(m.partition) === String(partition));
  }

  if (search) {
    const q = search.toLowerCase();
    results = results.filter(m => {
      const full = (m.key + JSON.stringify(m.payload)).toLowerCase();
      return full.includes(q);
    });
  }

  const max = Math.min(200, Math.max(1, parseInt(limit, 10) || 50));
  res.json({
    count: results.length,
    messages: results.slice(0, max)
  });
});

// Get Consumer Groups
app.get('/api/consumer-groups', (req, res) => {
  res.json({
    consumerGroups: CONSUMER_GROUPS,
    totalCount: CONSUMER_GROUPS.length
  });
});

// Toggle Background Stream Simulator
app.post('/api/simulator/toggle', (req, res) => {
  CLUSTER_STATE.simulatorRunning = !CLUSTER_STATE.simulatorRunning;
  broadcastWebSocket({
    type: 'SIMULATOR_STATE',
    running: CLUSTER_STATE.simulatorRunning
  });
  res.json({ simulatorRunning: CLUSTER_STATE.simulatorRunning });
});
// Get All Consumer Complaints
app.get('/api/complaints', (req, res) => {
  res.json({
    count: COMPLAINTS.length,
    complaints: COMPLAINTS
  });
});

// Consumer Submits Complaint (Ingests to MSK & Broadcasts to Producer)
app.post('/api/complaints', (req, res) => {
  const { customerName, customerEmail, category, orderId, description, severity } = req.body;

  if (!customerEmail || !description) {
    return res.status(400).json({ error: 'Customer Email and Complaint Description are required.' });
  }

  const id = 'CMP-' + Math.floor(10000 + Math.random() * 90000);
  const finalCategory = category || 'BILLING_DOUBLE_CHARGE';
  const finalSeverity = severity || (finalCategory.includes('BILLING') ? 'CRITICAL' : 'HIGH');

  let recommendation = 'PRIORITY SUPPORT AGENT ASSIGNED + 10% SERVICE CREDIT';
  if (finalCategory === 'BILLING_DOUBLE_CHARGE') {
    recommendation = 'INSTANT AUTO-REFUND APPROVED ($289.99) + 15% APOLOGY VOUCHER';
  } else if (finalCategory === 'SERVICE_OUTAGE') {
    recommendation = 'AUTOMATED FAILOVER ROUTED + 25% SLA CREDIT APPLIED';
  } else if (finalCategory === 'DAMAGED_SHIPMENT') {
    recommendation = 'PRIORITY REPLACEMENT DISPATCHED + RETURN LABEL ISSUED';
  }

  const complaint = {
    id,
    customerName: customerName || 'Anonymous Customer',
    customerEmail,
    category: finalCategory,
    orderId: orderId || ('ORD-' + Math.floor(10000 + Math.random() * 90000)),
    description,
    severity: finalSeverity,
    status: 'INGESTED',
    timestamp: new Date().toISOString(),
    sentimentScore: +(0.85 + Math.random() * 0.12).toFixed(2),
    aiRecommendation: recommendation
  };

  // Ingest into Amazon MSK Kafka Topic
  let kafkaRecord = null;
  try {
    kafkaRecord = produceKafkaRecord(
      'customer.complaints.v1',
      customerEmail,
      complaint,
      { 'event-type': 'CUSTOMER_GRIEVANCE', 'client-role': 'CONSUMER' }
    );
    complaint.partition = kafkaRecord.partition;
    complaint.offset = kafkaRecord.offset;
    complaint.brokerId = kafkaRecord.brokerId;
  } catch (err) {
    console.error('Failed to write complaint to MSK:', err);
  }

  COMPLAINTS.unshift(complaint);

  // Broadcast to Producer in Real-Time via WebSockets
  broadcastWebSocket({
    type: 'NEW_CONSUMER_COMPLAINT',
    complaint
  });

  res.status(201).json({
    status: 'INGESTED_TO_MSK',
    complaint,
    kafkaRecord
  });
});

// Producer Resolves Complaint
app.post('/api/complaints/:id/resolve', (req, res) => {
  const { id } = req.params;
  const { resolution } = req.body;

  const complaint = COMPLAINTS.find(c => c.id === id);
  if (!complaint) {
    return res.status(404).json({ error: `Complaint '${id}' not found` });
  }

  complaint.status = 'RESOLVED';
  complaint.resolution = resolution || complaint.aiRecommendation;
  complaint.resolvedAt = new Date().toISOString();

  // Ingest resolution event into Kafka topic
  try {
    produceKafkaRecord(
      'complaints.resolved.v1',
      complaint.customerEmail,
      {
        complaintId: complaint.id,
        resolution: complaint.resolution,
        resolvedAt: complaint.resolvedAt,
        processedBy: 'PRODUCER_OPERATIONS'
      },
      { 'event-type': 'COMPLAINT_RESOLVED' }
    );
  } catch (e) {}

  // Broadcast resolution update to Consumer in real-time
  broadcastWebSocket({
    type: 'COMPLAINT_RESOLVED',
    complaintId: id,
    complaint
  });

  res.json({
    status: 'RESOLVED',
    complaint
  });
});

// Fallback to index.html for single-page routing
app.use((req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

// ==========================================
// 4. WEBSOCKET REAL-TIME BROADCAST ENGINE
// ==========================================

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

function broadcastWebSocket(data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(msg);
    }
  });
}

wss.on('connection', (ws) => {
  // Send initial state payload to new client
  ws.send(JSON.stringify({
    type: 'INIT_STATE',
    cluster: CLUSTER_STATE,
    topics: TOPICS,
    consumerGroups: CONSUMER_GROUPS,
    recentMessages: RECENT_MESSAGES.slice(0, 30),
    complaints: COMPLAINTS
  }));

  // Handle incoming commands from client over WebSocket
  ws.on('message', (message) => {
    try {
      const parsed = JSON.parse(message);
      if (parsed.action === 'PRODUCE') {
        produceKafkaRecord(parsed.topic, parsed.key, parsed.payload, parsed.headers);
      } else if (parsed.action === 'TOGGLE_SIMULATOR') {
        CLUSTER_STATE.simulatorRunning = !CLUSTER_STATE.simulatorRunning;
        broadcastWebSocket({
          type: 'SIMULATOR_STATE',
          running: CLUSTER_STATE.simulatorRunning
        });
      }
    } catch (e) {
      console.error('WS message error:', e.message);
    }
  });
});

// ==========================================
// 5. BACKGROUND TELEMETRY & SIMULATOR TICKER (STANDALONE ONLY)
// ==========================================

function startBackgroundTicker() {
  setInterval(() => {
    if (!CLUSTER_STATE.simulatorRunning) return;

    // Jitter velocity
    const jitter = Math.floor(Math.random() * 120) - 60;
    CLUSTER_STATE.telemetry.ingressRate = Math.max(900, Math.min(2800, CLUSTER_STATE.telemetry.ingressRate + jitter));
    CLUSTER_STATE.telemetry.egressRate = Math.round(CLUSTER_STATE.telemetry.ingressRate * (1.9 + Math.random() * 0.2));
    CLUSTER_STATE.telemetry.latencyMs = +(2.8 + Math.random() * 2.2).toFixed(2);

    // Subtle broker load variation
    CLUSTER_STATE.brokers.forEach(b => {
      const d = Math.floor(Math.random() * 5) - 2;
      b.cpu = Math.max(12, Math.min(85, b.cpu + d));
    });

    // Periodically ingest simulated event (60% chance every second)
    if (Math.random() > 0.4) {
      const sample = SAMPLE_GENERATORS[Math.floor(Math.random() * SAMPLE_GENERATORS.length)]();
      try {
        produceKafkaRecord(sample.topic, sample.key, sample.payload);
      } catch (e) {}
    }

    // Broadcast telemetry snapshot
    broadcastWebSocket({
      type: 'TELEMETRY_TICK',
      telemetry: CLUSTER_STATE.telemetry,
      brokers: CLUSTER_STATE.brokers
    });
  }, 1000);
}

// ==========================================
// 6. START SERVER & SERVERLESS EXPORT
// ==========================================

if (require.main === module) {
  startBackgroundTicker();
  server.listen(PORT, () => {
    console.log('================================================================');
    console.log(`🚀 Amazon MSK Event Streaming Backend & Web Console Started!`);
    console.log(`🌐 Web Console URL:    http://localhost:${PORT}`);
    console.log(`📡 WebSocket Endpoint:  ws://localhost:${PORT}`);
    console.log(`📋 REST API Base:      http://localhost:${PORT}/api/health`);
    console.log('================================================================');
  });
}

module.exports = app;
module.exports.server = server;

