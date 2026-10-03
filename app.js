/**
 * Amazon MSK (Managed Streaming for Apache Kafka) Console Engine
 * Connected Full-Stack Engine: WebSocket + REST API Client + Live Simulator
 */

// ==========================================
// 1. STATE & DATA MODELS
// ==========================================

const MSK_STATE = {
  backendConnected: false,
  ws: null,
  user: {
    role: localStorage.getItem('msk_user_role') || 'PRODUCER',
    email: localStorage.getItem('msk_user_email') || 'producer@amazonmsk.com'
  },
  complaints: [],

  cluster: {
    name: 'msk-prod-eventstream-01',
    arn: 'arn:aws:kafka:us-east-1:123456789012:cluster/msk-prod-eventstream-01/a1b2c3d4-e5f6',
    kafkaVersion: '3.6.0',
    mode: 'Provisioned KRaft',
    region: 'us-east-1',
    brokers: [
      { id: 1, az: 'us-east-1a', subnet: 'subnet-081af29b', endpoint: 'b-1.msk-prod.kafka.us-east-1.amazonaws.com:9098', cpu: 26, diskUsedGb: 242, diskTotalGb: 1000, leaderPartitions: 8 },
      { id: 2, az: 'us-east-1b', subnet: 'subnet-09f14b62', endpoint: 'b-2.msk-prod.kafka.us-east-1.amazonaws.com:9098', cpu: 29, diskUsedGb: 238, diskTotalGb: 1000, leaderPartitions: 8 },
      { id: 3, az: 'us-east-1c', subnet: 'subnet-0aa4e710', endpoint: 'b-3.msk-prod.kafka.us-east-1.amazonaws.com:9098', cpu: 22, diskUsedGb: 250, diskTotalGb: 1000, leaderPartitions: 8 }
    ]
  },

  topics: [
    { name: 'orders.v1', partitions: 3, replication: 3, retentionHours: 168, cleanupPolicy: 'delete', messageCount: 34920, sizeMb: 184, status: 'Active' },
    { name: 'telemetry.iot.v2', partitions: 6, replication: 3, retentionHours: 72, cleanupPolicy: 'delete', messageCount: 182410, sizeMb: 512, status: 'Active' },
    { name: 'fraud.detection.events', partitions: 3, replication: 3, retentionHours: 336, cleanupPolicy: 'compact', messageCount: 12300, sizeMb: 45, status: 'Active' },
    { name: 'payment.transactions', partitions: 3, replication: 3, retentionHours: 720, cleanupPolicy: 'compact', messageCount: 89140, sizeMb: 320, status: 'Active' },
    { name: 'user.clickstream', partitions: 3, replication: 3, retentionHours: 48, cleanupPolicy: 'delete', messageCount: 412890, sizeMb: 890, status: 'Active' }
  ],

  consumerGroups: [
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
  ],

  messages: [],
  maxMessagesToKeep: 150,

  simulatorRunning: true,
  consumerPaused: false,
  ingressRate: 1420,
  egressRate: 2840,
  currentOffsets: {
    'orders.v1': { 0: 11640, 1: 11640, 2: 11640 },
    'telemetry.iot.v2': { 0: 30400, 1: 30400, 2: 30400, 3: 30405, 4: 30402, 5: 30403 },
    'fraud.detection.events': { 0: 4100, 1: 4100, 2: 4100 },
    'payment.transactions': { 0: 29713, 1: 29713, 2: 29714 },
    'user.clickstream': { 0: 137630, 1: 137630, 2: 137630 }
  },

  topologyDetails: {
    producer_apps: {
      title: 'Web & Mobile Applications (Producers)',
      type: 'PRODUCER FLEET',
      desc: 'High-concurrency client applications dispatching user actions, authenticated via Amazon API Gateway and AWS IAM SigV4 credentials.',
      specs: [
        { label: 'Ingress Interface', val: 'Amazon API Gateway + REST' },
        { label: 'Batch Size', val: '64 KB / 10ms linger' },
        { label: 'Security', val: 'IAM Roles for Service Accounts (IRSA)' },
        { label: 'Throughput', val: '680 msgs/s' }
      ]
    },
    producer_iot: {
      title: 'AWS IoT Core Telemetry Fleet',
      type: 'PRODUCER FLEET',
      desc: 'Thousands of distributed smart sensors and edge devices publishing real-time telemetry over MQTT, routed directly to Amazon MSK topics via AWS IoT Rule Actions.',
      specs: [
        { label: 'Protocol', val: 'MQTT 5.0 (TLS Port 8883)' },
        { label: 'Routing Rule', val: 'SELECT * FROM "devices/+/telemetry"' },
        { label: 'Compression', val: 'Snappy Codec' },
        { label: 'Device Count', val: '12,450 Connected Nodes' }
      ]
    },
    producer_microservices: {
      title: 'E-Commerce Microservices (ECS Fargate)',
      type: 'PRODUCER FLEET',
      desc: 'Containerized order management and inventory services utilizing the Kafka Producer Java API with idempotent write guarantees (acks=all).',
      specs: [
        { label: 'Host Runtime', val: 'AWS Fargate (Containers)' },
        { label: 'Idempotence', val: 'enable.idempotence=true' },
        { label: 'Partitioner', val: 'Murmur2 Hash on Customer ID' },
        { label: 'Ack Latency', val: '2.1 ms average' }
      ]
    },
    producer_cdc: {
      title: 'Aurora PostgreSQL CDC (Debezium)',
      type: 'CDC PRODUCER',
      desc: 'Change Data Capture connector streaming row-level database transactions directly to Kafka topics in real time with exact commit timestamps.',
      specs: [
        { label: 'Connector', val: 'Debezium PostgreSQL Connector' },
        { label: 'Replication Slot', val: 'msk_pg_wal_slot_01' },
        { label: 'Format', val: 'JSON with Schema Envelope' },
        { label: 'Consistency', val: 'At-Least-Once Delivery' }
      ]
    },
    broker_1: {
      title: 'Broker 1 (b-1.msk-cluster.us-east-1a)',
      type: 'AMAZON MSK BROKER',
      desc: 'Active broker node deployed in Availability Zone us-east-1a. Hosts partition leaders and handles client producer/consumer requests with automated self-healing.',
      specs: [
        { label: 'Availability Zone', val: 'us-east-1a' },
        { label: 'Subnet ID', val: 'subnet-081af29b' },
        { label: 'EBS GP3 Volume', val: '1,000 GiB (24% Used)' },
        { label: 'Partition Leaders', val: '8 Active Leaders' }
      ]
    },
    broker_2: {
      title: 'Broker 2 (b-2.msk-cluster.us-east-1b)',
      type: 'AMAZON MSK BROKER',
      desc: 'Active broker node deployed in Availability Zone us-east-1b. Synchronizes replication streams across AZs with zero data loss guarantee.',
      specs: [
        { label: 'Availability Zone', val: 'us-east-1b' },
        { label: 'Subnet ID', val: 'subnet-09f14b62' },
        { label: 'EBS GP3 Volume', val: '1,000 GiB (23% Used)' },
        { label: 'Partition Leaders', val: '8 Active Leaders' }
      ]
    },
    broker_3: {
      title: 'Broker 3 (b-3.msk-cluster.us-east-1c)',
      type: 'AMAZON MSK BROKER',
      desc: 'Active broker node deployed in Availability Zone us-east-1c. Ensures high availability; cluster retains full read/write operations during any single AZ maintenance.',
      specs: [
        { label: 'Availability Zone', val: 'us-east-1c' },
        { label: 'Subnet ID', val: 'subnet-0aa4e710' },
        { label: 'EBS GP3 Volume', val: '1,000 GiB (25% Used)' },
        { label: 'Partition Leaders', val: '8 Active Leaders' }
      ]
    },
    kraft: {
      title: 'Kafka KRaft Metadata Controller Quorum',
      type: 'CONSENSUS QUORUM',
      desc: 'Modern Apache Kafka KRaft consensus protocol running as managed controllers within Amazon MSK. Eliminates external ZooKeeper dependencies and speeds up partition leader elections to under 100ms.',
      specs: [
        { label: 'Consensus Protocol', val: 'Raft-based KRaft (Kafka 3.6)' },
        { label: 'Controller Count', val: '3 Quorum Members' },
        { label: 'Failover Time', val: '< 90ms automatic' },
        { label: 'Metadata Log', val: '@metadata internal topic' }
      ]
    },
    consumer_lambda: {
      title: 'AWS Lambda (Event Source Mapping)',
      type: 'DOWNSTREAM CONSUMER',
      desc: 'Serverless functions triggered by Amazon MSK event batches. Scales automatically from 0 to 1,000 concurrent executions according to stream velocity.',
      specs: [
        { label: 'Batch Size', val: '100 records per invocation' },
        { label: 'Batch Window', val: '500 milliseconds' },
        { label: 'Runtime', val: 'Node.js 20.x / Python 3.12' },
        { label: 'Processing Delay', val: '3.2ms average' }
      ]
    },
    consumer_fargate: {
      title: 'Fraud Detection Engine (ECS Fargate)',
      type: 'DOWNSTREAM CONSUMER',
      desc: 'Real-time machine learning inference pipeline evaluating financial transactions against risk heuristics with sub-10ms latency.',
      specs: [
        { label: 'Consumer Group', val: 'fraud-detection-worker' },
        { label: 'Concurrency', val: '3 Workers (1 per partition)' },
        { label: 'Model', val: 'XGBoost Risk Classifier' },
        { label: 'Consumer Lag', val: '1 record (Healthy)' }
      ]
    },
    consumer_opensearch: {
      title: 'Amazon OpenSearch Service',
      type: 'SEARCH & ANALYTICS',
      desc: 'Real-time indexing sink providing full-text search, live log aggregation, and real-time operational observability dashboards.',
      specs: [
        { label: 'Cluster Engine', val: 'OpenSearch 2.11' },
        { label: 'Index Pattern', val: 'msk-events-yyyy.MM.dd' },
        { label: 'Ingestion Delay', val: '< 2.5 seconds' },
        { label: 'Retention', val: '30 Days with UltraWarm' }
      ]
    },
    consumer_s3: {
      title: 'Amazon S3 Data Lake (MSK Connect)',
      type: 'DATA LAKE ARCHIVE',
      desc: 'Managed Kafka Connect S3 Sink connector micro-batching raw event streams into columnar Apache Parquet files for long-term Athena and Redshift queries.',
      specs: [
        { label: 'Connector', val: 'MSK Connect S3 Sink v2.8' },
        { label: 'Storage Class', val: 'S3 Standard -> Glacier Instant' },
        { label: 'File Format', val: 'Apache Parquet (Snappy)' },
        { label: 'Flush Interval', val: '60 Seconds or 50 MB' }
      ]
    }
  }
};

// ==========================================
// 2. TEMPLATE GENERATORS
// ==========================================

const EVENT_TEMPLATES = {
  ecommerce: () => ({
    event_id: 'evt_' + Math.random().toString(36).substring(2, 9),
    event_type: 'ORDER_PLACED',
    order_id: 'ord_' + Math.floor(100000 + Math.random() * 900000),
    customer: {
      id: 'cust_' + Math.floor(1000 + Math.random() * 9000),
      tier: ['GOLD', 'PLATINUM', 'STANDARD'][Math.floor(Math.random() * 3)],
      email: 'user' + Math.floor(Math.random() * 500) + '@example.com'
    },
    items: [
      { sku: 'AWS-SKU-902', name: 'High-Performance SSD Volume', quantity: 2, price_usd: 120.00 },
      { sku: 'KAFKA-ENT-BOOK', name: 'Event Streaming Architecture Guide', quantity: 1, price_usd: 49.99 }
    ],
    total_amount: 289.99,
    currency: 'USD',
    timestamp: new Date().toISOString()
  }),

  iot: () => ({
    event_id: 'iot_' + Math.random().toString(36).substring(2, 9),
    event_type: 'TELEMETRY_HEARTBEAT',
    device_id: 'sensor_zone_' + (['us_east', 'eu_central', 'ap_south'][Math.floor(Math.random() * 3)]) + '_' + Math.floor(10 + Math.random() * 90),
    telemetry: {
      temperature_c: +(21.0 + Math.random() * 12.5).toFixed(2),
      humidity_pct: +(45.0 + Math.random() * 30.0).toFixed(1),
      pressure_hpa: +(1012.0 + (Math.random() * 8 - 4)).toFixed(1),
      battery_pct: Math.floor(65 + Math.random() * 35)
    },
    firmware_ver: 'v2.4.11',
    status: 'OPTIMAL',
    timestamp: new Date().toISOString()
  }),

  fraud: () => ({
    event_id: 'frd_' + Math.random().toString(36).substring(2, 9),
    event_type: 'FRAUD_RISK_EVALUATION',
    transaction_id: 'tx_' + Math.floor(10000000 + Math.random() * 90000000),
    risk_score: +(0.1 + Math.random() * 0.85).toFixed(2),
    anomaly_detected: Math.random() > 0.8,
    indicators: ['RAPID_GEO_SHIFT', 'UNUSUAL_VELOCITY'].slice(0, Math.floor(Math.random() * 2) + 1),
    evaluated_by: 'amazon-sagemaker-risk-endpoint',
    timestamp: new Date().toISOString()
  }),

  payment: () => ({
    event_id: 'pay_' + Math.random().toString(36).substring(2, 9),
    event_type: 'PAYMENT_AUTHORIZED',
    payment_id: 'pi_' + Math.random().toString(36).substring(2, 12),
    sender_account: 'acc_us_' + Math.floor(10000 + Math.random() * 90000),
    amount: +(15.00 + Math.random() * 850.00).toFixed(2),
    currency: 'USD',
    processor: 'AWS_PAYMENT_GATEWAY',
    status: 'SETTLED',
    timestamp: new Date().toISOString()
  }),

  clickstream: () => ({
    event_id: 'clk_' + Math.random().toString(36).substring(2, 9),
    event_type: 'USER_NAVIGATION',
    session_id: 'sess_' + Math.random().toString(36).substring(2, 10),
    user_id: 'usr_' + Math.floor(1000 + Math.random() * 8000),
    page_url: ['/catalog/cloud-servers', '/checkout/payment', '/dashboard/analytics'][Math.floor(Math.random() * 3)],
    referrer: 'https://aws.amazon.com/msk/',
    dwell_time_ms: Math.floor(1200 + Math.random() * 8500),
    timestamp: new Date().toISOString()
  })
};

// ==========================================
// 3. BACKEND WEBSOCKET & REST INTEGRATION
// ==========================================

function initBackendConnection() {
  const isHttp = window.location.protocol.startsWith('http');
  if (!isHttp) {
    updateBackendStatusBadge('LOCAL (STATIC)', false);
    return;
  }

  const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${wsProtocol}//${window.location.host}`;

  try {
    const ws = new WebSocket(wsUrl);
    MSK_STATE.ws = ws;

    ws.onopen = () => {
      MSK_STATE.backendConnected = true;
      updateBackendStatusBadge('LIVE (WS)', true);
      showToast('Connected to Amazon MSK Backend API', 'success');
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        handleBackendMessage(data);
      } catch (e) {
        console.error('Failed to parse WS payload', e);
      }
    };

    ws.onclose = () => {
      MSK_STATE.backendConnected = false;
      updateBackendStatusBadge('RECONNECTING...', false);
      // Attempt reconnect after 3s
      setTimeout(initBackendConnection, 3000);
    };

    ws.onerror = () => {
      MSK_STATE.backendConnected = false;
      updateBackendStatusBadge('OFFLINE', false);
    };
  } catch (err) {
    updateBackendStatusBadge('OFFLINE', false);
  }
}

function updateBackendStatusBadge(statusText, isOnline) {
  const pill = document.getElementById('backend-status-pill');
  const text = document.getElementById('backend-status-text');
  if (text) text.textContent = statusText;
  if (text) {
    text.className = `value ${isOnline ? 'success' : 'orange'}`;
  }
}

function handleBackendMessage(data) {
  switch (data.type) {
    case 'INIT_STATE':
      if (data.topics) {
        MSK_STATE.topics = data.topics;
        initTopicSelectors();
        initTopicsTable();
      }
      if (data.cluster) {
        MSK_STATE.cluster = data.cluster;
        initBrokerGrid();
      }
      if (data.recentMessages && data.recentMessages.length > 0) {
        data.recentMessages.forEach(rec => ingestMessage(rec));
      }
      if (data.complaints && Array.isArray(data.complaints)) {
        MSK_STATE.complaints = data.complaints;
        renderConsumerComplaints();
        renderProducerComplaintsInbox();
      }
      break;

    case 'NEW_CONSUMER_COMPLAINT':
      if (data.complaint) {
        const exists = MSK_STATE.complaints.some(c => c.id === data.complaint.id);
        if (!exists) {
          MSK_STATE.complaints.unshift(data.complaint);
        } else {
          const idx = MSK_STATE.complaints.findIndex(c => c.id === data.complaint.id);
          MSK_STATE.complaints[idx] = data.complaint;
        }
        renderConsumerComplaints();
        renderProducerComplaintsInbox();
        if (window.AUDIO_ENGINE) AUDIO_ENGINE.playProduceSound();
        showToast(`🚨 New Consumer Grievance #${data.complaint.id} received in Producer Inbox!`, 'warning');
      }
      break;

    case 'COMPLAINT_RESOLVED':
      if (data.complaint) {
        const target = MSK_STATE.complaints.find(c => c.id === data.complaint.id);
        if (target) {
          target.status = 'RESOLVED';
          target.resolution = data.complaint.resolution;
          target.resolvedAt = data.complaint.resolvedAt;
        } else {
          MSK_STATE.complaints.unshift(data.complaint);
        }
        renderConsumerComplaints();
        renderProducerComplaintsInbox();
        if (window.AUDIO_ENGINE) AUDIO_ENGINE.playAckSound();
        showToast(`✅ Grievance #${data.complaint.id} resolved by Producer!`, 'success');
      }
      break;

    case 'EVENT_INGESTED':
      ingestMessage(data.record);
      if (data.topicStats) {
        const targetTopic = MSK_STATE.topics.find(t => t.name === data.topicStats.name);
        if (targetTopic) {
          targetTopic.messageCount = data.topicStats.messageCount;
          targetTopic.sizeMb = data.topicStats.sizeMb;
          const countCell = document.getElementById(`topic-msg-count-${targetTopic.name.replace(/\./g, '-')}`);
          if (countCell) countCell.textContent = targetTopic.messageCount.toLocaleString();
        }
      }
      break;

    case 'TELEMETRY_TICK':
      if (data.telemetry) {
        MSK_STATE.ingressRate = data.telemetry.ingressRate;
        MSK_STATE.egressRate = data.telemetry.egressRate;

        const navIngress = document.getElementById('nav-ingress-rate');
        const cardMsgIn = document.getElementById('card-metric-msg-in');
        const cardMsgOut = document.getElementById('card-metric-msg-out');
        const cardBytesIn = document.getElementById('card-metric-bytes-in');
        const cardBytesOut = document.getElementById('card-metric-bytes-out');

        if (navIngress) navIngress.textContent = `${MSK_STATE.ingressRate.toLocaleString()} msgs/s`;
        if (cardMsgIn) cardMsgIn.textContent = MSK_STATE.ingressRate.toLocaleString();
        if (cardMsgOut) cardMsgOut.textContent = MSK_STATE.egressRate.toLocaleString();
        if (cardBytesIn) cardBytesIn.textContent = `${(MSK_STATE.ingressRate * 0.0105).toFixed(1)} MB/s`;
        if (cardBytesOut) cardBytesOut.textContent = `${(MSK_STATE.egressRate * 0.0104).toFixed(1)} MB/s`;

        if (window.chartThroughput) {
          window.chartThroughput.push([MSK_STATE.ingressRate, MSK_STATE.egressRate]);
        }
        if (window.chartLatency) {
          window.chartLatency.push([data.telemetry.latencyMs]);
        }
      }

      if (data.brokers) {
        data.brokers.forEach(b => {
          const cpuEl = document.getElementById(`broker-${b.id}-cpu`);
          const cpuBar = document.getElementById(`broker-${b.id}-cpu-bar`);
          if (cpuEl && cpuBar) {
            cpuEl.textContent = `${b.cpu}%`;
            cpuBar.style.width = `${b.cpu}%`;
          }
        });
      }
      break;

    case 'TOPIC_CREATED':
      if (!MSK_STATE.topics.some(t => t.name === data.topic.name)) {
        MSK_STATE.topics.push(data.topic);
        initTopicSelectors();
        initTopicsTable();
      }
      break;

    case 'TOPIC_DELETED':
      MSK_STATE.topics = MSK_STATE.topics.filter(t => t.name !== data.topicName);
      initTopicSelectors();
      initTopicsTable();
      break;

    case 'SIMULATOR_STATE':
      MSK_STATE.simulatorRunning = data.running;
      updateTrafficToggleButton();
      break;
  }
}

// ==========================================
// 4. UI INITIALIZATION & EVENT LISTENERS
// ==========================================

document.addEventListener('DOMContentLoaded', () => {
  initTabs();
  initTopicSelectors();
  initBrokerGrid();
  initTopicsTable();
  initConsumerGroups();
  initProducerForm();
  initInspectorControls();
  initModals();
  initCharts();
  initSimulator();
  initBackendConnection();
  initAuthSystem();
  initComplaintsSystem();

  loadTemplate('ecommerce');

  document.getElementById('btn-quick-produce').addEventListener('click', () => {
    switchTab('tab-producer');
  });

  document.getElementById('btn-toggle-stream-sim').addEventListener('click', toggleStreamSimulator);

  document.getElementById('btn-copy-bootstrap').addEventListener('click', () => {
    const endpoints = MSK_STATE.cluster.brokers.map(b => b.endpoint).join(',');
    navigator.clipboard.writeText(endpoints).then(() => {
      showToast('Bootstrap brokers copied to clipboard!', 'success');
    }).catch(() => {
      showToast('Endpoints: ' + endpoints, 'info');
    });
  });

  window.showTopologyDetail = showTopologyDetail;
});

// ==========================================
// 5. TAB NAVIGATION
// ==========================================

function initTabs() {
  const tabButtons = document.querySelectorAll('.nav-item');
  tabButtons.forEach(btn => {
    btn.addEventListener('click', () => {
      const targetTab = btn.getAttribute('data-tab');
      switchTab(targetTab);
    });
  });
}

function switchTab(tabId) {
  document.querySelectorAll('.nav-item').forEach(btn => {
    if (btn.getAttribute('data-tab') === tabId) {
      btn.classList.add('active');
    } else {
      btn.classList.remove('active');
    }
  });

  document.querySelectorAll('.tab-pane').forEach(pane => {
    if (pane.id === tabId) {
      pane.classList.add('active');
    } else {
      pane.classList.remove('active');
    }
  });

  if (tabId === 'tab-dashboard' && window.chartThroughput) {
    window.chartThroughput.draw();
    window.chartLatency.draw();
  }
}

// ==========================================
// 6. BROKER GRID RENDERING
// ==========================================

function initBrokerGrid() {
  const container = document.getElementById('broker-grid-container');
  container.innerHTML = '';

  MSK_STATE.cluster.brokers.forEach(broker => {
    const diskPct = Math.round((broker.diskUsedGb / broker.diskTotalGb) * 100);
    const card = document.createElement('div');
    card.className = 'broker-card';
    card.id = `broker-card-${broker.id}`;
    card.innerHTML = `
      <div class="broker-header">
        <span class="broker-id">Broker ${broker.id}</span>
        <span class="broker-az-tag">${broker.az}</span>
      </div>
      <div class="broker-stat-bar-group">
        <div class="bar-label-wrap">
          <span>CPU Utilization</span>
          <strong id="broker-${broker.id}-cpu">${broker.cpu}%</strong>
        </div>
        <div class="progress-track">
          <div class="progress-fill fill-cyan" id="broker-${broker.id}-cpu-bar" style="width: ${broker.cpu}%"></div>
        </div>
      </div>
      <div class="broker-stat-bar-group">
        <div class="bar-label-wrap">
          <span>EBS Storage Used</span>
          <strong>${broker.diskUsedGb} / ${broker.diskTotalGb} GiB (${diskPct}%)</strong>
        </div>
        <div class="progress-track">
          <div class="progress-fill fill-emerald" style="width: ${diskPct}%"></div>
        </div>
      </div>
      <div class="cluster-info-row mt-10">
        <span>Subnet:</span>
        <strong style="font-family: var(--font-code); font-size: 0.72rem;">${broker.subnet}</strong>
      </div>
      <div class="cluster-info-row">
        <span>Partitions Hosted:</span>
        <strong>${broker.leaderPartitions} Leaders (ISR: 100%)</strong>
      </div>
    `;
    container.appendChild(card);
  });
}

function updateBrokerGridRealtime() {
  if (MSK_STATE.backendConnected) return; // Managed by backend websocket

  MSK_STATE.cluster.brokers.forEach(broker => {
    const cpuEl = document.getElementById(`broker-${broker.id}-cpu`);
    const cpuBar = document.getElementById(`broker-${broker.id}-cpu-bar`);
    if (cpuEl && cpuBar) {
      const jitter = Math.floor(Math.random() * 5) - 2;
      const newCpu = Math.max(12, Math.min(85, broker.cpu + jitter));
      broker.cpu = newCpu;
      cpuEl.textContent = `${newCpu}%`;
      cpuBar.style.width = `${newCpu}%`;
      if (newCpu > 70) {
        cpuBar.className = 'progress-fill fill-orange';
      } else {
        cpuBar.className = 'progress-fill fill-cyan';
      }
    }
  });
}

// ==========================================
// 7. TOPICS & CONSUMER GROUPS TABLES
// ==========================================

function initTopicSelectors() {
  const prodSelect = document.getElementById('producer-topic-select');
  const inspectSelect = document.getElementById('inspector-topic-filter');

  prodSelect.innerHTML = '';
  inspectSelect.innerHTML = '<option value="ALL">All Topics</option>';

  MSK_STATE.topics.forEach(t => {
    const opt1 = document.createElement('option');
    opt1.value = t.name;
    opt1.textContent = `${t.name} (${t.partitions} partitions)`;
    prodSelect.appendChild(opt1);

    const opt2 = document.createElement('option');
    opt2.value = t.name;
    opt2.textContent = t.name;
    inspectSelect.appendChild(opt2);
  });

  const countEl = document.getElementById('sidebar-topic-count');
  if (countEl) countEl.textContent = MSK_STATE.topics.length;
}

function initTopicsTable() {
  const tbody = document.getElementById('topics-table-body');
  tbody.innerHTML = '';

  MSK_STATE.topics.forEach(t => {
    const tr = document.createElement('tr');
    tr.id = `topic-row-${t.name.replace(/\./g, '-')}`;
    tr.innerHTML = `
      <td><strong style="color: var(--kafka-cyan); font-family: var(--font-code);">${t.name}</strong></td>
      <td>${t.partitions}</td>
      <td>${t.replication} (All AZs)</td>
      <td>${t.retentionHours} hrs</td>
      <td><span style="font-family: var(--font-code); font-size: 0.78rem;">${t.cleanupPolicy}</span></td>
      <td id="topic-msg-count-${t.name.replace(/\./g, '-')}">${t.messageCount.toLocaleString()}</td>
      <td>${t.sizeMb} MB</td>
      <td><span class="badge-status-active"><span class="status-dot"></span> ${t.status}</span></td>
      <td>
        <button class="btn-secondary btn-sm" onclick="quickProduceToTopic('${t.name}')">Produce</button>
      </td>
    `;
    tbody.appendChild(tr);
  });
}

window.quickProduceToTopic = function(topicName) {
  const select = document.getElementById('producer-topic-select');
  select.value = topicName;
  switchTab('tab-producer');
  showToast(`Switched target to topic: ${topicName}`, 'info');
};

function initConsumerGroups() {
  const container = document.getElementById('consumer-groups-container');
  container.innerHTML = '';

  MSK_STATE.consumerGroups.forEach(cg => {
    const card = document.createElement('div');
    card.className = 'consumer-card';
    card.id = `cg-card-${cg.groupId}`;

    const lagPct = Math.min(100, Math.max(5, cg.totalLag * 5));
    const isHigh = cg.totalLag > 10;

    card.innerHTML = `
      <div class="consumer-card-header">
        <div>
          <div class="consumer-group-id">${cg.groupId}</div>
          <div style="font-size: 0.75rem; color: var(--text-muted); margin-top: 2px;">
            Topic: <span style="color: var(--kafka-cyan);">${cg.topic}</span>
          </div>
        </div>
        <span class="consumer-state">${cg.state}</span>
      </div>

      <div class="lag-progress-container">
        <div class="lag-title-row">
          <span>Total Offset Lag</span>
          <strong id="cg-lag-${cg.groupId}">${cg.totalLag} msgs</strong>
        </div>
        <div class="lag-progress-bar">
          <div class="lag-fill ${isHigh ? 'high-lag' : ''}" id="cg-lag-fill-${cg.groupId}" style="width: ${lagPct}%;"></div>
        </div>
      </div>

      <div class="consumer-meta-grid">
        <div class="meta-sub-item">
          <span>PARTITIONS</span>
          <strong>${cg.partitionsLag.length} Assigned</strong>
        </div>
        <div class="meta-sub-item">
          <span>MEMBERS</span>
          <strong>${cg.members} Workers</strong>
        </div>
        <div class="meta-sub-item">
          <span>LATENCY</span>
          <strong>${cg.latencyMs} ms</strong>
        </div>
      </div>
    `;
    container.appendChild(card);
  });
}

// ==========================================
// 8. EVENT PRODUCER STUDIO (BACKEND CONNECTED)
// ==========================================

function initProducerForm() {
  const form = document.getElementById('producer-form');
  const templateSelect = document.getElementById('template-select');
  const editor = document.getElementById('producer-payload-editor');
  const btnFormat = document.getElementById('btn-format-json');
  const btnReset = document.getElementById('btn-reset-payload');
  const btnRandomKey = document.getElementById('btn-gen-random-key');
  const btnBurst = document.getElementById('btn-burst-publish');
  const btnClearAcks = document.getElementById('btn-clear-acks');

  templateSelect.addEventListener('change', (e) => {
    loadTemplate(e.target.value);
  });

  editor.addEventListener('input', () => {
    validateAndSizeJson();
  });

  btnFormat.addEventListener('click', () => {
    try {
      const parsed = JSON.parse(editor.value);
      editor.value = JSON.stringify(parsed, null, 2);
      validateAndSizeJson();
      showToast('JSON payload formatted', 'info');
    } catch (err) {
      showToast('Cannot format invalid JSON: ' + err.message, 'danger');
    }
  });

  btnReset.addEventListener('click', () => {
    loadTemplate(templateSelect.value);
  });

  btnRandomKey.addEventListener('click', () => {
    const prefix = templateSelect.value === 'iot' ? 'device_' : (templateSelect.value === 'ecommerce' ? 'order_' : 'key_');
    document.getElementById('producer-partition-key').value = prefix + Math.floor(10000 + Math.random() * 90000);
  });

  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    await publishFromProducerStudio();
  });

  btnBurst.addEventListener('click', async () => {
    await burstPublishEvents(25);
  });

  btnClearAcks.addEventListener('click', () => {
    document.getElementById('producer-ack-feed').innerHTML = '';
  });
}

function loadTemplate(templateKey) {
  const editor = document.getElementById('producer-payload-editor');
  const keyInput = document.getElementById('producer-partition-key');
  const topicSelect = document.getElementById('producer-topic-select');

  let gen = EVENT_TEMPLATES[templateKey] || EVENT_TEMPLATES.ecommerce;
  const payload = gen();
  editor.value = JSON.stringify(payload, null, 2);

  if (templateKey === 'ecommerce') {
    topicSelect.value = 'orders.v1';
    keyInput.value = payload.order_id;
  } else if (templateKey === 'iot') {
    topicSelect.value = 'telemetry.iot.v2';
    keyInput.value = payload.device_id;
  } else if (templateKey === 'fraud') {
    topicSelect.value = 'fraud.detection.events';
    keyInput.value = payload.transaction_id;
  } else if (templateKey === 'payment') {
    topicSelect.value = 'payment.transactions';
    keyInput.value = payload.payment_id;
  } else if (templateKey === 'clickstream') {
    topicSelect.value = 'user.clickstream';
    keyInput.value = payload.user_id;
  }

  validateAndSizeJson();
}

function validateAndSizeJson() {
  const editor = document.getElementById('producer-payload-editor');
  const validIndicator = document.getElementById('json-valid-indicator');
  const byteSizeEl = document.getElementById('payload-byte-size');

  const text = editor.value;
  const bytes = new Blob([text]).size;
  byteSizeEl.textContent = bytes > 1024 ? `${(bytes / 1024).toFixed(1)} KB` : `${bytes} bytes`;

  try {
    JSON.parse(text);
    validIndicator.textContent = 'Valid JSON';
    validIndicator.className = 'json-validation-badge';
    return true;
  } catch (e) {
    validIndicator.textContent = 'Invalid JSON';
    validIndicator.className = 'json-validation-badge invalid';
    return false;
  }
}

async function publishFromProducerStudio() {
  const topic = document.getElementById('producer-topic-select').value;
  const key = document.getElementById('producer-partition-key').value || ('key_' + Math.random().toString(36).substring(2, 8));
  const editor = document.getElementById('producer-payload-editor');
  let headers = {};

  try {
    const rawHeaders = document.getElementById('producer-headers').value;
    headers = rawHeaders ? JSON.parse(rawHeaders) : {};
  } catch (err) {
    showToast('Invalid headers JSON. Proceeding with empty headers.', 'warning');
  }

  let payload;
  try {
    payload = JSON.parse(editor.value);
  } catch (err) {
    showToast('Cannot publish: JSON payload is invalid!', 'danger');
    return;
  }

  // Attempt Backend REST Publish
  if (MSK_STATE.backendConnected) {
    try {
      const res = await fetch('/api/produce', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ topic, key, payload, headers })
      });
      const data = await res.json();
      if (res.ok && data.record) {
        renderAckCard(data.record);
        showToast(`[Backend ACK] Published to ${topic} [P${data.record.partition}, Offset #${data.record.offset}] in ${data.record.latencyMs}ms`, 'success');
        return;
      }
    } catch (e) {
      console.warn('REST publish failed, falling back to local simulation:', e);
    }
  }

  // Fallback Local Publish
  const topicObj = MSK_STATE.topics.find(t => t.name === topic) || { partitions: 3 };
  const partition = Math.abs(hashString(key)) % topicObj.partitions;
  
  if (!MSK_STATE.currentOffsets[topic]) MSK_STATE.currentOffsets[topic] = {};
  if (!MSK_STATE.currentOffsets[topic][partition]) MSK_STATE.currentOffsets[topic][partition] = 1000;
  const offset = ++MSK_STATE.currentOffsets[topic][partition];

  const ackLatency = +(1.5 + Math.random() * 3.0).toFixed(2);
  const brokerId = (partition % 3) + 1;
  const timestamp = new Date().toISOString();

  const record = {
    id: 'rec_' + Math.random().toString(36).substring(2, 9),
    topic,
    partition,
    offset,
    key,
    headers,
    payload,
    brokerId,
    latencyMs: ackLatency,
    timestamp
  };

  ingestMessage(record);
  renderAckCard(record);

  topicObj.messageCount = (topicObj.messageCount || 0) + 1;
  const countCell = document.getElementById(`topic-msg-count-${topic.replace(/\./g, '-')}`);
  if (countCell) countCell.textContent = topicObj.messageCount.toLocaleString();

  showToast(`Event published to ${topic} [P${partition}, Offset #${offset}] in ${ackLatency}ms`, 'success');
}

function renderAckCard(record) {
  const feed = document.getElementById('producer-ack-feed');
  const card = document.createElement('div');
  card.className = 'ack-card';
  card.innerHTML = `
    <div class="ack-header">
      <span class="ack-topic">${record.topic}</span>
      <span class="ack-latency">ACK ${record.latencyMs} ms &bull; Broker-${record.brokerId}</span>
    </div>
    <div class="ack-meta">
      <span>Part: <strong>${record.partition}</strong></span>
      <span>Offset: <strong>#${record.offset}</strong></span>
      <span class="ack-key">Key: ${record.key}</span>
    </div>
  `;
  feed.insertBefore(card, feed.firstChild);

  if (feed.children.length > 30) {
    feed.removeChild(feed.lastChild);
  }
}

async function burstPublishEvents(count = 25) {
  const btnBurst = document.getElementById('btn-burst-publish');
  btnBurst.disabled = true;
  btnBurst.textContent = `Publishing ${count}...`;

  if (MSK_STATE.backendConnected) {
    try {
      const res = await fetch('/api/produce/burst', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ count })
      });
      const data = await res.json();
      btnBurst.disabled = false;
      btnBurst.textContent = '⚡ Burst 25 Events';
      showToast(`Backend burst completed: ${data.count} events ingested across MSK brokers`, 'success');
      return;
    } catch (e) {
      console.warn('Backend burst failed, using local generator:', e);
    }
  }

  let sent = 0;
  const interval = setInterval(() => {
    if (sent >= count) {
      clearInterval(interval);
      btnBurst.disabled = false;
      btnBurst.textContent = '⚡ Burst 25 Events';
      showToast(`Burst complete: 25 events ingested across MSK brokers`, 'success');
      return;
    }

    const topic = MSK_STATE.topics[Math.floor(Math.random() * MSK_STATE.topics.length)];
    const templateKeys = Object.keys(EVENT_TEMPLATES);
    const template = EVENT_TEMPLATES[templateKeys[Math.floor(Math.random() * templateKeys.length)]]();
    const key = 'burst_' + Math.random().toString(36).substring(2, 8);
    const partition = Math.floor(Math.random() * topic.partitions);

    if (!MSK_STATE.currentOffsets[topic.name]) MSK_STATE.currentOffsets[topic.name] = {};
    if (!MSK_STATE.currentOffsets[topic.name][partition]) MSK_STATE.currentOffsets[topic.name][partition] = 2000;
    const offset = ++MSK_STATE.currentOffsets[topic.name][partition];

    const record = {
      id: 'rec_' + Math.random().toString(36).substring(2, 9),
      topic: topic.name,
      partition,
      offset,
      key,
      headers: { 'producer': 'burst-sim', 'batch-id': 'b-491' },
      payload: template,
      brokerId: (partition % 3) + 1,
      latencyMs: +(1.2 + Math.random() * 2.5).toFixed(2),
      timestamp: new Date().toISOString()
    };

    ingestMessage(record);
    renderAckCard(record);
    sent++;
  }, 40);
}

// ==========================================
// 9. LIVE STREAM INSPECTOR
// ==========================================

function initInspectorControls() {
  const topicFilter = document.getElementById('inspector-topic-filter');
  const partitionFilter = document.getElementById('inspector-partition-filter');
  const searchInput = document.getElementById('inspector-search-input');
  const btnPause = document.getElementById('btn-toggle-consumer-pause');
  const btnClear = document.getElementById('btn-clear-inspector');
  const btnExport = document.getElementById('btn-export-inspector');

  topicFilter.addEventListener('change', renderStreamTable);
  partitionFilter.addEventListener('change', renderStreamTable);
  searchInput.addEventListener('input', debounce(renderStreamTable, 150));

  btnPause.addEventListener('click', () => {
    MSK_STATE.consumerPaused = !MSK_STATE.consumerPaused;
    const icon = document.getElementById('pause-icon');
    const text = document.getElementById('pause-btn-text');
    if (MSK_STATE.consumerPaused) {
      icon.textContent = '▶️';
      text.textContent = 'Resume Stream';
      showToast('Live stream inspector paused', 'warning');
    } else {
      icon.textContent = '⏸️';
      text.textContent = 'Pause Stream';
      showToast('Live stream inspector resumed', 'info');
      renderStreamTable();
    }
  });

  btnClear.addEventListener('click', () => {
    MSK_STATE.messages = [];
    renderStreamTable();
    showToast('Captured stream cleared', 'info');
  });

  btnExport.addEventListener('click', () => {
    if (MSK_STATE.messages.length === 0) {
      showToast('No messages to export!', 'warning');
      return;
    }
    const dataStr = 'data:text/json;charset=utf-8,' + encodeURIComponent(JSON.stringify(MSK_STATE.messages, null, 2));
    const dlAnchor = document.createElement('a');
    dlAnchor.setAttribute('href', dataStr);
    dlAnchor.setAttribute('download', `msk-stream-export-${Date.now()}.json`);
    dlAnchor.click();
    showToast('Exported ' + MSK_STATE.messages.length + ' records to JSON file', 'success');
  });
}

function ingestMessage(record) {
  MSK_STATE.messages.unshift(record);
  if (MSK_STATE.messages.length > MSK_STATE.maxMessagesToKeep) {
    MSK_STATE.messages.pop();
  }

  const counter = document.getElementById('inspector-msg-counter');
  if (counter) counter.textContent = `${MSK_STATE.messages.length} messages captured`;

  if (!MSK_STATE.consumerPaused) {
    prependStreamTableRow(record);
  }
}

function prependStreamTableRow(record) {
  const tbody = document.getElementById('stream-table-body');
  if (!tbody) return;

  const selectedTopic = document.getElementById('inspector-topic-filter').value;
  const selectedPartition = document.getElementById('inspector-partition-filter').value;
  const search = document.getElementById('inspector-search-input').value.toLowerCase();

  if (selectedTopic !== 'ALL' && record.topic !== selectedTopic) return;
  if (selectedPartition !== 'ALL' && String(record.partition) !== selectedPartition) return;
  if (search) {
    const raw = (record.key + JSON.stringify(record.payload)).toLowerCase();
    if (!raw.includes(search)) return;
  }

  const tr = createStreamRowElement(record, true);
  tbody.insertBefore(tr, tbody.firstChild);

  if (tbody.children.length > 100) {
    tbody.removeChild(tbody.lastChild);
  }
}

function renderStreamTable() {
  const tbody = document.getElementById('stream-table-body');
  if (!tbody) return;
  tbody.innerHTML = '';

  const selectedTopic = document.getElementById('inspector-topic-filter').value;
  const selectedPartition = document.getElementById('inspector-partition-filter').value;
  const search = document.getElementById('inspector-search-input').value.toLowerCase();

  const filtered = MSK_STATE.messages.filter(record => {
    if (selectedTopic !== 'ALL' && record.topic !== selectedTopic) return false;
    if (selectedPartition !== 'ALL' && String(record.partition) !== selectedPartition) return false;
    if (search) {
      const raw = (record.key + JSON.stringify(record.payload)).toLowerCase();
      if (!raw.includes(search)) return false;
    }
    return true;
  });

  filtered.slice(0, 100).forEach(rec => {
    tbody.appendChild(createStreamRowElement(rec, false));
  });
}

function createStreamRowElement(record, flash = false) {
  const tr = document.createElement('tr');
  if (flash) tr.className = 'new-flash';

  const timeStr = record.timestamp.includes('T') ? record.timestamp.split('T')[1].replace('Z', '') : record.timestamp;
  const previewStr = JSON.stringify(record.payload);
  const partClass = record.partition === 0 ? 'part-0' : (record.partition === 1 ? 'part-1' : 'part-2');

  tr.innerHTML = `
    <td>${timeStr}</td>
    <td><span style="color: var(--kafka-cyan); font-weight: 600;">${record.topic}</span></td>
    <td><span class="tag-part ${partClass}">P${record.partition}</span></td>
    <td>#${record.offset}</td>
    <td style="color: #f1f5f9;">${record.key}</td>
    <td><div class="payload-preview">${escapeHtml(previewStr)}</div></td>
    <td><button class="btn-secondary btn-sm" onclick="openMessageDetail('${record.id}')">Inspect</button></td>
  `;

  tr.addEventListener('click', (e) => {
    if (e.target.tagName !== 'BUTTON') {
      openMessageDetail(record.id);
    }
  });

  return tr;
}

// ==========================================
// 10. MODALS & TOPOLOGY INSPECTION
// ==========================================

function initModals() {
  const modalMsg = document.getElementById('modal-msg-detail');
  const btnCloseMsg = document.getElementById('btn-close-msg-detail');
  const btnDoneMsg = document.getElementById('btn-done-msg-detail');
  const btnCopyMsg = document.getElementById('btn-copy-record-json');

  const closeMsg = () => modalMsg.classList.remove('open');
  btnCloseMsg.addEventListener('click', closeMsg);
  btnDoneMsg.addEventListener('click', closeMsg);
  modalMsg.addEventListener('click', (e) => {
    if (e.target === modalMsg) closeMsg();
  });

  btnCopyMsg.addEventListener('click', () => {
    const json = document.getElementById('modal-payload-code').textContent;
    navigator.clipboard.writeText(json).then(() => {
      showToast('Payload copied to clipboard', 'success');
    });
  });

  const modalTopic = document.getElementById('modal-create-topic');
  const btnOpenTopic = document.getElementById('btn-open-create-topic-modal');
  const btnCloseTopic = document.getElementById('btn-close-create-topic');
  const btnCancelTopic = document.getElementById('btn-cancel-create-topic');
  const formCreateTopic = document.getElementById('create-topic-form');

  const closeTopic = () => modalTopic.classList.remove('open');
  btnOpenTopic.addEventListener('click', () => modalTopic.classList.add('open'));
  btnCloseTopic.addEventListener('click', closeTopic);
  btnCancelTopic.addEventListener('click', closeTopic);
  modalTopic.addEventListener('click', (e) => {
    if (e.target === modalTopic) closeTopic();
  });

  formCreateTopic.addEventListener('submit', async (e) => {
    e.preventDefault();
    const name = document.getElementById('topic-name-input').value.trim();
    const partitions = parseInt(document.getElementById('topic-partitions-input').value, 10);
    const replication = parseInt(document.getElementById('topic-replication-input').value, 10);
    const retentionHours = parseInt(document.getElementById('topic-retention-hours').value, 10);
    const cleanupPolicy = document.getElementById('topic-cleanup-policy').value;

    if (MSK_STATE.backendConnected) {
      try {
        const res = await fetch('/api/topics', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name, partitions, replication, retentionHours, cleanupPolicy })
        });
        const data = await res.json();
        if (res.ok) {
          closeTopic();
          formCreateTopic.reset();
          showToast(`Kafka Topic "${name}" provisioned on Amazon MSK Backend!`, 'success');
          return;
        } else {
          showToast(data.error || 'Failed to create topic', 'danger');
          return;
        }
      } catch (err) {
        console.warn('Backend topic creation failed, using local:', err);
      }
    }

    if (MSK_STATE.topics.some(t => t.name === name)) {
      showToast(`Topic "${name}" already exists!`, 'danger');
      return;
    }

    const newTopic = {
      name,
      partitions,
      replication,
      retentionHours,
      cleanupPolicy,
      messageCount: 0,
      sizeMb: 0,
      status: 'Active'
    };

    MSK_STATE.topics.push(newTopic);
    initTopicSelectors();
    initTopicsTable();
    closeTopic();
    formCreateTopic.reset();
    showToast(`Kafka Topic "${name}" provisioned successfully!`, 'success');
  });
}

window.openMessageDetail = function(recordId) {
  const record = MSK_STATE.messages.find(m => m.id === recordId);
  if (!record) return;

  document.getElementById('msg-detail-title').textContent = `Message Offset #${record.offset} (${record.topic})`;
  document.getElementById('modal-meta-topic').textContent = record.topic;
  document.getElementById('modal-meta-partition').textContent = record.partition;
  document.getElementById('modal-meta-offset').textContent = record.offset;
  document.getElementById('modal-meta-broker').textContent = `Broker ${record.brokerId}`;
  document.getElementById('modal-meta-timestamp').textContent = record.timestamp;
  document.getElementById('modal-meta-key').textContent = record.key;

  document.getElementById('modal-headers-code').textContent = JSON.stringify(record.headers, null, 2);
  document.getElementById('modal-payload-code').textContent = JSON.stringify(record.payload, null, 2);

  document.getElementById('modal-msg-detail').classList.add('open');
};

function showTopologyDetail(nodeKey) {
  const item = MSK_STATE.topologyDetails[nodeKey];
  if (!item) return;

  const box = document.getElementById('topology-detail-box');
  document.getElementById('topo-node-badge').textContent = item.type;
  document.getElementById('topo-node-name').textContent = item.title;
  document.getElementById('topo-node-desc').textContent = item.desc;

  const specsGrid = document.getElementById('topo-specs-grid');
  specsGrid.innerHTML = '';
  item.specs.forEach(spec => {
    const div = document.createElement('div');
    div.className = 'spec-item';
    div.innerHTML = `<span>${spec.label}:</span> <strong>${spec.val}</strong>`;
    specsGrid.appendChild(div);
  });

  box.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

// ==========================================
// 11. REAL-TIME CANVAS CHARTS
// ==========================================

function initCharts() {
  const throughputCanvas = document.getElementById('realtimeThroughputChart');
  const latencyCanvas = document.getElementById('realtimeLatencyChart');

  if (!throughputCanvas || !latencyCanvas) return;

  window.chartThroughput = new RollingLineChart(throughputCanvas, {
    labels: ['Ingress msgs/s', 'Egress msgs/s'],
    colors: ['#00f2fe', '#10b981'],
    pointsCount: 30,
    minVal: 1000,
    maxVal: 3500
  });

  window.chartLatency = new RollingLineChart(latencyCanvas, {
    labels: ['Latency (ms)'],
    colors: ['#ff9900'],
    pointsCount: 30,
    minVal: 1,
    maxVal: 20
  });
}

class RollingLineChart {
  constructor(canvas, opts) {
    this.canvas = canvas;
    this.ctx = canvas.getContext('2d');
    this.opts = opts;
    this.series = opts.labels.map(() => Array(opts.pointsCount).fill(0));
    
    for (let i = 0; i < opts.pointsCount; i++) {
      if (opts.labels.length === 2) {
        this.series[0][i] = 1350 + Math.random() * 200;
        this.series[1][i] = 2700 + Math.random() * 350;
      } else {
        this.series[0][i] = 3.2 + Math.random() * 2.8;
      }
    }

    this.resize();
    window.addEventListener('resize', () => this.resize());
    this.draw();
  }

  resize() {
    const rect = this.canvas.parentElement.getBoundingClientRect();
    this.canvas.width = rect.width;
    this.canvas.height = 200;
    this.draw();
  }

  push(values) {
    values.forEach((val, idx) => {
      this.series[idx].shift();
      this.series[idx].push(val);
    });
    this.draw();
  }

  draw() {
    const { ctx, canvas, series, opts } = this;
    const w = canvas.width;
    const h = canvas.height;
    ctx.clearRect(0, 0, w, h);

    ctx.strokeStyle = 'rgba(255, 255, 255, 0.05)';
    ctx.lineWidth = 1;
    for (let y = 30; y < h; y += 40) {
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(w, y);
      ctx.stroke();
    }

    let min = Infinity;
    let max = -Infinity;
    series.forEach(s => {
      s.forEach(v => {
        if (v < min) min = v;
        if (v > max) max = v;
      });
    });
    min = Math.max(0, min * 0.85);
    max = max * 1.15;
    if (min === max) max += 10;

    series.forEach((s, idx) => {
      const color = opts.colors[idx];
      const step = w / (s.length - 1);

      ctx.beginPath();
      s.forEach((val, i) => {
        const x = i * step;
        const y = h - ((val - min) / (max - min)) * (h - 20) - 10;
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      });

      ctx.strokeStyle = color;
      ctx.lineWidth = 2.5;
      ctx.stroke();

      const lastX = (s.length - 1) * step;
      ctx.lineTo(lastX, h);
      ctx.lineTo(0, h);
      ctx.closePath();

      const grad = ctx.createLinearGradient(0, 0, 0, h);
      grad.addColorStop(0, color + '2b');
      grad.addColorStop(1, color + '00');
      ctx.fillStyle = grad;
      ctx.fill();
    });
  }
}

// ==========================================
// 12. FALLBACK STANDALONE SIMULATOR
// ==========================================

function initSimulator() {
  if (MSK_STATE.backendConnected) return;

  for (let i = 0; i < 10; i++) {
    simulateIncomingMessage();
  }

  setInterval(() => {
    if (!MSK_STATE.simulatorRunning || MSK_STATE.backendConnected) return;

    const jitter = Math.floor(Math.random() * 140) - 70;
    MSK_STATE.ingressRate = Math.max(800, Math.min(2600, MSK_STATE.ingressRate + jitter));
    MSK_STATE.egressRate = Math.round(MSK_STATE.ingressRate * (1.9 + Math.random() * 0.2));

    const navIngress = document.getElementById('nav-ingress-rate');
    const cardMsgIn = document.getElementById('card-metric-msg-in');
    const cardMsgOut = document.getElementById('card-metric-msg-out');
    const cardBytesIn = document.getElementById('card-metric-bytes-in');
    const cardBytesOut = document.getElementById('card-metric-bytes-out');

    if (navIngress) navIngress.textContent = `${MSK_STATE.ingressRate.toLocaleString()} msgs/s`;
    if (cardMsgIn) cardMsgIn.textContent = MSK_STATE.ingressRate.toLocaleString();
    if (cardMsgOut) cardMsgOut.textContent = MSK_STATE.egressRate.toLocaleString();
    if (cardBytesIn) cardBytesIn.textContent = `${(MSK_STATE.ingressRate * 0.0105).toFixed(1)} MB/s`;
    if (cardBytesOut) cardBytesOut.textContent = `${(MSK_STATE.egressRate * 0.0104).toFixed(1)} MB/s`;

    if (window.chartThroughput) {
      window.chartThroughput.push([MSK_STATE.ingressRate, MSK_STATE.egressRate]);
    }
    if (window.chartLatency) {
      const lat = +(3.2 + Math.random() * 2.2).toFixed(2);
      window.chartLatency.push([lat]);
    }

    updateBrokerGridRealtime();

    if (Math.random() > 0.3) {
      simulateIncomingMessage();
    }
  }, 1000);
}

function simulateIncomingMessage() {
  const topic = MSK_STATE.topics[Math.floor(Math.random() * MSK_STATE.topics.length)];
  const keys = Object.keys(EVENT_TEMPLATES);
  const templateFn = EVENT_TEMPLATES[keys[Math.floor(Math.random() * keys.length)]];
  const payload = templateFn();

  const key = payload.device_id || payload.order_id || payload.transaction_id || payload.user_id || 'key_' + Math.random().toString(36).substring(2, 7);
  const partition = Math.floor(Math.random() * topic.partitions);

  if (!MSK_STATE.currentOffsets[topic.name]) MSK_STATE.currentOffsets[topic.name] = {};
  if (!MSK_STATE.currentOffsets[topic.name][partition]) MSK_STATE.currentOffsets[topic.name][partition] = 2000;
  const offset = ++MSK_STATE.currentOffsets[topic.name][partition];

  const record = {
    id: 'rec_' + Math.random().toString(36).substring(2, 9),
    topic: topic.name,
    partition,
    offset,
    key,
    headers: { 'env': 'production', 'aws-region': 'us-east-1', 'schema-id': 'glue-18' },
    payload,
    brokerId: (partition % 3) + 1,
    latencyMs: +(2.0 + Math.random() * 2.5).toFixed(2),
    timestamp: new Date().toISOString()
  };

  ingestMessage(record);
}

function toggleStreamSimulator() {
  if (MSK_STATE.backendConnected) {
    fetch('/api/simulator/toggle', { method: 'POST' }).catch(() => {});
  } else {
    MSK_STATE.simulatorRunning = !MSK_STATE.simulatorRunning;
    updateTrafficToggleButton();
  }
}

function updateTrafficToggleButton() {
  const btn = document.getElementById('btn-toggle-stream-sim');
  const txt = document.getElementById('traffic-toggle-text');

  if (MSK_STATE.simulatorRunning) {
    btn.className = 'btn-traffic-toggle';
    txt.textContent = 'Simulate Traffic: ON';
    showToast('Background event stream generator started', 'success');
  } else {
    btn.className = 'btn-traffic-toggle off';
    txt.textContent = 'Simulate Traffic: PAUSED';
    showToast('Background event stream generator paused', 'warning');
  }
}

// ==========================================
// 13. UTILITY HELPERS
// ==========================================

function showToast(message, type = 'info') {
  const container = document.getElementById('toast-container');
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `<span>${message}</span>`;
  container.appendChild(toast);

  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transform = 'translateY(10px)';
    toast.style.transition = 'all 0.3s ease';
    setTimeout(() => toast.remove(), 300);
  }, 3200);
}

function hashString(str) {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) - hash) + str.charCodeAt(i);
    hash |= 0;
  }
  return hash;
}

function debounce(func, wait) {
  let timeout;
  return function executedFunction(...args) {
    const later = () => {
      clearTimeout(timeout);
      func(...args);
    };
    clearTimeout(timeout);
    timeout = setTimeout(later, wait);
  };
}

function escapeHtml(string) {
  return String(string)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// ==========================================
// 14. WEB AUDIO AMBIENT MUSIC & SOUND SYNTHESIZER
// ==========================================

const AUDIO_ENGINE = {
  ctx: null,
  enabled: false,
  musicTimer: null,
  musicStep: 0,
  scale: [261.63, 311.13, 349.23, 392.00, 466.16, 523.25, 622.25], // C minor pentatonic

  init() {
    if (!this.ctx) {
      const AudioCtx = window.AudioContext || window.webkitAudioContext;
      if (AudioCtx) this.ctx = new AudioCtx();
    }
    if (this.ctx && this.ctx.state === 'suspended') {
      this.ctx.resume();
    }
  },

  toggle() {
    this.init();
    this.enabled = !this.enabled;
    const btn = document.getElementById('btn-toggle-sound');
    const icon = document.getElementById('sound-icon');

    if (this.enabled) {
      btn.classList.add('active');
      btn.innerHTML = `<span id="sound-icon">🔊</span> Sound: ON`;
      this.startAmbientMusic();
      this.playAckSound();
      showToast('Futuristic Ambient Audio & SFX: ACTIVATED', 'success');
    } else {
      btn.classList.remove('active');
      btn.innerHTML = `<span id="sound-icon">🔈</span> Sound: OFF`;
      this.stopAmbientMusic();
      showToast('Audio muted', 'info');
    }
  },

  playTone(freq, duration = 0.15, type = 'sine', gainVal = 0.08) {
    if (!this.enabled || !this.ctx) return;
    try {
      const osc = this.ctx.createOscillator();
      const gain = this.ctx.createGain();
      osc.type = type;
      osc.frequency.setValueAtTime(freq, this.ctx.currentTime);
      gain.gain.setValueAtTime(gainVal, this.ctx.currentTime);
      gain.gain.exponentialRampToValueAtTime(0.0001, this.ctx.currentTime + duration);
      osc.connect(gain);
      gain.connect(this.ctx.destination);
      osc.start();
      osc.stop(this.ctx.currentTime + duration);
    } catch (e) {}
  },

  playProduceSound() {
    if (!this.enabled) return;
    this.playTone(523.25, 0.12, 'sine', 0.06);
    setTimeout(() => this.playTone(659.25, 0.14, 'sine', 0.06), 70);
    setTimeout(() => this.playTone(783.99, 0.22, 'sine', 0.08), 140);
  },

  playAckSound() {
    if (!this.enabled) return;
    this.playTone(880.00, 0.1, 'triangle', 0.05);
    setTimeout(() => this.playTone(1318.51, 0.28, 'sine', 0.07), 80);
  },

  playStepSound() {
    if (!this.enabled) return;
    this.playTone(392.00, 0.15, 'sine', 0.05);
    setTimeout(() => this.playTone(587.33, 0.25, 'triangle', 0.05), 100);
  },

  startAmbientMusic() {
    if (this.musicTimer) clearInterval(this.musicTimer);
    this.musicTimer = setInterval(() => {
      if (!this.enabled) return;
      const note = this.scale[Math.floor(Math.random() * this.scale.length)];
      this.playTone(note, 0.8, 'sine', 0.025);
      if (Math.random() > 0.5) {
        setTimeout(() => {
          const highNote = this.scale[Math.floor(Math.random() * this.scale.length)] * 2;
          this.playTone(highNote, 0.5, 'sine', 0.015);
        }, 300);
      }
    }, 1400);
  },

  stopAmbientMusic() {
    if (this.musicTimer) {
      clearInterval(this.musicTimer);
      this.musicTimer = null;
    }
  }
};

// ==========================================
// 15. AUTOMATED PRESENTATION & GUIDED TOUR ENGINE
// ==========================================

const AUTOPILOT = {
  running: false,
  paused: false,
  currentStepIndex: 0,
  stepTimeout: null,

  steps: [
    {
      title: '1. Amazon MSK Multi-AZ Cluster Overview',
      desc: 'Observing the active Apache Kafka 3.6.0 cluster. Notice 3 brokers running across Availability Zones us-east-1a, 1b, and 1c with live CPU metrics, GP3 EBS storage, and rolling CloudWatch velocity charts.',
      duration: 6500,
      async action() {
        switchTab('tab-dashboard');
        AUDIO_ENGINE.playStepSound();
      }
    },
    {
      title: '2. Real-Time Event Producer Studio',
      desc: 'Publishing a live event directly into the Kafka topic. The engine computes Murmur2 partition key hashing, attaches headers, and confirms receipt with sub-5ms broker acknowledgment.',
      duration: 7000,
      async action() {
        switchTab('tab-producer');
        loadTemplate('iot');
        document.getElementById('producer-partition-key').value = 'sensor_node_alpha_9';
        validateAndSizeJson();
        AUDIO_ENGINE.playProduceSound();
        await new Promise(r => setTimeout(r, 1200));
        await publishFromProducerStudio();
        AUDIO_ENGINE.playAckSound();
      }
    },
    {
      title: '3. High-Throughput Burst Event Ingestion',
      desc: 'Simulating high-concurrency traffic: ingesting a burst of 25 events across multiple partitions. Watch the ACKs panel log instantaneous partition leader responses.',
      duration: 6500,
      async action() {
        switchTab('tab-producer');
        AUDIO_ENGINE.playProduceSound();
        await burstPublishEvents(25);
        AUDIO_ENGINE.playAckSound();
      }
    },
    {
      title: '4. Live Stream Consumer Inspector',
      desc: 'Downstream consumer feed updating in real time via WebSockets. We can inspect individual Kafka packets, examine partition allocation (P0, P1, P2), and review record metadata.',
      duration: 7500,
      async action() {
        switchTab('tab-inspector');
        AUDIO_ENGINE.playStepSound();
        await new Promise(r => setTimeout(r, 1500));
        // Open inspector on the newest message
        if (MSK_STATE.messages.length > 0) {
          openMessageDetail(MSK_STATE.messages[0].id);
          AUDIO_ENGINE.playAckSound();
          await new Promise(r => setTimeout(r, 3000));
          document.getElementById('modal-msg-detail').classList.remove('open');
        }
      }
    },
    {
      title: '5. Managed Kafka Topic Provisioning',
      desc: 'Dynamic topic management: creating a new Kafka topic "analytics.fraud.stream.v1" with 6 partitions and 7-day retention across all 3 AWS Availability Zones.',
      duration: 7000,
      async action() {
        switchTab('tab-topics');
        AUDIO_ENGINE.playStepSound();
        const modal = document.getElementById('modal-create-topic');
        modal.classList.add('open');
        document.getElementById('topic-name-input').value = 'analytics.fraud.stream.v1';
        document.getElementById('topic-partitions-input').value = '6';
        await new Promise(r => setTimeout(r, 1800));

        // Submit creation
        const form = document.getElementById('create-topic-form');
        form.dispatchEvent(new Event('submit'));
        AUDIO_ENGINE.playAckSound();
      }
    },
    {
      title: '6. Consumer Groups & Partition Lag Monitoring',
      desc: 'Observing downstream consumers (e.g. ecommerce-analytics, fraud-worker, s3-data-lake-sink). Visualizing partition assignments and monitoring lag to guarantee zero message processing delays.',
      duration: 6500,
      async action() {
        switchTab('tab-consumers');
        AUDIO_ENGINE.playStepSound();
      }
    },
    {
      title: '7. Amazon MSK Cloud Architecture & Data Flow',
      desc: 'Interactive architectural view: Producers (IoT, Apps, Microservices) -> Amazon MSK Multi-AZ Cluster (KRaft Quorum) -> Downstream Consumers (AWS Lambda, Fargate, S3 Lake).',
      duration: 7500,
      async action() {
        switchTab('tab-topology');
        AUDIO_ENGINE.playStepSound();
        showTopologyDetail('broker_1');
        await new Promise(r => setTimeout(r, 2500));
        showTopologyDetail('consumer_lambda');
        AUDIO_ENGINE.playAckSound();
      }
    }
  ],

  start() {
    this.running = true;
    this.paused = false;
    this.currentStepIndex = 0;

    const banner = document.getElementById('autopilot-banner');
    const startBtn = document.getElementById('btn-start-autopilot');
    const btnText = document.getElementById('autopilot-btn-text');
    banner.classList.add('active');
    startBtn.classList.add('running');
    btnText.textContent = '⏹ Stop Tour';

    showToast('🎬 Guided Tour started! Sit back and watch the workflow.', 'info');
    this.executeCurrentStep();
  },

  stop() {
    this.running = false;
    this.paused = false;
    if (this.stepTimeout) clearTimeout(this.stepTimeout);

    const banner = document.getElementById('autopilot-banner');
    const startBtn = document.getElementById('btn-start-autopilot');
    const btnText = document.getElementById('autopilot-btn-text');
    banner.classList.remove('active');
    startBtn.classList.remove('running');
    btnText.textContent = '🎬 Start Guided Tour';

    showToast('Guided Tour ended.', 'info');
  },

  togglePause() {
    this.paused = !this.paused;
    const btn = document.getElementById('ap-btn-pause-resume');
    if (this.paused) {
      btn.textContent = '▶ Resume';
      if (this.stepTimeout) clearTimeout(this.stepTimeout);
      showToast('Tour paused', 'warning');
    } else {
      btn.textContent = '⏸ Pause';
      showToast('Tour resumed', 'info');
      this.scheduleNextStep(2000);
    }
  },

  nextStep() {
    if (this.stepTimeout) clearTimeout(this.stepTimeout);
    if (this.currentStepIndex < this.steps.length - 1) {
      this.currentStepIndex++;
      this.executeCurrentStep();
    } else {
      this.completeTour();
    }
  },

  prevStep() {
    if (this.stepTimeout) clearTimeout(this.stepTimeout);
    if (this.currentStepIndex > 0) {
      this.currentStepIndex--;
      this.executeCurrentStep();
    }
  },

  async executeCurrentStep() {
    const step = this.steps[this.currentStepIndex];
    if (!step) return;

    // Update Banner UI
    document.getElementById('ap-step-num').textContent = `Step ${this.currentStepIndex + 1} of ${this.steps.length}`;
    document.getElementById('ap-step-title').textContent = step.title;
    document.getElementById('ap-step-desc').textContent = step.desc;
    const pct = Math.round(((this.currentStepIndex + 1) / this.steps.length) * 100);
    document.getElementById('ap-progress-fill').style.width = `${pct}%`;

    // Execute step action
    await step.action();

    // Schedule next
    if (this.running && !this.paused) {
      this.scheduleNextStep(step.duration);
    }
  },

  scheduleNextStep(delay) {
    if (this.stepTimeout) clearTimeout(this.stepTimeout);
    this.stepTimeout = setTimeout(() => {
      if (!this.running || this.paused) return;
      if (this.currentStepIndex < this.steps.length - 1) {
        this.currentStepIndex++;
        this.executeCurrentStep();
      } else {
        this.completeTour();
      }
    }, delay);
  },

  completeTour() {
    AUDIO_ENGINE.playAckSound();
    showToast('🎉 Guided Tour Complete! You are fully prepared for your review!', 'success');
    setTimeout(() => {
      this.stop();
    }, 3500);
  }
};

// Hook up Autopilot & Sound buttons on DOM ready
document.addEventListener('DOMContentLoaded', () => {
  const btnStartTour = document.getElementById('btn-start-autopilot');
  if (btnStartTour) {
    btnStartTour.addEventListener('click', () => {
      if (AUTOPILOT.running) {
        AUTOPILOT.stop();
      } else {
        AUTOPILOT.start();
      }
    });
  }

  const btnSound = document.getElementById('btn-toggle-sound');
  if (btnSound) {
    btnSound.addEventListener('click', () => {
      AUDIO_ENGINE.toggle();
    });
  }

  const apNext = document.getElementById('ap-btn-next');
  const apPrev = document.getElementById('ap-btn-prev');
  const apPause = document.getElementById('ap-btn-pause-resume');
  const apExit = document.getElementById('ap-btn-exit');

  if (apNext) apNext.addEventListener('click', () => AUTOPILOT.nextStep());
  if (apPrev) apPrev.addEventListener('click', () => AUTOPILOT.prevStep());
  if (apPause) apPause.addEventListener('click', () => AUTOPILOT.togglePause());
  if (apExit) apExit.addEventListener('click', () => AUTOPILOT.stop());

  const btnComplaint = document.getElementById('btn-simulate-complaint');
  if (btnComplaint) {
    btnComplaint.addEventListener('click', simulateCustomerComplaintWorkflow);
  }
});

async function simulateCustomerComplaintWorkflow() {
  AUDIO_ENGINE.playProduceSound();
  const compId = 'CMP-' + Math.floor(10000 + Math.random() * 90000);
  const custId = 'cust_' + Math.floor(1000 + Math.random() * 9000);
  const complaints = [
    { text: "Charged twice for order #90214 ($289.99), money debited but cart shows transaction failed!", category: "BILLING_DOUBLE_CHARGE", severity: "CRITICAL", rec: "INSTANT AUTO-REFUND APPROVED ($289.99) + 15% APOLOGY VOUCHER" },
    { text: "Production database replica disconnected with high replication lag in us-east-1a.", category: "CLOUD_INCIDENT", severity: "HIGH", rec: "FAILOVER ROUTED TO US-EAST-1B + $50 SERVICE SLA CREDIT" },
    { text: "High-value enterprise order delayed beyond 24-hour SLA window.", category: "DELIVERY_DELAY", severity: "HIGH", rec: "PRIORITY EXPRESS COURIER DISPATCHED + DEDICATED AGENT ASSIGNED" }
  ];
  const item = complaints[Math.floor(Math.random() * complaints.length)];

  const record = {
    id: 'rec_' + Math.random().toString(36).substring(2, 9),
    topic: 'orders.v1',
    partition: Math.floor(Math.random() * 3),
    offset: Math.floor(11700 + Math.random() * 100),
    key: custId,
    headers: { 'event-type': 'CUSTOMER_GRIEVANCE', 'ai-classifier': 'sagemaker-nlp' },
    payload: {
      complaint_id: compId,
      customer_id: custId,
      category: item.category,
      description: item.text,
      sentiment_score: 0.94,
      priority: item.severity,
      action_recommended: item.rec,
      timestamp: new Date().toISOString()
    },
    brokerId: (Math.floor(Math.random() * 3) + 1),
    latencyMs: +(1.8 + Math.random() * 1.5).toFixed(2),
    timestamp: new Date().toISOString()
  };

  ingestMessage(record);
  renderAckCard(record);

  const box = document.getElementById('complaint-live-result');
  if (box) {
    box.style.display = 'block';
    document.getElementById('live-comp-id').textContent = `Complaint #${compId} Ingested (${item.category})`;
    document.getElementById('live-comp-severity').textContent = `${item.severity} PRIORITY`;
    document.getElementById('live-comp-text').textContent = `"${item.text}"`;
    document.getElementById('live-comp-rec').textContent = item.rec;
    document.getElementById('live-comp-obs').textContent = `Amazon MSK ingested complaint across Broker ${record.brokerId} [Partition ${record.partition}, Offset #${record.offset}] in ${record.latencyMs}ms. Downstream AI consumer triaged sentiment in 6.4ms. Total end-to-end resolution: ~8.2ms with zero packet drops.`;
    box.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }

  AUDIO_ENGINE.playAckSound();
  showToast(`Complaint #${compId} triaged & auto-resolved in 8.2ms!`, 'success');
}

// ==========================================
// 16. AUTHENTICATION & ROLE SWITCHING ENGINE
// ==========================================

function initAuthSystem() {
  const modalAuth = document.getElementById('modal-auth-login');
  const btnOpenLogin = document.getElementById('btn-open-login');
  const btnCloseLogin = document.getElementById('btn-close-auth-login');
  const btnRoleConsumer = document.getElementById('btn-select-consumer-role');
  const btnRoleProducer = document.getElementById('btn-select-producer-role');
  const authForm = document.getElementById('auth-login-form');
  const emailInput = document.getElementById('auth-email-input');
  const passwordInput = document.getElementById('auth-password-input');
  const btnAutofillConsumer = document.getElementById('btn-autofill-consumer');
  const btnAutofillProducer = document.getElementById('btn-autofill-producer');

  let selectedRole = (MSK_STATE.user && MSK_STATE.user.role) || 'PRODUCER';

  function updateRoleSelectorUI(role) {
    selectedRole = role;
    if (role === 'CONSUMER') {
      if (btnRoleConsumer) btnRoleConsumer.classList.add('active');
      if (btnRoleProducer) btnRoleProducer.classList.remove('active');
      if (emailInput && (!emailInput.value || emailInput.value.includes('producer'))) {
        emailInput.value = 'consumer@customer.com';
      }
    } else {
      if (btnRoleProducer) btnRoleProducer.classList.add('active');
      if (btnRoleConsumer) btnRoleConsumer.classList.remove('active');
      if (emailInput && (!emailInput.value || emailInput.value.includes('consumer'))) {
        emailInput.value = 'producer@amazonmsk.com';
      }
    }
  }

  // Open & Close
  if (btnOpenLogin) {
    btnOpenLogin.addEventListener('click', () => {
      updateRoleSelectorUI(MSK_STATE.user.role || 'PRODUCER');
      if (modalAuth) modalAuth.classList.add('open');
    });
  }

  if (btnCloseLogin) {
    btnCloseLogin.addEventListener('click', () => {
      if (modalAuth) modalAuth.classList.remove('open');
    });
  }

  if (modalAuth) {
    modalAuth.addEventListener('click', (e) => {
      if (e.target === modalAuth) {
        modalAuth.classList.remove('open');
      }
    });
  }

  // Role toggle buttons
  if (btnRoleConsumer) {
    btnRoleConsumer.addEventListener('click', () => updateRoleSelectorUI('CONSUMER'));
  }
  if (btnRoleProducer) {
    btnRoleProducer.addEventListener('click', () => updateRoleSelectorUI('PRODUCER'));
  }

  // Autofill helpers
  if (btnAutofillConsumer) {
    btnAutofillConsumer.addEventListener('click', () => {
      updateRoleSelectorUI('CONSUMER');
      if (emailInput) emailInput.value = 'consumer@customer.com';
      if (passwordInput) passwordInput.value = 'msk-secure-pass-2026';
      showToast('Auto-filled Consumer credentials', 'info');
    });
  }

  if (btnAutofillProducer) {
    btnAutofillProducer.addEventListener('click', () => {
      updateRoleSelectorUI('PRODUCER');
      if (emailInput) emailInput.value = 'producer@amazonmsk.com';
      if (passwordInput) passwordInput.value = 'msk-secure-pass-2026';
      showToast('Auto-filled Producer credentials', 'info');
    });
  }

  // Form submit
  if (authForm) {
    authForm.addEventListener('submit', (e) => {
      e.preventDefault();
      const email = (emailInput && emailInput.value.trim()) || 'user@amazonmsk.com';
      const password = passwordInput && passwordInput.value;

      if (!password) {
        showToast('Please enter password', 'danger');
        return;
      }

      setAuthUser(selectedRole, email);
      if (modalAuth) modalAuth.classList.remove('open');

      // Switch view context based on role
      if (selectedRole === 'CONSUMER') {
        switchTab('tab-consumer-portal');
        const formEmail = document.getElementById('comp-cust-email');
        if (formEmail) formEmail.value = email;
        showToast(`Signed in as CONSUMER (${email}). Consumer Portal Active!`, 'success');
      } else {
        switchTab('tab-producer');
        showToast(`Signed in as PRODUCER (${email}). Producer Studio & Complaints Inbox Active!`, 'success');
      }

      if (window.AUDIO_ENGINE) AUDIO_ENGINE.playAckSound();
    });
  }

  // Apply initial user state on load
  setAuthUser(MSK_STATE.user.role, MSK_STATE.user.email);
}

function setAuthUser(role, email) {
  MSK_STATE.user = { role, email };
  try {
    localStorage.setItem('msk_user_role', role);
    localStorage.setItem('msk_user_email', email);
  } catch (e) {}

  const roleLabel = document.getElementById('user-role-label');
  const emailLabel = document.getElementById('user-email-label');
  const avatarIcon = document.getElementById('user-avatar-icon');

  if (roleLabel) roleLabel.textContent = role;
  if (emailLabel) emailLabel.textContent = email;
  if (avatarIcon) avatarIcon.textContent = role === 'CONSUMER' ? '🧑‍💼' : '⚙️';

  const badge = document.getElementById('user-profile-badge');
  if (badge) {
    if (role === 'CONSUMER') {
      badge.style.borderColor = 'rgba(16, 185, 129, 0.5)';
      badge.style.background = 'rgba(16, 185, 129, 0.12)';
    } else {
      badge.style.borderColor = 'rgba(255, 153, 0, 0.5)';
      badge.style.background = 'rgba(255, 153, 0, 0.12)';
    }
  }

  if (role === 'CONSUMER') {
    const custEmail = document.getElementById('comp-cust-email');
    if (custEmail) custEmail.value = email;
  }
}

// ==========================================
// 17. CONSUMER GRIEVANCES & PRODUCER RESOLVER PIPELINE
// ==========================================

function initComplaintsSystem() {
  const compForm = document.getElementById('consumer-complaint-form');
  const btnRefreshConsumer = document.getElementById('btn-refresh-consumer-complaints');
  const btnRefreshProducer = document.getElementById('btn-refresh-producer-inbox');

  // Submit Complaint as Consumer
  if (compForm) {
    compForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      const btnSubmit = document.getElementById('btn-submit-consumer-complaint');
      if (btnSubmit) {
        btnSubmit.disabled = true;
        btnSubmit.innerHTML = 'Ingesting to MSK...';
      }

      const customerName = document.getElementById('comp-cust-name')?.value.trim() || 'Sarah Jenkins';
      const customerEmail = document.getElementById('comp-cust-email')?.value.trim() || 'consumer@customer.com';
      const orderId = document.getElementById('comp-order-id')?.value.trim() || ('ORD-' + Math.floor(10000 + Math.random() * 90000));
      const category = document.getElementById('comp-category-select')?.value || 'BILLING_DOUBLE_CHARGE';
      const severity = document.getElementById('comp-severity-select')?.value || 'CRITICAL';
      const description = document.getElementById('comp-description')?.value.trim();

      if (!description) {
        showToast('Please provide a complaint description', 'warning');
        if (btnSubmit) {
          btnSubmit.disabled = false;
          btnSubmit.innerHTML = 'Submit Grievance to Amazon MSK';
        }
        return;
      }

      try {
        const res = await fetch('/api/complaints', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ customerName, customerEmail, orderId, category, severity, description })
        });
        const data = await res.json();

        if (res.ok && data.complaint) {
          if (!MSK_STATE.complaints.some(c => c.id === data.complaint.id)) {
            MSK_STATE.complaints.unshift(data.complaint);
          }
          renderConsumerComplaints();
          renderProducerComplaintsInbox();
          if (window.AUDIO_ENGINE) AUDIO_ENGINE.playProduceSound();
          showToast(`Grievance #${data.complaint.id} dispatched to Amazon MSK & routed to Producer Inbox!`, 'success');

          // Reset description & generate new random order ID
          const descField = document.getElementById('comp-description');
          if (descField) descField.value = '';
          const orderField = document.getElementById('comp-order-id');
          if (orderField) orderField.value = 'ORD-' + Math.floor(10000 + Math.random() * 90000);
        } else {
          showToast(data.error || 'Failed to submit grievance', 'danger');
        }
      } catch (err) {
        console.error('Failed to submit complaint to API:', err);
        // Fallback local ingestion
        const localId = 'CMP-' + Math.floor(10000 + Math.random() * 90000);
        const localComp = {
          id: localId,
          customerName,
          customerEmail,
          orderId,
          category,
          severity,
          description,
          status: 'INGESTED',
          timestamp: new Date().toISOString(),
          sentimentScore: 0.94,
          aiRecommendation: 'INSTANT AUTO-REFUND APPROVED ($289.99) + 15% APOLOGY VOUCHER',
          partition: 1,
          offset: 12480,
          brokerId: 2
        };
        MSK_STATE.complaints.unshift(localComp);
        renderConsumerComplaints();
        renderProducerComplaintsInbox();
        showToast(`Grievance #${localId} ingested to local queue`, 'info');
      } finally {
        if (btnSubmit) {
          btnSubmit.disabled = false;
          btnSubmit.innerHTML = `
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <line x1="22" y1="2" x2="11" y2="13"></line>
              <polygon points="22 2 15 22 11 13 2 9 22 2"></polygon>
            </svg>
            Submit Grievance to Amazon MSK
          `;
        }
      }
    });
  }

  if (btnRefreshConsumer) btnRefreshConsumer.addEventListener('click', fetchComplaints);
  if (btnRefreshProducer) btnRefreshProducer.addEventListener('click', fetchComplaints);

  // Initial fetch
  fetchComplaints();
}

async function fetchComplaints() {
  try {
    const res = await fetch('/api/complaints');
    if (res.ok) {
      const data = await res.json();
      if (data.complaints && Array.isArray(data.complaints)) {
        MSK_STATE.complaints = data.complaints;
        renderConsumerComplaints();
        renderProducerComplaintsInbox();
      }
    }
  } catch (e) {
    console.warn('Could not fetch complaints from backend:', e);
  }
}

function escapeHtml(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

function renderConsumerComplaints() {
  const container = document.getElementById('consumer-my-complaints-list');
  if (!container) return;

  if (MSK_STATE.complaints.length === 0) {
    container.innerHTML = `
      <div class="empty-state-card" style="padding: 30px; text-align: center; color: var(--text-muted);">
        <p style="font-size: 1rem; margin-bottom: 6px;">No complaints filed yet.</p>
        <small>Fill out the grievance form on the left to submit a live issue to Amazon MSK.</small>
      </div>
    `;
    return;
  }

  container.innerHTML = '';
  MSK_STATE.complaints.forEach(c => {
    const isResolved = c.status === 'RESOLVED';
    const card = document.createElement('div');
    card.className = `consumer-complaint-card ${isResolved ? 'status-resolved' : 'status-pending'}`;
    card.innerHTML = `
      <div class="complaint-card-header">
        <div class="comp-id-row" style="display: flex; align-items: center; gap: 8px;">
          <span class="badge-mini" style="background: rgba(0, 242, 254, 0.15); color: #00f2fe; font-family: var(--font-code); font-weight: 700;">#${escapeHtml(c.id)}</span>
          <span class="comp-cat-label" style="font-size: 0.8rem; font-weight: 600; color: #cbd5e1;">${escapeHtml(c.category.replace(/_/g, ' '))}</span>
        </div>
        <span class="badge-mini" style="padding: 3px 8px; border-radius: 4px; font-weight: 700; font-size: 0.72rem; ${isResolved ? 'background: rgba(16, 185, 129, 0.2); color: #10b981;' : 'background: rgba(255, 153, 0, 0.2); color: #ff9900;'}">
          ${isResolved ? '✔ RESOLVED' : '⏳ PENDING PRODUCER'}
        </span>
      </div>

      <div class="comp-order-ref" style="font-size: 0.75rem; color: var(--text-muted); margin: 6px 0;">
        Order Ref: <code style="color: #38bdf8;">${escapeHtml(c.orderId || 'N/A')}</code> &bull; Priority: <strong style="color: ${c.severity === 'CRITICAL' ? '#f43f5e' : '#ff9900'};">${escapeHtml(c.severity || 'HIGH')}</strong>
      </div>

      <p class="comp-desc-text" style="font-size: 0.85rem; color: #f1f5f9; margin-bottom: 10px; line-height: 1.4;">
        "${escapeHtml(c.description)}"
      </p>

      ${isResolved ? `
        <div class="comp-resolution-box" style="background: rgba(16, 185, 129, 0.1); border: 1px solid rgba(16, 185, 129, 0.3); border-radius: 6px; padding: 10px; margin-top: 8px;">
          <div style="font-size: 0.72rem; font-weight: 700; color: #10b981; margin-bottom: 4px;">✔ PRODUCER RESOLUTION APPLIED:</div>
          <div style="font-size: 0.82rem; color: #e2e8f0; font-weight: 600;">${escapeHtml(c.resolution || c.aiRecommendation)}</div>
          <div style="font-size: 0.68rem; color: var(--text-muted); margin-top: 4px;">Resolved at: ${new Date(c.resolvedAt || c.timestamp).toLocaleTimeString()}</div>
        </div>
      ` : `
        <div class="comp-kafka-meta" style="font-size: 0.7rem; color: var(--text-muted); display: flex; justify-content: space-between; align-items: center; border-top: 1px dashed rgba(255, 255, 255, 0.1); padding-top: 8px; margin-top: 8px;">
          <span>Kafka Topic: <code>customer.complaints.v1</code></span>
          <span style="color: #ff9900;">⚡ Live in Producer Inbox</span>
        </div>
      `}
    `;
    container.appendChild(card);
  });
}

function renderProducerComplaintsInbox() {
  const container = document.getElementById('producer-complaints-list');
  const countBadge = document.getElementById('producer-pending-complaints-count');
  if (!container) return;

  const pendingCount = MSK_STATE.complaints.filter(c => c.status !== 'RESOLVED').length;
  if (countBadge) {
    countBadge.textContent = `${pendingCount} Pending Review`;
    countBadge.className = pendingCount > 0 ? 'badge-count glow-orange' : 'badge-count';
  }

  if (MSK_STATE.complaints.length === 0) {
    container.innerHTML = `
      <div class="empty-inbox" style="grid-column: 1 / -1; padding: 30px; text-align: center; color: var(--text-muted);">
        <p style="font-size: 1rem; margin-bottom: 6px;">No complaints in queue.</p>
        <small>Any grievances submitted by Consumers will stream into this Producer Inbox in real-time.</small>
      </div>
    `;
    return;
  }

  container.innerHTML = '';
  MSK_STATE.complaints.forEach(c => {
    const isResolved = c.status === 'RESOLVED';
    const card = document.createElement('div');
    card.className = `complaint-inbox-item ${isResolved ? 'is-resolved' : 'is-pending'}`;
    card.id = `producer-comp-${c.id}`;
    card.innerHTML = `
      <div class="inbox-item-header" style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 8px;">
        <div class="comp-title-group" style="display: flex; align-items: center; gap: 8px;">
          <span class="badge-mini" style="background: rgba(0, 242, 254, 0.15); color: #00f2fe; font-family: var(--font-code); font-weight: 700;">#${escapeHtml(c.id)}</span>
          <strong class="comp-category-tag" style="font-size: 0.84rem; color: #f8fafc;">${escapeHtml(c.category.replace(/_/g, ' '))}</strong>
        </div>
        <div class="comp-badge-group" style="display: flex; gap: 6px;">
          <span class="badge-mini" style="background: ${c.severity === 'CRITICAL' ? 'rgba(244, 63, 94, 0.2)' : 'rgba(255, 153, 0, 0.2)'}; color: ${c.severity === 'CRITICAL' ? '#f43f5e' : '#ff9900'}; font-weight: 700;">
            ${escapeHtml(c.severity || 'HIGH')}
          </span>
          <span class="badge-mini" style="${isResolved ? 'background: rgba(16, 185, 129, 0.2); color: #10b981;' : 'background: rgba(255, 153, 0, 0.2); color: #ff9900;'}; font-weight: 700;">
            ${isResolved ? '✔ RESOLVED' : 'INGESTED'}
          </span>
        </div>
      </div>

      <div class="inbox-customer-meta" style="font-size: 0.74rem; color: var(--text-muted); display: flex; flex-direction: column; gap: 2px; margin-bottom: 8px;">
        <div><strong>Customer:</strong> <span style="color: #cbd5e1;">${escapeHtml(c.customerName || 'Customer')} (${escapeHtml(c.customerEmail)})</span></div>
        <div><strong>Order ID:</strong> <code style="color: #38bdf8;">${escapeHtml(c.orderId || 'N/A')}</code> &bull; <strong>Logged:</strong> ${new Date(c.timestamp).toLocaleTimeString()}</div>
      </div>

      <div class="inbox-desc-quote" style="background: rgba(0, 0, 0, 0.25); border-left: 3px solid #ff9900; padding: 8px 12px; border-radius: 4px; font-size: 0.82rem; color: #f1f5f9; line-height: 1.4; margin-bottom: 10px;">
        "${escapeHtml(c.description)}"
      </div>

      <!-- AI Recommendation Box -->
      <div class="ai-recommendation-box" style="background: rgba(16, 185, 129, 0.08); border: 1px solid rgba(16, 185, 129, 0.25); border-radius: 6px; padding: 10px; margin-bottom: 10px;">
        <div style="font-size: 0.7rem; color: #10b981; font-weight: 700; display: flex; align-items: center; gap: 6px; margin-bottom: 4px;">
          <span>🤖 AI TRIAGE RECOMMENDATION (Confidence: ${Math.round((c.sentimentScore || 0.94) * 100)}%):</span>
        </div>
        <div style="font-size: 0.82rem; color: #f8fafc; font-weight: 600;">
          ${escapeHtml(c.aiRecommendation || 'PRIORITY RESOLUTION APPLIED')}
        </div>
      </div>

      <!-- Kafka Broker & Partition Provenance -->
      <div class="inbox-kafka-provenance" style="font-size: 0.7rem; color: var(--text-muted); display: flex; justify-content: space-between; align-items: center; padding: 6px 0; border-top: 1px solid rgba(255, 255, 255, 0.05); margin-bottom: 8px;">
        <span>MSK Ingestion: <strong>Broker ${c.brokerId !== undefined ? c.brokerId : 2} &bull; Partition ${c.partition !== undefined ? c.partition : 1} &bull; Offset #${c.offset || '11682'}</strong></span>
        <span>Topic: <code>customer.complaints.v1</code></span>
      </div>

      <!-- Resolution Action Bar -->
      <div class="inbox-item-actions">
        ${isResolved ? `
          <div style="padding: 8px 12px; background: rgba(16, 185, 129, 0.15); border-radius: 6px; color: #10b981; font-size: 0.8rem; font-weight: 700; display: flex; align-items: center; gap: 6px;">
            <span>✔ RESOLUTION COMMITTED TO MSK:</span>
            <span style="color: #f1f5f9; font-weight: 500;">${escapeHtml(c.resolution || c.aiRecommendation)}</span>
          </div>
        ` : `
          <button type="button" class="btn-primary full-width btn-resolve-complaint" data-id="${c.id}" data-rec="${escapeHtml(c.aiRecommendation || 'INSTANT AUTO-REFUND APPROVED ($289.99) + 15% APOLOGY VOUCHER')}">
            ⚡ Approve & Resolve Grievance (Publish to complaints.resolved.v1)
          </button>
        `}
      </div>
    `;

    container.appendChild(card);
  });

  // Attach click listeners to all resolve buttons
  container.querySelectorAll('.btn-resolve-complaint').forEach(btn => {
    btn.addEventListener('click', async (e) => {
      e.stopPropagation();
      const id = btn.getAttribute('data-id');
      const rec = btn.getAttribute('data-rec');

      btn.disabled = true;
      btn.textContent = 'Resolving on MSK cluster...';

      try {
        const res = await fetch(`/api/complaints/${id}/resolve`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ resolution: rec })
        });
        const data = await res.json();

        if (res.ok && data.complaint) {
          const target = MSK_STATE.complaints.find(c => c.id === id);
          if (target) {
            target.status = 'RESOLVED';
            target.resolution = data.complaint.resolution;
            target.resolvedAt = data.complaint.resolvedAt;
          }
          renderProducerComplaintsInbox();
          renderConsumerComplaints();
          if (window.AUDIO_ENGINE) AUDIO_ENGINE.playAckSound();
          showToast(`✅ Grievance #${id} resolved & published to complaints.resolved.v1!`, 'success');
        } else {
          showToast(data.error || 'Failed to resolve grievance', 'danger');
          btn.disabled = false;
          btn.textContent = '⚡ Approve & Resolve Grievance';
        }
      } catch (err) {
        console.warn('API resolve error, applying local:', err);
        const target = MSK_STATE.complaints.find(c => c.id === id);
        if (target) {
          target.status = 'RESOLVED';
          target.resolution = rec;
          target.resolvedAt = new Date().toISOString();
        }
        renderProducerComplaintsInbox();
        renderConsumerComplaints();
        showToast(`Grievance #${id} resolved locally`, 'info');
      }
    });
  });
}



