# Amazon MSK (Managed Kafka) for Event Streaming — Full-Stack Console & Backend

An enterprise-grade, real-time event streaming platform and interactive control console built for **"Amazon MSK Managed Kafka for Event Streaming"**.

This project contains both the **Frontend Streaming Console** and the **Real-Time Backend API + WebSocket Server**, replicating an enterprise AWS MSK event streaming architecture.

---

## 🌟 Key Architecture & Capabilities

```
+-------------------------------------------------------------+
|                      PRODUCERS                              |
|  [Web & Mobile]  [IoT Core Sensors]  [ECS Fargate Workers]  |
+------------------------------+------------------------------+
                               | (REST / WebSocket / APIs)
                               v
+-------------------------------------------------------------+
|              AMAZON MSK BACKEND ENGINE (server.js)          |
|  - Express REST API (/api/produce, /api/topics, etc.)       |
|  - WebSocket Streaming Broadcaster (ws://localhost:3000)    |
|  - Multi-AZ Partition Router & Key Hasher                   |
|  - Kafka KRaft Consensus & Broker Load Balancer             |
+------------------------------+------------------------------+
                               | (Real-time Broadcast)
                               v
+-------------------------------------------------------------+
|               FRONTEND CONSOLE (index.html / app.js)        |
|  - Live CloudWatch Throughput & Velocity Charts             |
|  - Event Producer Studio (Custom Key, Schema, Headers)      |
|  - Live Stream Inspector & JSON Record Explorer             |
|  - Managed Topics Provisioning & Consumer Lag Tracking      |
|  - Interactive Cloud Architecture Topology Visualizer       |
+-------------------------------------------------------------+
```

---

## 🚀 How to Run in VS Code

### Step 1: Open Terminal in VS Code
Open the project folder in **Visual Studio Code** and open the terminal (`Ctrl + ~`).

### Step 2: Start the Full-Stack Server
Run:
```powershell
npm start
```
*(Or `node server.js` / `npm run dev` for auto-reloading)*

### Step 3: Open in Browser
Navigate to:
👉 **[http://localhost:3000](http://localhost:3000)**

The frontend automatically connects to the backend over WebSocket and displays **`BACKEND: LIVE (WS)`** in the top navigation bar!

---

## 📋 Backend REST API Reference

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/health` | Service health status, uptime, and version |
| `GET` | `/api/cluster` | MSK cluster metadata, broker nodes, throughput metrics |
| `GET` | `/api/topics` | List all managed Kafka topics with partition counts |
| `POST` | `/api/topics` | Provision a new topic (`{ name, partitions, replication, retentionHours, cleanupPolicy }`) |
| `DELETE`| `/api/topics/:name`| Delete a topic from the cluster |
| `POST` | `/api/produce` | Publish an event to MSK topic (`{ topic, key, payload, headers }`) |
| `POST` | `/api/produce/burst` | Ingest a burst of simulated events (`{ count: 25 }`) |
| `GET` | `/api/messages` | Fetch recent streamed messages (`?topic=...&partition=...&search=...`) |
| `GET` | `/api/consumer-groups` | Downstream consumer groups, partition assignments & lag |
| `POST` | `/api/simulator/toggle` | Toggle background streaming traffic generator |

---

## 📡 WebSocket Streaming Protocol

Connect to `ws://localhost:3000` from any client:
- **`INIT_STATE`**: Receives full cluster metadata, topic lists, and recent message buffers.
- **`EVENT_INGESTED`**: Broadcasted immediately whenever any producer publishes a message.
- **`TELEMETRY_TICK`**: Real-time velocity snapshot (Ingress msgs/s, Egress msgs/s, Broker CPU/Disk).
- **`TOPIC_CREATED` / `TOPIC_DELETED`**: Synchronizes topic catalogs across all connected clients.

---

## 🛠️ Tech Stack
- **Backend**: Node.js, Express, `ws` (WebSockets), CORS
- **Frontend**: Vanilla HTML5, Vanilla CSS3 (AWS Obsidian dark glassmorphism theme), Vanilla JS (ES6+)
- **Visuals**: Native HTML5 Canvas (rolling line charts) & SVG (interactive cloud topology diagram)
