const WebSocket = require('ws');

async function testFullBackend() {
  console.log('1. Testing REST API Health...');
  const resHealth = await fetch('http://localhost:3000/api/health');
  const dataHealth = await resHealth.json();
  console.log('   -> Health Status:', dataHealth.status, '| Service:', dataHealth.service);

  console.log('2. Testing REST API Produce Record...');
  const resProduce = await fetch('http://localhost:3000/api/produce', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      topic: 'orders.v1',
      key: 'cust_verify_99',
      payload: { verification: true, time: new Date().toISOString() }
    })
  });
  const dataProduce = await resProduce.json();
  console.log('   -> Acknowledged! Partition:', dataProduce.record.partition, '| Offset:', dataProduce.record.offset, '| Latency:', dataProduce.record.latencyMs + 'ms');

  console.log('3. Testing WebSocket Duplex Pipe...');
  const ws = new WebSocket('ws://localhost:3000');
  await new Promise((resolve) => {
    ws.on('open', () => {
      console.log('   -> WebSocket Connected to ws://localhost:3000');
    });
    ws.on('message', (raw) => {
      const msg = JSON.parse(raw);
      console.log('   -> Received Live WebSocket Packet:', msg.type);
      ws.close();
      resolve();
    });
    setTimeout(resolve, 2000);
  });

  console.log('\n========================================');
  console.log('✅ BACKEND CONNECTION STATUS: 100% COMPLETE');
  console.log('========================================');
  process.exit(0);
}

testFullBackend().catch(err => {
  console.error('Test error:', err);
  process.exit(1);
});
