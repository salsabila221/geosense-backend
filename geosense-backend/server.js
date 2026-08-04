const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mqtt = require('mqtt');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json()); // Menerima payload JSON dari API

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

// --- KONFIGURASI MQTT (HIVEMQ CLOUD) ---
const MQTT_BROKER = 'mqtts://3cd0f777a8ec4a20af722dd1214f7eb3.s1.eu.hivemq.cloud:8883';
const MQTT_TOPIC = 'geosense/data';

const mqttOptions = {
  username: 'Kelom2',
  password: 'TCPRule1',
  clientId: `express_backend_${Math.random().toString(16).substring(2, 8)}`
};

const mqttClient = mqtt.connect(MQTT_BROKER, mqttOptions);

mqttClient.on('connect', () => {
  console.log('✅ Backend terhubung ke HiveMQ Cloud Broker!');
  mqttClient.subscribe(MQTT_TOPIC, (err) => {
    if (!err) {
      console.log(`📥 Subscribed ke topik: '${MQTT_TOPIC}'`);
    }
  });
});

mqttClient.on('error', (err) => {
  console.error('⚠️ MQTT Error:', err.message);
});

io.on('connection', (socket) => {
  console.log(`🔌 Client Frontend terhubung! (ID Socket: ${socket.id})`);
  socket.on('disconnect', () => {
    console.log(`❌ Client Frontend terputus: ${socket.id}`);
  });
});

// RELAY: Data MQTT Python -> Broadcast ke Frontend
mqttClient.on('message', (topic, message) => {
  if (topic === MQTT_TOPIC) {
    try {
      const dataPayload = JSON.parse(message.toString());
      io.emit('geosense_update', dataPayload);
    } catch (error) {
      console.error('⚠️ Gagal membaca data JSON:', error.message);
    }
  }
});

// API ADMIN EMERGENCY OVERRIDE
app.post('/api/admin/override', (req, res) => {
  const { status } = req.query;
  console.log(`⚠️ Admin Emergency Override dipicu ke status: ${status}`);

  const overridePayload = {
    timestamp: Math.floor(Date.now() / 1000),
    kondisi_tanah: "Override Admin",
    status: status === "Warning" ? "WASPADA" : status.toUpperCase(),
    aktivitas: "ADMIN OVERRIDE",
    p2p_amplitude: 0,
    rms: 0
  };

  // Broadcast langsung ke seluruh dashboard frontend
  io.emit('geosense_update', overridePayload);
  res.json({ success: true, message: `Status diubah ke ${status}` });
});

app.get('/', (req, res) => res.send('Server Backend GeoSense Aktif 🚀'));

// Port diubah ke 8000 menyesuaikan Frontend
const PORT = 8000;
server.listen(PORT, () => {
  console.log(`🚀 Backend GeoSense running di http://localhost:${PORT}`);
});