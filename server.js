const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const amqp = require('amqplib');
const mongoose = require('mongoose');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const rabbitMQUrl = 'amqp://zenAdmin:pulsepasswrd_@192.168.252.191';

const queueNames = ['Pulse_Sensor', 'ADXL345', 'MLX90614', 'MAX30102', 'Pulse_irt'];
const wsClients = {};

// Conexión a MongoDB Atlas
mongoose.connect('mongodb+srv://YaelAguilar:lgunsfsascsxv3w5w4r@zenpulsedb.movczbj.mongodb.net/', { useNewUrlParser: true, useUnifiedTopology: true });

// Definir el esquema del documento
const dataSchema = new mongoose.Schema({
  _id: mongoose.Schema.Types.ObjectId, // Cambié Id a _id y usé el tipo ObjectId de MongoDB
  data: { type: mongoose.Schema.Types.Mixed } // Utilizamos un campo 'data' de tipo Mixto para almacenar datos de las colas
});

// Crear modelo basado en el esquema
const DataModel = mongoose.model('datas', dataSchema);

async function setupWebSocket() {
  const connection = await amqp.connect(rabbitMQUrl);

  for (const queueName of queueNames) {
    const channel = await connection.createChannel();
    await channel.assertQueue(queueName, { durable: true });

    channel.consume(queueName, async (msg) => {
      const messageContent = msg.content.toString();
      console.log(`Received from ${queueName}: ${messageContent}`);

      // Send data to database
      await saveDataToMongoDB({ data: { [queueName]: messageContent } });

      if (wsClients[queueName]) {
        wsClients[queueName].send(messageContent);
      }
    }, { noAck: true });
  }
}

async function saveDataToMongoDB(data) {
  try {
    // Asegurémonos de que haya un _id en los datos
    if (!data._id) {
      // Usamos 'new' para crear un nuevo ObjectId
      data._id = new mongoose.Types.ObjectId();
    }

    const newData = new DataModel(data);
    await newData.save();
    console.log('Data saved to MongoDB:', newData);
  } catch (error) {
    console.error('Error saving data to MongoDB:', error);
  }
}

wss.on('connection', (ws, req) => {
  const queueKey = req.url.replace('/', '');
  wsClients[queueKey] = ws;

  ws.on('close', () => {
    delete wsClients[queueKey];
  });
});

setupWebSocket();

server.listen(3001, () => {
  console.log('Server listening on port 3001');
});
