const { app } = require('@azure/functions');
const mqtt = require('mqtt');
const admin = require('firebase-admin');

// Initialize Firestore only once
const serviceAccount = JSON.parse(Buffer.from(process.env.FIRESTORE_KEY, 'base64').toString());
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});
const db = admin.firestore();

app.timer('timerTrigger', {
  schedule: '0 0 0,12 * * *', // every 12 hours
  handler: async (myTimer, context) => {
    const timeStamp = new Date().toLocaleString('en-PK', { timeZone: 'Asia/Karachi' });
    context.log('Timer trigger function ran at:', timeStamp);

    const client = mqtt.connect({
      host: process.env.MQTT_BROKER,
      port: process.env.MQTT_PORT
    });

    client.on('connect', () => {
      console.log('Connected to MQTT broker');
      client.subscribe('/asani/devices/power/powerenergy/stmc/#', (err) => {
        if (!err) {
          console.log('Subscribed to main topic');
          gatherCurrentValues();
        } else {
          console.error('Subscription error:', err);
          client.end();
        }
      });
    });

    const subtopics = new Set(); // Use a Set to avoid duplicates
    const processedDevices = new Set(); // To track processed devices

    client.on('message', (topic, message) => {
      const subtopic = topic.split('/').pop(); // Extract the last part of the topic as subtopic
      subtopics.add(subtopic);
    });

    async function gatherCurrentValues() {
      // Wait for 5 seconds to gather subtopics
      await new Promise(resolve => setTimeout(resolve, 5000));

      for (const subtopic of subtopics) {
        await processSubtopic(subtopic);
      }

      client.end();
      console.log('Client Disconnected!');
    }

    async function processSubtopic(subtopic) {
      return new Promise((resolve, reject) => {
        const topic = `/asani/devices/power/powerenergy/stmc/${subtopic}`;
        const messageHandler = async (topic, message) => {
          try {
            const data = JSON.parse(message.toString());
            if (data.PowerData && data.PowerData.DeviceID && data.PowerData.Energy1) {
              const deviceId = data.PowerData.DeviceID;
              const energyValue = data.PowerData.Energy1;

              // Skip if the device has already been processed
              if (processedDevices.has(deviceId)) {
                resolve();
                return;
              }

              try {
                // Query Firestore for collections with matching device IDs
                const collections = await db.listCollections();
                for await (const collectionRef of collections) {
                  const querySnapshot = await collectionRef.where('data.deviceID', '==', deviceId).get();
                  for (const doc of querySnapshot.docs) {
                    const nestedData = {
                      data: admin.firestore.FieldValue.arrayUnion({
                        time: admin.firestore.Timestamp.now(),
                        value: energyValue
                      })
                    };
                    await db.collection(collectionRef.id).doc('energy').set(nestedData, { merge: true });
                    console.log(`Updated energy data for device ${deviceId} in collection ${collectionRef.id}`);
                  }
                }
                // Mark the device as processed
                processedDevices.add(deviceId);
              } catch (error) {
                console.error('Error updating energy data:', error);
              }
            } else {
              console.error('Invalid data format. DeviceID or Energy1 field is missing.');
            }
          } catch (err) {
            console.error('Message parsing error:', err);
          } finally {
            client.removeListener('message', messageHandler);
            resolve();
          }
        };

        client.subscribe(topic, (err) => {
          if (!err) {
            client.on('message', messageHandler);
          } else {
            console.error(`Subscription error for subtopic ${subtopic}:`, err);
            reject(err);
          }
        });
      });
    }

    client.on('error', (err) => {
      console.error('MQTT Client Error:', err);
    });
  }
});