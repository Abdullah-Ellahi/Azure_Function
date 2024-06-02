const mqtt = require('mqtt');
const admin = require('firebase-admin');

// Initialize Firestore
const serviceAccount = JSON.parse(Buffer.from(process.env.FIRESTORE_KEY, 'base64').toString());
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});
const db = admin.firestore();

// MQTT Client setup
const client = mqtt.connect({
  host: process.env.MQTT_BROKER,
  port: process.env.MQTT_PORT
});

client.on('connect', () => {
  console.log('Connected to MQTT broker');
  client.subscribe(process.env.MQTT_TOPIC, (err) => {
    if (!err) {
      console.log(`Subscribed to topic ${process.env.MQTT_TOPIC}`);
      
      // Schedule the initial message publication after subscription
      scheduleMessagePublication();
    } else {
      console.error('Subscription error:', err);
    }
  });
});

client.on('message', async (topic, message) => {
  const data = JSON.parse(message.toString());

  // Ensure DeviceID and Energy1 fields exist
  if (data.PowerData && data.PowerData.DeviceID && data.PowerData.Energy1) {
    const timestamp = new Date().toISOString();

    // Create the nested structure with the timestamp as the field name
    const nestedData = {
      data: admin.firestore.FieldValue.arrayUnion({
        time: timestamp,
        value: data.PowerData.Energy1
      })
    };

    try {
      // Add the nested data to Firestore
      const docRef = await db.collection('7qxiwxv5gMVH57ZGTfENnL45lkj2').doc('energy').set(nestedData, { merge: true });
      console.log('Document written with ID:', docRef.id);
    } catch (error) {
      console.error('Error writing document:', error);
    }
  } else {
    console.error('Invalid data format. DeviceID or Energy1 field is missing.');
  }
});

function scheduleMessagePublication() {
  // Check if the current time is midnight or noon UTC
  // const currentHour = new Date().getUTCHours();
  // if (currentHour === 0 || currentHour === 12) {
  //   // Simulate publishing a message to trigger the 'message' event for testing
    client.publish(process.env.MQTT_TOPIC, JSON.stringify({ message: 'This is from Azure function' }));
  // } else {
  //   console.log('Ignoring message publication outside of specified times.');
  // }
}

module.exports = async function (context, myTimer) {
  const timeStamp = new Date().toISOString();
  context.log('Timer trigger function ran at:', timeStamp);

  // Check if the current time is midnight or noon UTC
  // const currentHour = new Date().getUTCHours();
  // if (currentHour === 0 || currentHour === 12) {
    // Schedule the message publication if the current time matches the specified times
    scheduleMessagePublication();
  // } else {
  //   console.log('Ignoring timer trigger outside of specified times.');
  // }
};
