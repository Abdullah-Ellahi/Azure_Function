// const { app } = require('@azure/functions');
// const mqtt = require('mqtt');
// const admin = require('firebase-admin');

//     const serviceAccount = JSON.parse(Buffer.from('ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAiYXNhbmktbW9iaWxlIiwKICAicHJpdmF0ZV9rZXlfaWQiOiAiMjUxMTU5NTEzNTIyMDVlNTIzMzhhYzExNzk0ZmQzMWY2NDg5NGFjNCIsCiAgInByaXZhdGVfa2V5IjogIi0tLS0tQkVHSU4gUFJJVkFURSBLRVktLS0tLVxuTUlJRXZRSUJBREFOQmdrcWhraUc5dzBCQVFFRkFBU0NCS2N3Z2dTakFnRUFBb0lCQVFDV3dxV3B6TDczSzNXOVxubTZYOEd4MmpUdVJQQXl1cWhUYVljUnpsSFYyaXV3aWtITmtVYXFWTzFNTCtjN2dYZE5KQTdwR2tjSWM1alpPOFxuZG80Y0tCYmwrMXgxOHZJTmM1MEdhcjBXM25YVEJUKzV5ZldMdWU1NXNXLzl4NDRsdE1GVXVySWQyQUp6RnVNOVxueTRGVUtoT2VJZ2FnT0tVSFovUlAwbUw2OXRDVGNKQWdSNUprSjZPUTg5UTUzM29JTWk2cFVRSzAyLzU0RFU0S1xuQTRhRHFsSktzSjRaL3FQd1hOMEN0c2ZDT0daYnY4VkNqU2JNMEJ6UG1sUWJ6QUZXNVZkWEo1WW1wclZGbngzY1xuUWlpV0xkL09oSW9hY0todnY5ak4yd2QxL2RXdDQ4bm9BM2dQNFNCNm05NGV4cUR1bmJtaU5UNmtCQUx2Q3FFNFxuVXRsdHZQa2ZBZ01CQUFFQ2dnRUFIVFBGOEI2VFRRQlRsYTh0RnlqUGRGOG5OVERaaC9XMnpnOTI5K0JZejFrb1xuUXRXOXJNazlqUWdFWDFKZDhkazBrdFJEVE5WcE5CbzY2RElVczlxR0dQVTRBemJVY1F1WWVuSTVmMUVIQktHTFxuSnZzNlAxNzhGUUZzUG4ybGlLTTdJYVRxTklLdmNzaVhxdFFkRUliMzl0VURjSXlZVnRkcGlQNnBJcENsZFRGM1xuMzR2aGJhQVJSTmcwTm9lbVhMNHAyWDdrcm9mZk1uWnY5TUNhUWFENmtjRkJURE5Rd1hkWENaUzZKRzB0QllhZ1xudllYMlVUZWtidjlHUTE4aDhxcXJjUW45MFNobWxXbnVDRjZjNWdaUFJzRXpTdnlYY2tJM2pENU1PWFpjbWZvcFxuanZxeEhWdll0a21PZ0JWbDZKNlRISkRMYVNNTXhmNUd4eURnU2V6SDZRS0JnUURPRk1ibkJwSkxpZGdnbldQTFxubGhpZXVzd2FkQnkwSVdoNEJ3aWFPNkt2L1RwSlB1dTBwMzB6QzQwQTIzb0tPTmpQS1RRWFNOdWk1Z09zdHJrN1xuTDRrVWRCRGs4aWp0WjdCZ1o2S0RsemdFeERHRHVkeDY5bXZKbXE3VmRTYU9Yc2pHR2RQVE5uUnRLMnBNaHFpSVxuVklmT3RvNzdQVldIVzJ4a1pRdjl1T1FROXdLQmdRQzdSMlFpZkwyQnl6UjhGc1lZMEljU3I2MGxOUExyMzRlSlxuUHNCcnJUL1g0N3Y4Q05sWFJCek96UGhhNnhLcWNxQlZlTm5hK0FDVlByUzAxU2pFMk5TdWYvYUZHVGpqV04yVVxuWnJTN284dDRZc3RCUWtMZy95QzlnZUp0anhkM1lPQncvaFg5bVl5RmN0eTcxdS8zVHArOUxrS25HcStkSkhZQVxuSXhKR3lHLzNHUUtCZ1FDdGFiWm1PWlZwa3prWFdObmRPeFRFblJPYlB4SFlValNDckFpRklLR3B6a0Q1MmNTbVxuWkRwcWRkSFZZdHF6TjFyYUdDWUpZZm5RZmhXaGhMRWlLTUlGUUJYblVnODJsd1pJV1d6YnBxZ3crcGRmN1VxbVxuL2kxOW5IaDZqdlkrMzJ1N3A0Z2tON0tKR3Z0OEllUTN0RW9EbklOOHp3UGx2dnpiRGx5a3lLekJ5UUtCZ0d5UlxucVpwWXFHQWJWcFR0ZXZBZHk0Qm91YjdOSUZyZm5pcFJaNm5FcVROV0FiL052WG5hc2J5dGxQallPRno1MEx0Y1xuTVNmQkNFMTlLYk4vczMvU21CR0ZlM3VUc0tnVDkzaTF4ZWJWd1BwTWc3cVVXRU1waEdoNGFMVE05SFN2ZzgzZFxuYnhBeXVRVVFIcWtDcE9EQkF0ZmdmcUZ6VmdXS2dxSDNzdnUzN1RqWkFvR0FGdGNNTEY1V0t3clNEMGdPL2tQT1xuaSsveWFHZy83ZHpmV3JTNXlkUSs0NHhSRVljQzRmclNXSTJFQ0ZmS2tDZTRXKzBncG1yQXc3THJ0b2NLanBtcFxuRkRpK3VZYnpBb1gxODZGYmdwc2VmOFF3Yk9hSUowWEUvT2ZVRGxjNGd1MEwrVTRVR1JXTWtXcUl5N0VmQm5oWVxueDdsRlo4b2dac3ZJZDhYSWhyMGo2a2M9XG4tLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tXG4iLAogICJjbGllbnRfZW1haWwiOiAiZmlyZWJhc2UtYWRtaW5zZGstdjZueDdAYXNhbmktbW9iaWxlLmlhbS5nc2VydmljZWFjY291bnQuY29tIiwKICAiY2xpZW50X2lkIjogIjEwNzg1MTY2MDc2ODcwOTc2NDMyMyIsCiAgImF1dGhfdXJpIjogImh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwKICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAiYXV0aF9wcm92aWRlcl94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0cyIsCiAgImNsaWVudF94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkvZmlyZWJhc2UtYWRtaW5zZGstdjZueDclNDBhc2FuaS1tb2JpbGUuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICJ1bml2ZXJzZV9kb21haW4iOiAiZ29vZ2xlYXBpcy5jb20iCn0K', 'base64').toString());

//     // if (!admin.apps.length) {
//         admin.initializeApp({
//             credential: admin.credential.cert(serviceAccount),
//             name: 'asani-mobile'
//         },
//         'asani-mobile'
//     );
//     // }

//     const getFirestoreInstance = () => {
//         if (!admin.apps.find(app => app.name === 'asani-mobile')) {
//             throw new Error('Firebase app named "asani-mobile" does not exist');
//         }
//         return admin.firestore(admin.app('asani-mobile'));
//     };

//     const db1 = getFirestoreInstance();

// app.timer('Merged_Data_Bridge', {
//     schedule: '*/30 * * * * *',
//     // process.env.TIMER_SCHEDULE_2 || '*/30 * * * *', // If TIMER_SCHEDULE_2 not available then every 30 mins
//     handler: async (myTimer, context) => {

//         const timeStamp = new Date().toLocaleString('en-PK', { timeZone: 'Asia/Karachi' });
//         const currentHour = new Date().getHours();
//         const currentMinute = new Date().getMinutes();
//         const currentSec = new Date().getSeconds();
//     context.log('Merged_Data_Bridge: function ran at:', timeStamp);
//     context.log('Merged_Data_Bridge: function ran with:', db1);

//     if (
//         (currentMinute === 59 && currentSec >=50) || 
//         (currentMinute === 0 && currentSec <= 30) 
//         // || 
//         // (currentMinute === 29 && currentSec >=50) || 
//         // (currentMinute == 30 && currentSec <= 30)
//     ){
//         tempUpdatedDeviceMap = new Map();
//         levelUpdatedDeviceMap = new Map();
//         // energyUpdatedDeviceMap = new Map();

//         const client = mqtt.connect({
//             host: process.env.MQTT_BROKER,
//             port: process.env.MQTT_PORT
//         });

//         client.on('connect', () => {
//             const topics = [
//                 process.env.LI_TOPIC,
//                 process.env.ENERGY_TOPIC_1,
//                 process.env.ENERGY_TOPIC_2,
//             ];

//             client.subscribe(topics, (err, granted) => {
//                 if (!err) {
//                     console.log('Merged_Data_Bridge: Subscribed to topics', granted.map(grant => grant.topic));
//                     gatherCurrentValues();
//                 } else {
//                     console.error('Merged_Data_Bridge: Subscription error:', err);
//                     client.end();
//                 }
//             });
//         });

//         const subtopics = new Set(); // Use a Set to avoid duplicates
//         const messageQueue = [];

//         client.on('message', (topic, message) => {
//             const subtopic = topic.split('/').pop(); // Extract the last part of the topic as subtopic
//             subtopics.add(subtopic);
//             messageQueue.push({ topic, message });
//         });

//         async function gatherCurrentValues() {
//             // Wait for 6 seconds to gather subtopics
//             await new Promise(resolve => setTimeout(resolve, 5000));

//             // Process all messages at once
//             await processMessages(messageQueue);

//             client.end();
//             console.log('LevelIndicatorDataBridge: Client Disconnected!');

//             // Call the cleanup function for the updated device IDs
//             console.log('LevelIndicatorDataBridge: Cleaning up: ', Array.from(levelUpdatedDeviceMap.entries()));
//             await cleanupOldLevelEntries(Array.from(levelUpdatedDeviceMap.entries()));

//             // Call the cleanup function for the updated device IDs
//             console.log('Temp_Humid_DataBridge: Cleaning up: ', Array.from(tempUpdatedDeviceMap.entries()));
//             await cleanupOldTempEntries(Array.from(tempUpdatedDeviceMap.entries()));
//         }

//         async function processMessages(messages) {
//             const tempDeviceDataMap = new Map();
//             const levelDeviceDataMap = new Map();
//             const EnergyDeviceDataMap = new Map();

//             // Aggregate data for each device
//             messages.forEach(({ topic, message }) => {
//                 try {
//                     const data = JSON.parse(message.toString());
//                     if (data.SensorData && data.SensorData.DeviceID) {
//                         if(data.SensorData.Level){
//                             const DeviceID = data.SensorData.DeviceID;
//                             const tankLevel = data.SensorData.Level;
    
//                             if (!levelDeviceDataMap.has(DeviceID)) {
//                                 levelDeviceDataMap.set(DeviceID, []);
//                             }
    
//                             levelDeviceDataMap.get(DeviceID).push({
//                                 time: admin.firestore.Timestamp.now(),
//                                 value: tankLevel
//                             });
//                         }
//                         if (data.SensorData.Temperature){
//                             const DeviceID = data.SensorData.DeviceID;
//                             const temperature = data.SensorData.Temperature;
//                             const humidity = data.SensorData.Humidity;
    
//                             if (!tempDeviceDataMap.has(DeviceID)) {
//                                 tempDeviceDataMap.set(DeviceID, []);
//                             }
    
//                             tempDeviceDataMap.get(DeviceID).push({
//                                 time: admin.firestore.Timestamp.now(),
//                                 temp: parseFloat(temperature),
//                                 humid: parseFloat(humidity),
//                             });    
//                         }
//                     } else {
//                         console.error('LevelIndicatorDataBridge: Invalid data format. DeviceID or Level field is missing.');
//                     }

//                     if (data.PowerData && data.PowerData.DeviceID) {
//                         const deviceId = data.PowerData.DeviceID;
//                         const energyValue = data.PowerData.Energy1 || data.PowerData.Energy;

//                         if (!EnergyDeviceDataMap.has(deviceId)) {
//                             EnergyDeviceDataMap.set(deviceId, []);
//                         }

//                         EnergyDeviceDataMap.get(deviceId).push({
//                             time: admin.firestore.Timestamp.now(),
//                             value: energyValue
//                         });
//                     } else {
//                         console.error('NewEnergyDataBridge: Invalid data format. DeviceID or Energy field is missing.');
//                     }
//                 } catch (err) {
//                     console.error('LevelIndicatorDataBridge: Message parsing error:', err);
//                 }
//             });

//             // Fetch all devices data from Firestore in one go
//             const collections = await db1.listCollections();

//             for (const collectionRef of collections) {
//                 const deviceDoc = await collectionRef.doc('device').get();
//                 if (deviceDoc.exists) {
//                     const deviceData = deviceDoc.data();

//                     if (deviceData.devices && Array.isArray(deviceData.devices.waterlevelindicator)) {
//                         const waterLevelIndicators = deviceData.devices.waterlevelindicator;

//                         for (let i = 0; i < waterLevelIndicators.length; i++) {
//                             const DeviceID = waterLevelIndicators[i].deviceID;
//                             if (levelDeviceDataMap.has(DeviceID)) {
//                                 const nestedData = {
//                                     data: admin.firestore.FieldValue.arrayUnion(...levelDeviceDataMap.get(DeviceID))
//                                 };

//                                 await db1.collection(collectionRef.id).doc(`waterlevelindicator(${i})`).set(nestedData, { merge: true });
//                                 console.log(`LevelIndicatorDataBridge: Updated water level data for device ${DeviceID} in collection ${collectionRef.id}, document 'device'`);

//                                 // Mark the device as having new entries with its index and collection ID
//                                 levelUpdatedDeviceMap.set(DeviceID, { index: i, collectionID: collectionRef.id });
//                                 console.log(`LevelIndicatorDataBridge: ${DeviceID} has been processed and added to be cleaned up with index ${i} in collection ${collectionRef.id}!`);
//                             }
//                         }
//                     } else {
//                         console.error(`LevelIndicatorDataBridge: 'devices.waterlevelindicator' is not an array or 'devices' is missing in document ${deviceDoc.id} of collection ${collectionRef.id}`);
//                     }

//                     if (deviceData.devices && Array.isArray(deviceData.devices.temperaturemonitor)) {
//                         const temperaturemonitors = deviceData.devices.temperaturemonitor;

//                         for (let i = 0; i < temperaturemonitors.length; i++) {
//                             const DeviceID = temperaturemonitors[i].deviceID;
//                             if (tempDeviceDataMap.has(DeviceID)) {
//                                 const nestedData = {
//                                     data: admin.firestore.FieldValue.arrayUnion(...tempDeviceDataMap.get(DeviceID))
//                                 };

//                                 await db1.collection(collectionRef.id).doc(`temperaturemonitor(${i})`).set(nestedData, { merge: true });
//                                 console.log(`Temp_Humidity_DataBridge: Updated temperature data for device ${DeviceID} in collection ${collectionRef.id}, document 'device'`);

//                                 // Mark the device as having new entries with its index and collection ID
//                                 tempUpdatedDeviceMap.set(DeviceID, { index: i, collectionID: collectionRef.id });
//                                 console.log(`Temp_Humidity_DataBridge: ${DeviceID} has been processed and added to be cleaned up with index ${i} in collection ${collectionRef.id}!`);
//                             }
//                         }
//                     } else {
//                         console.error(`Temp_Humidity_DataBridge: 'devices.temperaturemonitor' is not an array or 'devices' is missing in document ${deviceDoc.id} of collection ${collectionRef.id}`);
//                     }

//                     if (currentHour === 12 && currentMinute < 5 || currentHour === 11 && currentMinute >= 58){
//                         if (deviceData.devices && Array.isArray(deviceData.devices.powermonitor)) {
//                             const powermonitors = deviceData.devices.powermonitor;
    
//                             for (let i = 0; i < powermonitors.length; i++) {
//                                 const deviceId = powermonitors[i].deviceID;
//                                 if (EnergyDeviceDataMap.has(deviceId)) {
//                                     const nestedData = {
//                                         data: admin.firestore.FieldValue.arrayUnion(...EnergyDeviceDataMap.get(deviceId))
//                                     };
    
//                                     await db1.collection(collectionRef.id).doc(`powermonitor(${i})`).set(nestedData, { merge: true });
//                                     console.log(`NewEnergyDataBridge: Updated power monitor data for device ${deviceId} in collection ${collectionRef.id}, document 'device'`);
    
//                                     // Mark the device as processed
//                                     EnergyDeviceDataMap.delete(deviceId);
//                                 }
//                             }
//                         } else {
//                             console.error(`NewEnergyDataBridge: 'devices.powermonitor' is not an array or 'devices' is missing in document ${deviceDoc.id} of collection ${collectionRef.id}`);
//                         }    
//                     } else {
//                         console.log('NewEnergyDataBridge: Current hours are not 12');
//                     }

//                 } else {
//                     console.error(`LevelIndicatorDataBridge: 'device' document does not exist in collection ${collectionRef.id}`);
//                 }
//             }
//         }

//         async function cleanupOldLevelEntries(deviceMapEntries) {
//             const currentHour = new Date().getHours();
//             const currentMinute = new Date().getMinutes();

//             if (!(currentHour === 12 && currentMinute < 5 || currentHour === 11 && currentMinute >= 58)) {
//                 console.log('cleanupOldEntries: Current hour is not 12, skipping cleanup.');
//                 return;
//             }
        
//             const twentyFourHoursAgo = admin.firestore.Timestamp.fromDate(new Date(Date.now() - 24 * 60 * 60 * 1000));
//             try {
//                 for (const [DeviceID, { index, collectionID }] of deviceMapEntries) {
//                     const docRef = db1.collection(collectionID).doc(`waterlevelindicator(${index})`);
//                     const dataDoc = await docRef.get();
//                     if (dataDoc.exists) {
//                         const data = dataDoc.data().data;
//                         const filteredData = data.filter(entry => entry.time >= twentyFourHoursAgo);
//                         console.log(`LevelIndicatorDataBridge: filtered Data for device ${DeviceID} in collection ${collectionID}:`, filteredData);
//                         await docRef.update({ data: filteredData });
//                         console.log(`LevelIndicatorDataBridge: Cleaned up old entries for device ${DeviceID} in collection ${collectionID}:`, filteredData);
//                     } else {
//                         console.error(`LevelIndicatorDataBridge: waterlevelindicator(${index}) document does not exist in collection ${collectionID}`);
//                     }
//                 }
//             } catch (error) {
//                 console.error('LevelIndicatorDataBridge: Error cleaning up old entries:', error);
//             }
//         }

//         async function cleanupOldTempEntries(deviceMapEntries) {
//             const currentHour = new Date().getHours();
//             const currentMinute = new Date().getMinutes();

//             if (!(currentHour === 12 && currentMinute < 5 || currentHour === 11 && currentMinute >= 58)) {
//                 console.log('cleanupOldEntries: Current hour is not 12, skipping cleanup.');
//                 return;
//             }

//             const twentyFourHoursAgo = admin.firestore.Timestamp.fromDate(new Date(Date.now() - 24 * 60 * 60 * 1000));
//             try {
//                 for (const [DeviceID, { index, collectionID }] of deviceMapEntries) {
//                     const docRef = db1.collection(collectionID).doc(`temperaturemonitor(${index})`);
//                     const dataDoc = await docRef.get();
//                     if (dataDoc.exists) {
//                         const data = dataDoc.data().data;
//                         const filteredData = data.filter(entry => entry.time >= twentyFourHoursAgo);
//                         console.log(`Temp_Humidity_DataBridge: filtered Data for device ${DeviceID} in collection ${collectionID}:`, filteredData);
//                         await docRef.update({ data: filteredData });
//                         console.log(`Temp_Humidity_DataBridge: Cleaned up old entries for device ${DeviceID} in collection ${collectionID}:`, filteredData);
//                     } else {
//                         console.error(`Temp_Humidity_DataBridge: temperaturemonitor(${index}) document does not exist in collection ${collectionID}`);
//                     }
//                 }
//             } catch (error) {
//                 console.error('Temp_Humidity_DataBridge: Error cleaning up old entries:', error);
//             }
//         }

//         client.on('error', (err) => {
//             console.error('LevelIndicatorDataBridge: MQTT Client Error:', err);
//         });
//     }
//     else{
//         console.log('Merged_Data_Bridge: NOT THE RIGHT TIME!');
//     }
// }});
