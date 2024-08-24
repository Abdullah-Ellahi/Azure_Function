// const { firestore } = require('@google-cloud/firestore');
// const { TableClient, AzureNamedKeyCredential } = require('@azure/data-tables');
// const { app } = require('@azure/functions');
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

// // Azure Table Storage credentials and setup
// const account = 'asanidatabridgestore';
// const accountKey = 'BC4aRhRVJwq51lTaGlqC19UQGy7GtEG2nPEThpbcNMzv6qqseKfSX9l+/d4xEXoE7LweqLtmFPou+AStlt8TBw==';
// const tableName = 'DeviceDataTable';
// const credential = new AzureNamedKeyCredential(account, accountKey);
// const tableClient = new TableClient(`https://${account}.table.core.windows.net`, tableName, credential);

// app.http('Azure_Table_Device_Inserter', {
//     // schedule: '*/30 * * * * *',
//     // process.env.TIMER_SCHEDULE_2 || '*/30 * * * *', // If TIMER_SCHEDULE_2 not available then every 30 mins
//     handler: async (myTimer, context) => {
//     const timeStamp = new Date().toISOString();
//     context.log('Azure_Table_Device_Inserter: HTTP trigger function ran!', timeStamp);

//     const updatedDeviceMap = new Map();

//     try {
//         // Fetch all collections from Firestore
//         const collections = await db1.listCollections();
//         let key = 0;

//         for (const collectionRef of collections) {
//             const deviceDoc = await collectionRef.doc('device').get();
//             if (deviceDoc.exists) {
//                 const deviceData = deviceDoc.data();

//                 // Process 'waterlevelindicator' devices
//                 if (deviceData.devices && Array.isArray(deviceData.devices.waterlevelindicator)) {
//                     const waterLevelIndicators = deviceData.devices.waterlevelindicator;

//                     for (let i = 0; i < waterLevelIndicators.length; i++) {
//                         const DeviceID = waterLevelIndicators[i].deviceID;
//                         // if (updatedDeviceMap.has(DeviceID)) {
//                             updatedDeviceMap.set(key, {deviceID: DeviceID, index: i, collectionID: collectionRef.id, type: 'waterlevelindicator' });
//                             key++;
//                             context.log(`Azure_Table_Device_Inserter: ${DeviceID} has been Added and added to be cleaned up with index ${i} in collection ${collectionRef.id}!`);
//                         // }
//                     }
//                 } else {
//                     context.log(`Azure_Table_Device_Inserter: 'devices.waterlevelindicator' is not an array or 'devices' is missing in document ${deviceDoc.id} of collection ${collectionRef.id}`);
//                 }

//                 // Process 'temperaturemonitor' devices
//                 if (deviceData.devices && Array.isArray(deviceData.devices.temperaturemonitor)) {
//                     const temperatureMonitors = deviceData.devices.temperaturemonitor;

//                     for (let i = 0; i < temperatureMonitors.length; i++) {
//                         const DeviceID = temperatureMonitors[i].deviceID;
//                         // if (updatedDeviceMap.has(DeviceID)) {
//                             updatedDeviceMap.set(key, {deviceID: DeviceID, index: i, collectionID: collectionRef.id, type: 'temperaturemonitor' });
//                             key++;
//                             context.log(`Azure_Table_Device_Inserter: ${DeviceID} has been processed and added to be cleaned up with index ${i} in collection ${collectionRef.id}!`);
//                         // }
//                     }
//                 } else {
//                     context.log(`Azure_Table_Device_Inserter: 'devices.temperaturemonitor' is not an array or 'devices' is missing in document ${deviceDoc.id} of collection ${collectionRef.id}`);
//                 }

//                 // Process 'powermonitor' devices
//                 const currentHour = new Date().getHours();
//                 const currentMinute = new Date().getMinutes();
//                 // if (currentHour === 12 && currentMinute < 5 || currentHour === 11 && currentMinute >= 58) {
//                     if (deviceData.devices && Array.isArray(deviceData.devices.powermonitor)) {
//                         const powerMonitors = deviceData.devices.powermonitor;

//                         for (let i = 0; i < powerMonitors.length; i++) {
//                             const DeviceID = powerMonitors[i].deviceID;
//                             // if (updatedDeviceMap.has(DeviceID)) {
//                                 updatedDeviceMap.set(key, {deviceID: DeviceID, index: i, collectionID: collectionRef.id, type: 'powermonitor' });
//                                 key++;
//                                 context.log(`Azure_Table_Device_Inserter: ${DeviceID} has been processed and added to be cleaned up with index ${i} in collection ${collectionRef.id}!`);
//                             // }
//                         }
//                     } else {
//                         context.log(`Azure_Table_Device_Inserter: 'devices.powermonitor' is not an array or 'devices' is missing in document ${deviceDoc.id} of collection ${collectionRef.id}`);
//                     }
//                 // } else {
//                 //     context.log('NewEnergyDataBridge: Current hours are not 12');
//                 // }

//             } else {
//                 context.log(`Azure_Table_Device_Inserter: 'device' document does not exist in collection ${collectionRef.id}`);
//             }
//         }

//         console.log('Azure_Table_Device_Inserter: ALL COLLECTION READ COMPLETE!');

//         // List and delete all entities in the table
//         try {
//             const entities = tableClient.listEntities();
            
//             for await (const entity of entities) {
//                 await tableClient.deleteEntity(entity.partitionKey, entity.rowKey);
//                 console.log(`Azure_Table_Device_Inserter: Deleted entity: ${entity.partitionKey}, ${entity.rowKey}`);
//             }
            
//             console.log("Azure_Table_Device_Inserter: All entities have been deleted.");
//         } catch (error) {
//             console.error("Azure_Table_Device_Inserter: Error clearing the table:", error);
//         }

//         // Store entries in Azure Table Storage
//         for (const [key, {deviceID, index, collectionID, type }] of updatedDeviceMap.entries()) {
//             const entry = {
//                 partitionKey: `${deviceID}-${index}`,
//                 rowKey: `${deviceID}-${index}`,
//                 userUID: `${collectionID}`,
//                 deviceID: `${deviceID}`,
//                 index: `${index}`,
//                 type: `${type}`
//             };

//             // Add entry to Azure Table Storage
//             await tableClient.createEntity(entry);
//             context.log(`Azure_Table_Device_Inserter: Added entry for deviceID: ${deviceID}, index: ${index}, type: ${type}`);
//         }

//         context.log('Azure_Table_Device_Inserter: Data processing and storage completed.');
//     } catch (error) {
//         context.error('Azure_Table_Device_Inserter: Error processing data:', error);
//     }
// }});
