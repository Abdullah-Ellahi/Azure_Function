const { app } = require('@azure/functions');
const { TableClient, AzureNamedKeyCredential } = require('@azure/data-tables');

// Azure Table Storage credentials and setup
const account = 'asanidatabridgestore';
const accountKey = 'BC4aRhRVJwq51lTaGlqC19UQGy7GtEG2nPEThpbcNMzv6qqseKfSX9l+/d4xEXoE7LweqLtmFPou+AStlt8TBw==';
const tableName = 'DeviceDataTable';
const credential = new AzureNamedKeyCredential(account, accountKey);
const tableClient = new TableClient(`https://${account}.table.core.windows.net`, tableName, credential);

app.timer('Fetch_Device_Data', {
    schedule: '*/30 * * * * *', // Runs every 30 seconds
    handler: async (myTimer, context) => {
        const timeStamp = new Date().toLocaleString('en-PK', { timeZone: 'Asia/Karachi' });
        context.log('Function ran at:', timeStamp);

        try {
            // Retrieve all entities from the table
            const entities = tableClient.listEntities();
            const deviceDataMap = new Map();

            for await (const entity of entities) {
                const userUID = entity.userUID;
                const deviceID = entity.deviceID;
                const deviceIndex = entity.deviceIndex;

                if (!deviceDataMap.has(userUID)) {
                    deviceDataMap.set(userUID, []);
                }

                deviceDataMap.get(userUID).push({ deviceID, deviceIndex });
            }

            // Process the data
            for (const [userUID, devices] of deviceDataMap.entries()) {
                // Perform your processing logic here
                context.log(`User UID: ${userUID}`);
                context.log(`Devices:`, devices);
            }
        } catch (err) {
            context.log('Error retrieving data:', err.message);
        }
    }
});
