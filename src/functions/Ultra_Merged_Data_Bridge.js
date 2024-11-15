const { app } = require('@azure/functions');
const mqtt = require('mqtt');
const admin = require('firebase-admin');
const { Firestore, Timestamp } = require('@google-cloud/firestore');
const { PassThrough } = require('stream');
const { google } = require('googleapis');
const fs = require('fs');
const path = require('path');
const pdfMake = require('pdfmake/build/pdfmake');
const pdfFonts = require('pdfmake/build/vfs_fonts');
const { TableClient, AzureNamedKeyCredential } = require('@azure/data-tables');
const axios = require('axios');

const serviceAccount = JSON.parse(Buffer.from(process.env.FIRESTORE_KEY, 'base64').toString());

if (!admin.apps.length) {
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        name: 'asani-mobile'
    },
        'asani-mobile'
    );
}

const getFirestoreInstance = () => {
    if (!admin.apps.find(app => app.name === 'asani-mobile')) {
        throw new Error('Firebase app named "asani-mobile" does not exist');
    }
    return admin.firestore(admin.app('asani-mobile'));
};

const db = getFirestoreInstance();

// Azure Table Storage credentials and setup
const account = 'asanidatabridgestore';
const accountKey = process.env.AZURE_TABLE_KEY;
const tableName = 'DeviceDataTable';
const credential = new AzureNamedKeyCredential(account, accountKey);
const tableClient = new TableClient(`https://${account}.table.core.windows.net`, tableName, credential);


pdfMake.vfs = pdfFonts.pdfMake.vfs;

const base64Key = process.env.FIRESTORE_KEY;

// Decode the base64 string to get the JSON key
const jsonKey = Buffer.from(base64Key, 'base64').toString('utf8');
const keyObject = JSON.parse(jsonKey);

// Firestore and Google Drive configurations
const firestore = new Firestore({
    credentials: keyObject,
    projectId: keyObject.project_id, // Ensure this matches the Project ID in your key
});

const drive = google.drive('v3');


const SLACK_WEBHOOK_URL = process.env.SLACK_WEBHOOK_URL;

async function notifySlack(message) {
    const slackPayload = {
        text: message
    };
    try {
        const response = await axios.post(SLACK_WEBHOOK_URL, slackPayload);
        console.log(`Slack notification sent successfully: ${response.status}`);
    } catch (error) {
        console.error(`Failed to send Slack notification. Error: ${error.message}`);
    }
}

app.timer('Ultra_Merged_Data_Bridge', {
    schedule: '*/30 * * * * *',
    handler: async (myTimer, context) => {

        const timeStamp = new Date().toLocaleString('en-PK', { timeZone: 'Asia/Karachi' });
        context.log('D542_Data_Bridge: function ran at:', timeStamp);

        // // Set the date and time for the test purposes
        // const date = new Date(Date.UTC(2024, 7, 16, 5, 0, 0)); // 6 PM UTC
        // // Extract the current hour, minute, and second
        // const currentHour = date.getHours(); // 23 (11 PM in Pakistan)
        // const currentMinute = date.getMinutes(); // 0
        // const currentSec = date.getSeconds(); // 0

        let currentHour = new Date().getHours();
        let currentMinute = new Date().getMinutes();
        let currentSec = new Date().getSeconds();

        if (// Pharmevo_PDF_Generator
            (currentHour === 9 && currentMinute === 59 && currentSec >= 50) ||
            (currentHour === 10 && currentMinute === 0 && currentSec <= 25)
        ) {
            try {
                // Calculate the timestamp for the past 24 hours in PKT
                const utcNow = new Date();
                const pkNow = new Date(utcNow.getTime() + 5 * 60 * 60 * 1000); // PKT is UTC+5

                // const pkNow = new Date(Date.UTC(2024, 7, 13, 5, 0, 0) + 5 * 60 * 60 * 1000); // Month is zero-indexed (7 = August)

                // // Calculate utcNow by subtracting 5 hours from pkNow (PKT is UTC+5)
                // const utcNow = new Date(pkNow.getTime() - 5 * 60 * 60 * 1000);

                const past24Hours = new Date(utcNow.getTime() - 24 * 60 * 60 * 1000);

                console.log('utcNow', utcNow);
                console.log('pkNow', pkNow);
                console.log('past24Hours', past24Hours);

                let currentTime = new Date(past24Hours.getTime());// current time a day ago

                // Get the date components for the current date for the pdf header
                let currentYear = pkNow.getUTCFullYear();
                let currentMonth = (pkNow.getUTCMonth() + 1).toString().padStart(2, '0'); // Months are zero-indexed
                let currentDay = pkNow.getUTCDate().toString().padStart(2, '0');

                // Get the date components for the previous date
                let previousYear = currentTime.getUTCFullYear();
                let previousMonth = (currentTime.getUTCMonth() + 1).toString().padStart(2, '0'); // Months are zero-indexed
                let previousDay = currentTime.getUTCDate().toString().padStart(2, '0');

                // Construct the date strings
                let currentDateString = `${currentYear}/${currentMonth}/${currentDay}`;
                let previousDateString = `${previousYear}/${previousMonth}/${previousDay}`;

                // Construct the final date string in the format "todaysdate - previousdate"
                let dateString = `${currentDateString} - ${previousDateString}`;

                // Fetch the specific document 'tempconductivity' inside the 'D542' collection
                const docRef = firestore.collection('PdlRtsNxcOQGzNwZHYIqnfk3fQL2').doc('tempconductivity');
                const doc = await docRef.get();

                if (!doc.exists) {
                    context.log('PharmevoPDFGenerator: Document "tempconductivity" not found.');
                    return;
                }

                // Get the array named 'data' from the document
                const arrayField = doc.data()['data'];

                if (!Array.isArray(arrayField)) {
                    context.log('PharmevoPDFGenerator: Field "data" is not an array.');
                    return;
                }

                // Initialize variables
                const entries = [];
                const intervalMinutes = 15;
                const intervalMillis = intervalMinutes * 60 * 1000;

                try {
                    // Iterate over each interval
                    while (currentTime <= pkNow) {
                        // Find an entry for the current interval
                        const matchingEntries = arrayField.filter((item) => {
                            const itemTime = item.time instanceof Timestamp ? item.time.toDate() : new Date(item.time);
                            return (
                                itemTime >= new Date(currentTime.getTime() - 2 * 60 * 1000) && itemTime <= new Date(currentTime.getTime() + 2 * 60 * 1000)
                            );
                        });

                        // If no entry found, try to find nearby entries
                        if (matchingEntries.length === 0) {
                            const nearbyEntries = arrayField.filter((item) => {
                                const itemTime = item.time instanceof Timestamp ? item.time.toDate() : new Date(item.time);
                                return (
                                    itemTime >= new Date(currentTime.getTime() - 5 * 60 * 1000) && itemTime <= new Date(currentTime.getTime() + 5 * 60 * 1000)
                                );
                            });

                            if (nearbyEntries.length > 0) {
                                entries.push(nearbyEntries[0]); // Choose the closest entry
                                // console.log('A nearby entry', nearbyEntries[0]);
                            }
                        } else {
                            entries.push(matchingEntries[0]); // Choose the first entry for the interval
                            // console.log('A matching entry', matchingEntries[0]);
                        }

                        // Move to the next interval
                        currentTime = new Date(currentTime.getTime() + intervalMillis);
                    }
                }
                catch (error) {
                    notifySlack(`PharmevoPDFGenerator: Error processing the array "data": ${error.message}`);
                    console.log(`PharmevoPDFGenerator: Error processing the array "data": ${error.message}`);
                }

                context.log(`PharmevoPDFGenerator: Completed processing. Total entries found: ${entries.length}.`);

                if (entries.length === 0) {
                    context.log('PharmevoPDFGenerator: No data found in the array "data" for the past 24 hours.');
                    return;
                }

                const docDefinition = {
                    content: [
                        {
                            text: `Report for ${dateString}`,
                            style: 'header',
                            margin: [0, 0, 0, 20] // Adds 20 units of space after the header
                        },
                        {
                            table: {
                                headerRows: 2,
                                widths: [60, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30],
                                body: buildTableBody(entries),
                            },
                            alignment: 'center', // Center-aligns the table within the document
                            fontSize: 8, // Reduced font size
                            layout: {
                                hLineWidth: function (i, node) {
                                    return (i === 0 || i === node.table.body.length) ? 1 : 0.5;
                                },
                                vLineWidth: function (i, node) {
                                    return 0.5; // Defines the width of vertical lines
                                },
                                hLineColor: function (i, node) {
                                    return '#aaaaaa'; // Horizontal line color
                                },
                                vLineColor: function (i, node) {
                                    return '#aaaaaa'; // Vertical line color
                                },
                                paddingLeft: function (i, node) { return 10; },
                                paddingRight: function (i, node) { return 4; },
                                paddingTop: function (i, node) { return 2; },
                                paddingBottom: function (i, node) { return 2; },
                            },
                        },
                    ],
                    styles: {
                        header: {
                            fontSize: 18,
                            bold: true,
                            alignment: 'center', // Center-aligns the header
                        },
                        tableContent: {
                            alignment: 'center', // Center-aligns the text in each cell
                        },
                    },
                    defaultStyle: {
                        alignment: 'center' // Center-aligns all text by default
                    }
                };

                console.log("PharmevoPDFGenerator: Generating PDF Starting...");

                try {
                    // Generate the PDF and stream it directly to Google Drive
                    const pdfDoc = pdfMake.createPdf(docDefinition);

                    // Convert the PDF document to a buffer and then create a readable stream from it
                    pdfDoc.getBuffer(async (buffer) => {
                        try {
                            const stream = new PassThrough();
                            stream.end(buffer); // Write the buffer to the stream

                            stream.on('end', () => {
                                console.log("PharmevoPDFGenerator: Stream ended successfully");
                            });

                            stream.on('error', (err) => {
                                console.error("PharmevoPDFGenerator: Stream error", err);
                            });

                            console.log("PharmevoPDFGenerator: Stream created successfully");

                            const credentials = {
                                "type": "service_account",
                                "project_id": "asani-23d6e",
                                "private_key_id": "bc02eb03ccd0cbd3939bc7abbc8510fce40eb71d",
                                "private_key": `-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCret7VONboAzyC\nr9e6SrhpeUl/kIDbsN1CZUG49gLV2uXOVBFcymSNhlpF5fUsAjW9bOmSRebD+FJK\nzizcTaVKFBWBthHvmxAXg7nckicGXm30o2fzC7GIK/SJmnNHuw5NDrsKz6hJfNr7\n87Aahk3aXhy9M6TWUoP3YHWwaYTyV1M07BJ+eQsrBziI3Ns09rqK22cf2e3WKai8\n34LOUU3HmjTaNQN1JAvZ1nKSE7nehM1Bsb0az0vNfcU8RVPiLk3gwCi/rIUfqRnY\nb9PrIpeGbMrWikQ8nAv1VLQPPLF1GYgpg0Tgz0HmsrC0SBLWVA45rDgErwBySoxq\nELN7ZHHjAgMBAAECggEAI1zMVTZKg0Afm2e92ioMi5u6f3cCS8vWZRnSit0YypdA\nquMFTZhjUhgxx5Zbj/7IrE6ogESwCRvNhKWt9KSCgJtRsBUm772PfPnqO0VcjOCu\nVjPYfPOdYdKGtFXIjjNSCKwQAHPAZZ883ZZsa5vktDKSRKZAyP3tPveCbaXlB8CJ\njT46agE1zDPC2ks1IKA6K8lIBqXMyY+Y33uaPu2KfkKNKO1Q55QmmQwh1xxAjuXt\nUyC12UDuanlbz5+h1qqcED/SgSojm5xnCf8tqGPn2+zWQSs+ziftrQDkU+Ou2rhV\niK0iWvWnz0dfvWdOMgt6tFP1g6LfwgVyg6kvscZrWQKBgQDnbq+rhk8irbWjHfJQ\n6cfbD76UjBvxb9G5BDxvTprQQhTfzrYP6Zl2qmb7eJowNpBGuW67+Yyb29PotKWU\nLA5qv3Hd17jdT8zi8URvAoXTaHw27mgWIyzpcZHlQN8NJ9wNTQ2N481D334AujM6\nExVVcSeXZXGglkuhN8Qb3ZbZPQKBgQC9rvEJ8PQ2KZqQ2gJmJ9mGSH3AfSqqmgwy\nywVGtTCerHbtq3hmj/kCwrHqZPTf89186KM1+oJu3u8qHb/PDHjmgSKZJ8YawZjE\n2J41KgvHqYwk8eXoSnEgfTv0PU7ynnDJ7K4uF+LbLR/BmNoKKBQ4jNamH3NBuYla\n5Rj83JfpnwKBgDIc7p8WmyYoNS+X+XN529PciekCmb5i9ye3+hokDDPSS1DXIuc5\nLwjaByimycRs3DrkRk0f0UmihTVbgIsgKxMEV3SxFI+fiv8NPUNIaZAIjPUcFJ/O\niBgYhn382Q+xNN4QdEHoNPdgeplPuG332ePBd9A5oCwkv0WunKFoDxUFAoGBAJPz\nVlM4vGNaxBLj2ScxQfv5owx3FMaI7abYsD7cqRW60/DB5GGrPsEmd3uqT043erUp\nB9Tclsw/2zBwj+HBirPQWdo4Kc9I3g38IDq7lwCfgI54l6k+F0sOI6t3/pJDfxoh\nICMXwnzOj4yD2t49MLiDMhHVSiMjwr1BIOJqdZtlAoGBAN9Udm8SQS3iUL/7acDH\np2lu9dvdKUH098Lq+agxpVF6Oy6jlWt7hj5R91vVu+BIUwfewmucXGgy0KJdl5jo\nCjZdTzPdT1f2slDI7H23I2g/nuxxqQXkiTrfbdr9jj5uyOQMf3H27Uw0caJl0Mil\nS3ZoExmFLaPMP4SuOfZ9qNl4\n-----END PRIVATE KEY-----\n`,
                                "client_email": "asani-api-services@asani-23d6e.iam.gserviceaccount.com",
                                "client_id": "113166054771521531055",
                                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                                "token_uri": "https://oauth2.googleapis.com/token",
                                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                                "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/asani-api-services%40asani-23d6e.iam.gserviceaccount.com",
                                "universe_domain": "googleapis.com"
                            };

                            // Initialize Google Auth with the provided credentials
                            const auth = new google.auth.GoogleAuth({
                                credentials: credentials,
                                scopes: ['https://www.googleapis.com/auth/drive.file'],
                            });

                            console.log("PharmevoPDFGenerator: Authenticated successfully");

                            const driveService = google.drive({ version: 'v3', auth });

                            const pktFormattedDate = pkNow.toISOString().replace(/T/, '_').replace(/\..+/, '') + '_PKT';

                            const fileMetadata = {
                                name: `Report_${pktFormattedDate}.pdf`,
                                parents: [`${process.env.DRIVE_FOLDER_ID}`], // Ensure this is the correct folder ID
                            };

                            console.log("PharmevoPDFGenerator: Uploading PDF to Google Drive");

                            const media = {
                                mimeType: 'application/pdf',
                                body: stream, // Streaming the PDF directly
                            };

                            const response = await driveService.files.create({
                                resource: fileMetadata,
                                media: media,
                                fields: 'id',
                            });

                            console.log('PharmevoPDFGenerator: PDF uploaded successfully, File ID:', response.data.id);
                        } catch (error) {
                            notifySlack(`PharmevoPDFGenerator: Error during PDF upload to Drive: ${error.message}`);
                            console.error('PharmevoPDFGenerator: Error during PDF upload to Drive: ', error);
                        }
                    });

                    console.log("PharmevoPDFGenerator: PDF generation crossed successfully");
                } catch (error) {
                    notifySlack(`PharmevoPDFGenerator: Error occurred while generating the PDF: ${error.message}`);
                    console.error('PharmevoPDFGenerator: Error occurred while generating the PDF', error);
                }
            } catch (error) {
                notifySlack(`PharmevoPDFGenerator: Error occurred except during PDF generation: ${error.message}`);
                context.log('PharmevoPDFGenerator: Error occurred except during PDF generation:', error);
            }

            function buildTableBody(entries) {
                // Define the header rows
                const headerRow1 = [
                    { text: 'Timestamp', rowSpan: 2, alignment: 'center' },
                    { text: 'After Mix-Bed Conductivity', colSpan: 2, alignment: 'center' }, {},
                    { text: 'Non-Ceph Loop (Supply)', colSpan: 2, alignment: 'center' }, {},
                    { text: 'Non-Ceph Loop (Return)', colSpan: 2, alignment: 'center' }, {},
                    { text: 'Ceph Loop (Supply)', colSpan: 2, alignment: 'center' }, {},
                    { text: 'Ceph Loop (Return)', colSpan: 2, alignment: 'center' }, {},
                ];

                const headerRow2 = [
                    { text: 'Timestamp', alignment: 'center' },
                    { text: 'Temp.', alignment: 'center' },
                    { text: 'Cond.', alignment: 'center' },
                    { text: 'Temp.', alignment: 'center' },
                    { text: 'Cond.', alignment: 'center' },
                    { text: 'Temp.', alignment: 'center' },
                    { text: 'Cond.', alignment: 'center' },
                    { text: 'Temp.', alignment: 'center' },
                    { text: 'Cond.', alignment: 'center' },
                    { text: 'Temp.', alignment: 'center' },
                    { text: 'Cond.', alignment: 'center' },
                ];

                // Combine the headers with the data
                const body = [];
                body.push(headerRow1);
                body.push(headerRow2);

                try {
                    // Add data rows
                    entries.forEach((item) => {
                        let adjusted = false; // Initialize adjusted flag

                        let itemTime = item.time instanceof Timestamp ? item.time.toDate() : new Date(item.time);
                        itemTime = new Date(itemTime.setHours(itemTime.getHours() + 5)); // Add five hours

                        const minutes = itemTime.getMinutes();
                        let adjustedMinutes;

                        if (minutes !== 0 || minutes !== 15 || minutes !== 30 || minutes !== 45) {
                            // Adjust minutes to nearest 0, 15, 30, or 45
                            if (minutes >= 0 && minutes < 10) {
                                adjustedMinutes = 0;
                            } else if (minutes >= 10 && minutes < 25) {
                                adjustedMinutes = 15;
                            } else if (minutes >= 25 && minutes < 40) {
                                adjustedMinutes = 30;
                            } else if (minutes >= 40 && minutes < 55) {
                                adjustedMinutes = 45;
                            } else if (minutes >= 55) {
                                adjustedMinutes = 0;
                                itemTime.setHours(itemTime.getHours() + 1); // Adjust hour if rolling over
                            }

                            if (adjustedMinutes !== minutes) {
                                adjusted = true;
                                itemTime.setMinutes(adjustedMinutes); // Set adjusted minutes
                            }
                        }
                        // // Log the original time
                        console.log("Original time:", itemTime);

                        // Get hours, minutes, and seconds
                        let hours = itemTime.getUTCHours();
                        let minutesStr = itemTime.getUTCMinutes().toString().padStart(2, '0');
                        // let seconds = itemTime.getUTCSeconds().toString().padStart(2, '0');

                        // Determine AM or PM
                        let ampm = hours >= 12 ? 'PM' : 'AM';

                        // Convert 24-hour time to 12-hour time
                        hours = hours % 12;
                        hours = hours ? hours : 12; // the hour '0' should be '12'

                        // Construct the date and time string
                        let timeString = `${hours.toString().padStart(2, '0')}:${minutesStr} ${ampm}`;

                        if (adjusted) {
                            timeString = timeString + '.';
                        }

                        let row = [
                            timeString,
                            item.t1 || '',
                            item.c1 || '',
                            item.t2 || '',
                            item.c2 || '',
                            item.t3 || '',
                            item.c3 || '',
                            item.t4 || '',
                            item.c4 || '',
                            item.t5 || '',
                            item.c5 || '',
                        ];

                        body.push(row);
                        // console.log(row);
                    });
                } catch (error) {
                    notifySlack(`PharmevoPDFGenerator: Error occurred while building the PDF table body: ${error.message}`);
                    console.log('PharmevoPDFGenerator: Error occurred while building the PDF table body', error);
                }

                return body;
            }
        }
        else {
            console.log('PharmevoPDFGenerator: NOT THE RIGHT TIME! : ', currentHour, currentMinute, currentSec);
        }

        if (// D542_Data_Bridge
            (currentMinute % 5 === 4 && currentSec >= 50) ||
            (currentMinute % 5 === 0 && currentSec <= 10)
        ) {

            const client = mqtt.connect({
                host: process.env.MQTT_BROKER,
                port: process.env.MQTT_PORT
            });

            client.on('connect', () => {
                const topics = [
                    '/asani/devices/gateway/PowerMeter/1/A55D6WXK',
                    '/asani/devices/gateway/PowerMeter/2/A55D6WXK',
                    '/asani/devices/gateway/PowerMeter/3/A55D6WXK',
                    '/asani/devices/gateway/PowerMeter/4/A55D6WXK',
                    '/asani/devices/gateway/PowerMeter/5/A55D6WXK',
                ];

                client.subscribe(topics, (err, granted) => {
                    if (!err) {
                        console.log('D542_Data_Bridge: Subscribed to topics', granted.map(grant => grant.topic));
                        gatherCurrentValues();
                    } else {
                        console.error('D542_Data_Bridge: Subscription error:', err);
                        client.end();
                    }
                });
            });

            const subtopics = new Set(); // Use a Set to avoid duplicates
            const messageQueue = [];

            client.on('message', (topic, message) => {
                const subtopic = topic.split('/').pop(); // Extract the last part of the topic as subtopic
                subtopics.add(subtopic);
                messageQueue.push({ topic, message });
            });

            async function gatherCurrentValues() {
                // Wait for 6 seconds to gather subtopics
                await new Promise(resolve => setTimeout(resolve, 12000));

                // Process all messages at once
                await processMessages(messageQueue);

                client.end();
                console.log('D542_Data_Bridge: Client Disconnected!');
            }

            async function processMessages(messages) {
                const tempCondDataMap = new Map();

                // Aggregate data for each device
                messages.forEach( async ({ topic, message }) => {
                    try {
                        const data = JSON.parse(message.toString());

                        if (data.TDS210C && data.TDS210C.Conductivity && data.TDS210C.Temperature) {
                            const fieldId = 'sharedKey'; // Use a shared key to store all topics together.

                            // Retrieve the existing entry if it exists, otherwise create a new one
                            let entry = tempCondDataMap.get(fieldId) || {
                                time: admin.firestore.Timestamp.now(),
                                c1: 0, t1: 0,
                                c2: 0, t2: 0,
                                c3: 0, t3: 0,
                                c4: 0, t4: 0,
                                c5: 0, t5: 0
                            };

                            if (topic === '/asani/devices/gateway/PowerMeter/1/A55D6WXK') {
                                if (parseFloat(entry.c1) === 0 || parseFloat(entry.t1) === 0) {
                                    console.log('message from topic 1', data);
                                    entry.c1 = data.TDS210C.Conductivity;
                                    entry.t1 = data.TDS210C.Temperature;
                                } else {
                                    console.log('message from topic 1: already have non-zero values', data);
                                }
                            } else if (topic === '/asani/devices/gateway/PowerMeter/2/A55D6WXK') {
                                if (parseFloat(entry.c2) === 0 || parseFloat(entry.t2) === 0) {
                                    console.log('message from topic 2', data);
                                    entry.c2 = data.TDS210C.Conductivity;
                                    entry.t2 = data.TDS210C.Temperature;
                                } else {
                                    console.log('message from topic 2: already have non-zero values', data);
                                }
                            } else if (topic === '/asani/devices/gateway/PowerMeter/3/A55D6WXK') {
                                if (parseFloat(entry.c3) === 0 || parseFloat(entry.t3) === 0) {
                                    console.log('message from topic 3', data);
                                    entry.c3 = data.TDS210C.Conductivity;
                                    entry.t3 = data.TDS210C.Temperature;
                                } else {
                                    console.log('message from topic 3: already have non-zero values', data);
                                }
                            } else if (topic === '/asani/devices/gateway/PowerMeter/4/A55D6WXK') {
                                if (parseFloat(entry.c4) === 0 || parseFloat(entry.t4) === 0) {
                                    console.log('message from topic 4', data);
                                    entry.c4 = data.TDS210C.Conductivity;
                                    entry.t4 = data.TDS210C.Temperature;
                                } else {
                                    console.log('message from topic 4: already have non-zero values', data);
                                }
                            } else if (topic === '/asani/devices/gateway/PowerMeter/5/A55D6WXK') {
                                if (parseFloat(entry.c5) === 0 || parseFloat(entry.t5) === 0) {
                                    console.log('message from topic 5', data);
                                    entry.c5 = data.TDS210C.Conductivity;
                                    entry.t5 = data.TDS210C.Temperature;
                                } else {
                                    console.log('message from topic 5: already have non-zero values', data);
                                }
                            }

                            // Update the map with the modified or new entry
                            tempCondDataMap.set(fieldId, entry);

                        } else {
                            console.error('D542_Data_Bridge: Invalid data format. Temperature or Conductivity field is missing.');
                        }
                    } catch (err) {
                        await notifySlack(`D542_Data_Bridge: Message parsing error: ${err.message}\n\nMessage parsing falied on this: ${message}`);
                        console.error('D542_Data_Bridge: Message parsing error:', err);
                    }
                });

                console.log(`D542_Data_Bridge: tempCondDataMap:`, tempCondDataMap);

                // Check if there is any data to write
                if (tempCondDataMap.size === 0) {
                    console.log('No valid messages received. No data to write.');
                    return; // Exit early if no data
                }

                const tempCondDataArray = Array.from(tempCondDataMap.values()).flat(); // Convert to an array

                const nestedData = {
                    data: admin.firestore.FieldValue.arrayUnion(...tempCondDataArray),
                };

                await db.collection('PdlRtsNxcOQGzNwZHYIqnfk3fQL2').doc(`tempconductivity`).set(nestedData, { merge: true });
                console.log(`D542_Data_Bridge: Updated temperature and conductivity data`);
            }
        } else {
            console.log('D542_Data_Bridge: NOT THE RIGHT TIME! : ', currentHour, currentMinute, currentSec);
        }

        if (// Opt_Meregd_Data_Bridge
            (currentMinute === 59 && currentSec >= 50) ||
            (currentMinute === 0 && currentSec < 30)
        ) {
            tempUpdatedDeviceMap = new Map();
            levelUpdatedDeviceMap = new Map();

            const client = mqtt.connect({
                host: process.env.MQTT_BROKER,
                port: process.env.MQTT_PORT
            });

            client.on('connect', () => {
                const topics = [
                    process.env.LI_TOPIC,
                    process.env.ENERGY_TOPIC_1,
                    process.env.ENERGY_TOPIC_2,
                    process.env.ENERGY_TOPIC_3,
                ];

                client.subscribe(topics, (err, granted) => {
                    if (!err) {
                        console.log('Merged_Data_Bridge: Subscribed to topics', granted.map(grant => grant.topic));
                        gatherCurrentValues();
                    } else {
                        console.error('Merged_Data_Bridge: Subscription error:', err);
                        client.end();
                    }
                });
            });

            const subtopics = new Set(); // Use a Set to avoid duplicates
            const messageQueue = [];

            client.on('message', (topic, message) => {
                const subtopic = topic.split('/').pop(); // Extract the last part of the topic as subtopic
                subtopics.add(subtopic);
                messageQueue.push({ topic, message });
            });

            async function gatherCurrentValues() {
                // Wait for 6 seconds to gather subtopics
                await new Promise(resolve => setTimeout(resolve, 5000));

                // Process all messages at once
                await processMessages(messageQueue);

                client.end();
                console.log('LevelIndicatorDataBridge: Client Disconnected!');

                // Call the cleanup function for the updated device IDs
                console.log('LevelIndicatorDataBridge: Cleaning up: ', Array.from(levelUpdatedDeviceMap.entries()));
                await cleanupOldLevelEntries(Array.from(levelUpdatedDeviceMap.entries()));

                // Call the cleanup function for the updated device IDs
                console.log('Temp_Humid_DataBridge: Cleaning up: ', Array.from(tempUpdatedDeviceMap.entries()));
                await cleanupOldTempEntries(Array.from(tempUpdatedDeviceMap.entries()));
            }

            async function processMessages(messages) {
                const tempDeviceDataMap = new Map();
                const levelDeviceDataMap = new Map();
                const EnergyDeviceDataMap = new Map();

                // Aggregate data for each device
                messages.forEach(async ({ topic, message }) => {
                    try {
                        const data = JSON.parse(message.toString());
                        if (data.SensorData && data.SensorData.DeviceID) {
                            if (data.SensorData.Level) {
                                const DeviceID = data.SensorData.DeviceID;
                                let tankLevel = parseFloat(data.SensorData.Level);

                                // Check if the parsing was successful, otherwise set a default value (e.g., 0)
                                if (isNaN(tankLevel)) {
                                    tankLevel = 0;  // or any other default value you prefer
                                }

                                if (!levelDeviceDataMap.has(DeviceID)) {
                                    levelDeviceDataMap.set(DeviceID, []);
                                }

                                levelDeviceDataMap.get(DeviceID).push({
                                    time: admin.firestore.Timestamp.now(),
                                    value: tankLevel
                                });
                            }
                            if (data.SensorData.Temperature) {
                                const DeviceID = data.SensorData.DeviceID;
                                let temperature = parseFloat(data.SensorData.Temperature);
                                if (isNaN(temperature)) {
                                    temperature = 0;  // or any other default value you prefer
                                }

                                let humidity = parseFloat(data.SensorData.Humidity);
                                if (isNaN(humidity)) {
                                    humidity = 0;  // or any other default value you prefer
                                }

                                if (!tempDeviceDataMap.has(DeviceID)) {
                                    tempDeviceDataMap.set(DeviceID, []);
                                }

                                tempDeviceDataMap.get(DeviceID).push({
                                    time: admin.firestore.Timestamp.now(),
                                    temp: parseFloat(temperature),
                                    humid: parseFloat(humidity),
                                });
                            }
                        } else {
                            console.error('LevelIndicatorDataBridge: Invalid data format. DeviceID or Level field is missing.');
                        }

                        if (data.PowerData && data.PowerData.DeviceID) {
                            const deviceId = data.PowerData.DeviceID;
                            let energyValue = parseFloat(data.PowerData.Energy1 || data.PowerData.Energy);

                            console.log('NewEnergyDataBridge: Message recieved from device ID: '+deviceId+' with energy value: '+energyValue);

                            // Check if the parsing was successful, otherwise set a default value (e.g., 0)
                            if (isNaN(energyValue)) {
                                energyValue = 0;  // or any other default value you prefer
                            }

                            if (!EnergyDeviceDataMap.has(deviceId)) {
                                EnergyDeviceDataMap.set(deviceId, []);
                            }

                            EnergyDeviceDataMap.get(deviceId).push({
                                time: admin.firestore.Timestamp.now(),
                                value: energyValue
                            });
                        } else {
                            console.error('NewEnergyDataBridge: Invalid data format. DeviceID or Energy field is missing.');
                        }
                    } catch (err) {
                        await notifySlack(`LevelIndicatorDataBridge: Message parsing error: ${err.message}\n\nMessage parsing falied on this: ${message}`);
                        console.error('LevelIndicatorDataBridge: Message parsing error:', err);
                    }
                });

                // Fetch all device entries from Table Storage
                const entities = tableClient.listEntities();

                for await (const entity of entities) {
                    const userUID = entity.userUID;
                    const deviceID = entity.deviceID;
                    const index = entity.index;
                    const type = entity.type;

                    let deviceDataMap = new Map();
                    let dataBridgeName = new Map();

                    // Determine which device map and data bridge to use based on the type
                    switch (type) {
                        case 'waterlevelindicator':
                            deviceDataMap = levelDeviceDataMap;
                            dataBridgeName = 'LevelIndicatorDataBridge';
                            levelUpdatedDeviceMap.set(deviceID, { index: index, collectionID: userUID });
                            break;
                        case 'temperaturemonitor':
                            deviceDataMap = tempDeviceDataMap;
                            dataBridgeName = 'Temp_Humidity_DataBridge';
                            tempUpdatedDeviceMap.set(deviceID, { index: index, collectionID: userUID });
                            break;
                        case 'powermonitor':
                            if (((currentHour === 12 && currentMinute < 3) || (currentHour === 11 && currentMinute >= 58)) || ((currentHour === 0 && currentMinute < 5) || (currentHour === 23 && currentMinute >= 58))) {
                                deviceDataMap = EnergyDeviceDataMap;
                                dataBridgeName = 'NewEnergyDataBridge';
                            }
                            else {
                                console.log('NewEnergyDataBridge: Current hours are not 12');
                            }
                            break;
                        default:
                            console.error(`${type} is not a recognized device type`);
                            continue;
                    }

                    if (deviceDataMap.has(deviceID)) {
                        try {
                            console.log('Adding the entry: ', deviceDataMap.get(deviceID));
                            const nestedData = {
                                data: admin.firestore.FieldValue.arrayUnion(...deviceDataMap.get(deviceID))
                            };

                            await db.collection(userUID).doc(`${type}(${index})`).set(nestedData, { merge: true });
                            console.log(`${dataBridgeName}: Updated ${type} data for device ${deviceID} in collection ${userUID}, document '${type}'`);

                            // Mark the device as having new entries with its index and collection ID
                            // deviceDataMap.delete(deviceID); // removed this line becuase this was preventing logging for the ssame device in different accounts
                            console.log(`${dataBridgeName}: ${deviceID} has been processed and added to be cleaned up with index ${index} in collection ${userUID}!`);
                        }
                        catch (error) {
                            await notifySlack(`Opt_Merged_Data_Bridge: Error in adding data to firestore: ${err.message}\n\nError Occured here...\nuserUID: ${userUID}\deviceID: ${deviceID}`);
                            console.error('Opt_Merged_Data_Bridge: Error in adding data to firestore: ', err);    
                        }
                    } else {
                        console.error(`${dataBridgeName}: Device ${deviceID} not found in the ${type} data map`);
                    }
                }
            }

            async function cleanupOldLevelEntries(deviceMapEntries) {
                const currentHour = new Date().getHours();
                const currentMinute = new Date().getMinutes();

                if (!(currentHour === 12 && currentMinute < 5 || currentHour === 11 && currentMinute >= 58)) {
                    console.log('cleanupOldEntries: Current hour is not 12, skipping cleanup.');
                    return;
                }

                const twentyFourHoursAgo = admin.firestore.Timestamp.fromDate(new Date(Date.now() - 24 * 60 * 60 * 1000));
                try {
                    for (const [DeviceID, { index, collectionID }] of deviceMapEntries) {
                        const docRef = db.collection(collectionID).doc(`waterlevelindicator(${index})`);
                        const dataDoc = await docRef.get();
                        if (dataDoc.exists) {
                            const data = dataDoc.data().data;
                            const filteredData = data.filter(entry => entry.time >= twentyFourHoursAgo);
                            console.log(`LevelIndicatorDataBridge: filtered Data for device ${DeviceID} in collection ${collectionID}:`, filteredData);
                            await docRef.update({ data: filteredData });
                            console.log(`LevelIndicatorDataBridge: Cleaned up old entries for device ${DeviceID} in collection ${collectionID}:`, filteredData);
                        } else {
                            console.error(`LevelIndicatorDataBridge: waterlevelindicator(${index}) document does not exist in collection ${collectionID}`);
                        }
                    }
                } catch (error) {
                    await notifySlack(`LevelIndicatorDataBridge: Error cleaning up old entries: ${error.message}`);
                    console.error('LevelIndicatorDataBridge: Error cleaning up old entries:', error);
                }
            }

            async function cleanupOldTempEntries(deviceMapEntries) {
                const currentHour = new Date().getHours();
                const currentMinute = new Date().getMinutes();

                if (!(currentHour === 12 && currentMinute < 5 || currentHour === 11 && currentMinute >= 58)) {
                    console.log('cleanupOldEntries: Current hour is not 12, skipping cleanup.');
                    return;
                }

                const twentyFourHoursAgo = admin.firestore.Timestamp.fromDate(new Date(Date.now() - 24 * 60 * 60 * 1000));
                try {
                    for (const [DeviceID, { index, collectionID }] of deviceMapEntries) {
                        const docRef = db.collection(collectionID).doc(`temperaturemonitor(${index})`);
                        const dataDoc = await docRef.get();
                        if (dataDoc.exists) {
                            const data = dataDoc.data().data;
                            const filteredData = data.filter(entry => entry.time >= twentyFourHoursAgo);
                            console.log(`Temp_Humidity_DataBridge: filtered Data for device ${DeviceID} in collection ${collectionID}:`, filteredData);
                            await docRef.update({ data: filteredData });
                            console.log(`Temp_Humidity_DataBridge: Cleaned up old entries for device ${DeviceID} in collection ${collectionID}:`, filteredData);
                        } else {
                            console.error(`Temp_Humidity_DataBridge: temperaturemonitor(${index}) document does not exist in collection ${collectionID}`);
                        }
                    }
                } catch (error) {
                    await notifySlack(`Temp_Humidity_DataBridge: Error cleaning up old entries: ${error.message}`);
                    console.error('Temp_Humidity_DataBridge: Error cleaning up old entries:', error);
                }
            }

            client.on('error', (err) => {
                console.error('LevelIndicatorDataBridge: MQTT Client Error:', err);
            });
        }
        else {
            console.log('Merged_Data_Bridge: NOT THE RIGHT TIME! ', currentHour, currentMinute, currentSec);
        }

        if (// Azure_Table_Device_Inserter
            (currentHour === 23 && currentMinute === 54 && currentSec >= 55) ||
            (currentHour === 23 && currentMinute === 55 && currentSec < 30)
        ) {
            const timeStamp = new Date().toISOString();
            context.log('Azure_Table_Device_Inserter: Timer trigger function ran!', timeStamp);

            const updatedDeviceMap = new Map();

            try {
                // Fetch all collections from Firestore
                const collections = await db.listCollections();
                let key = 0;

                try {
                    for (const collectionRef of collections) {
                        const deviceDoc = await collectionRef.doc('device').get();
                        if (deviceDoc.exists) {
                            const deviceData = deviceDoc.data();

                            // Process 'waterlevelindicator' devices
                            if (deviceData.devices && Array.isArray(deviceData.devices.waterlevelindicator)) {
                                const waterLevelIndicators = deviceData.devices.waterlevelindicator;

                                for (let i = 0; i < waterLevelIndicators.length; i++) {
                                    const DeviceID = waterLevelIndicators[i].deviceID;
                                    // if (updatedDeviceMap.has(DeviceID)) {
                                    updatedDeviceMap.set(key, { deviceID: DeviceID, index: i, collectionID: collectionRef.id, type: 'waterlevelindicator' });
                                    key++;
                                    context.log(`Azure_Table_Device_Inserter: ${DeviceID} has been Added and added to be cleaned up with index ${i} in collection ${collectionRef.id}!`);
                                    // }
                                }
                            } else {
                                context.log(`Azure_Table_Device_Inserter: 'devices.waterlevelindicator' is not an array or 'devices' is missing in document ${deviceDoc.id} of collection ${collectionRef.id}`);
                            }

                            // Process 'temperaturemonitor' devices
                            if (deviceData.devices && Array.isArray(deviceData.devices.temperaturemonitor)) {
                                const temperatureMonitors = deviceData.devices.temperaturemonitor;

                                for (let i = 0; i < temperatureMonitors.length; i++) {
                                    const DeviceID = temperatureMonitors[i].deviceID;
                                    // if (updatedDeviceMap.has(DeviceID)) {
                                    updatedDeviceMap.set(key, { deviceID: DeviceID, index: i, collectionID: collectionRef.id, type: 'temperaturemonitor' });
                                    key++;
                                    context.log(`Azure_Table_Device_Inserter: ${DeviceID} has been processed and added to be cleaned up with index ${i} in collection ${collectionRef.id}!`);
                                    // }
                                }
                            } else {
                                context.log(`Azure_Table_Device_Inserter: 'devices.temperaturemonitor' is not an array or 'devices' is missing in document ${deviceDoc.id} of collection ${collectionRef.id}`);
                            }

                            // if (currentHour === 12 && currentMinute < 5 || currentHour === 11 && currentMinute >= 58) {
                            if (deviceData.devices && Array.isArray(deviceData.devices.powermonitor)) {
                                const powerMonitors = deviceData.devices.powermonitor;

                                for (let i = 0; i < powerMonitors.length; i++) {
                                    const DeviceID = powerMonitors[i].deviceID;
                                    // if (updatedDeviceMap.has(DeviceID)) {
                                    updatedDeviceMap.set(key, { deviceID: DeviceID, index: i, collectionID: collectionRef.id, type: 'powermonitor' });
                                    key++;
                                    context.log(`Azure_Table_Device_Inserter: ${DeviceID} has been processed and added to be cleaned up with index ${i} in collection ${collectionRef.id}!`);
                                    // }
                                }
                            } else {
                                context.log(`Azure_Table_Device_Inserter: 'devices.powermonitor' is not an array or 'devices' is missing in document ${deviceDoc.id} of collection ${collectionRef.id}`);
                            }
                            // } else {
                            //     context.log('NewEnergyDataBridge: Current hours are not 12');
                            // }

                        } else {
                            context.log(`Azure_Table_Device_Inserter: 'device' document does not exist in collection ${collectionRef.id}`);
                        }
                    }
                }
                catch (error) {
                    notifySlack(`Azure_Table_Device_Inserter: Error reading devices from Firestore: ${error.message}`);
                    console.error("Azure_Table_Device_Inserter: Error reading devices from Firestore: ", error);
                }

                console.log('Azure_Table_Device_Inserter: ALL COLLECTION READ COMPLETE!');

                // List and delete all entities in the table
                try {
                    const entities = tableClient.listEntities();

                    for await (const entity of entities) {
                        await tableClient.deleteEntity(entity.partitionKey, entity.rowKey);
                        console.log(`Azure_Table_Device_Inserter: Deleted entity: ${entity.partitionKey}, ${entity.rowKey}`);
                    }

                    console.log("Azure_Table_Device_Inserter: All entities have been deleted.");
                } catch (error) {
                    notifySlack(`Azure_Table_Device_Inserter: Error clearing the old table: ${error.message}`);
                    console.error("Azure_Table_Device_Inserter: Error clearing the old table:", error);
                }

                // Store entries in Azure Table Storage
                for (const [key, { deviceID, index, collectionID, type }] of updatedDeviceMap.entries()) {
                    const entry = {
                        partitionKey: `${deviceID}-${index}-${collectionID}`,
                        rowKey: `${deviceID}-${index}-${collectionID}`,
                        userUID: `${collectionID}`,
                        deviceID: `${deviceID}`,
                        index: `${index}`,
                        type: `${type}`
                    };

                    // Add entry to Azure Table Storage
                    try {
                        await tableClient.upsertEntity(entry);
                        context.log(`Azure_Table_Device_Inserter: Added entry for deviceID: ${deviceID}, index: ${index}, type: ${type}`);
                    }
                    catch (error) {
                        notifySlack(`Azure_Table_Device_Inserter: Error inserting entry in Azure table: ${error.message}\n\nError occurred when...\nuserUID: ${collectionID}\ndeviceID: ${deviceID}\nindex: ${index}\ntype: ${type}\n`);
                        context.error('Azure_Table_Device_Inserter: Error inserting entry in Azure table:', error);        
                    }
                }
                context.log('Azure_Table_Device_Inserter: Data processing and storage completed.');
            } catch (error) {
                notifySlack(`Azure_Table_Device_Inserter: Error processing entry for Azure table: ${error.message}`);
                context.error('Azure_Table_Device_Inserter: Error processing entry for Azure table:', error);
            }
        }
        else {
            console.log('Azure_Table_Device_Inserter: NOT THE RIGHT TIME! ', currentHour, currentMinute, currentSec);
        }

    }
});
