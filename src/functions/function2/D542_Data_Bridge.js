const { app } = require('@azure/functions');
const mqtt = require('mqtt');
const newAdmin = require('firebase-admin');
const { Firestore, Timestamp } = require('@google-cloud/firestore');
const { PassThrough } = require('stream');
const { google } = require('googleapis');
const fs = require('fs');
const path = require('path');
const pdfMake = require('pdfmake/build/pdfmake');
const pdfFonts = require('pdfmake/build/vfs_fonts');

pdfMake.vfs = pdfFonts.pdfMake.vfs;

    const newServiceAccount = JSON.parse(Buffer.from('ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAiYXNhbmktbW9iaWxlIiwKICAicHJpdmF0ZV9rZXlfaWQiOiAiMjUxMTU5NTEzNTIyMDVlNTIzMzhhYzExNzk0ZmQzMWY2NDg5NGFjNCIsCiAgInByaXZhdGVfa2V5IjogIi0tLS0tQkVHSU4gUFJJVkFURSBLRVktLS0tLVxuTUlJRXZRSUJBREFOQmdrcWhraUc5dzBCQVFFRkFBU0NCS2N3Z2dTakFnRUFBb0lCQVFDV3dxV3B6TDczSzNXOVxubTZYOEd4MmpUdVJQQXl1cWhUYVljUnpsSFYyaXV3aWtITmtVYXFWTzFNTCtjN2dYZE5KQTdwR2tjSWM1alpPOFxuZG80Y0tCYmwrMXgxOHZJTmM1MEdhcjBXM25YVEJUKzV5ZldMdWU1NXNXLzl4NDRsdE1GVXVySWQyQUp6RnVNOVxueTRGVUtoT2VJZ2FnT0tVSFovUlAwbUw2OXRDVGNKQWdSNUprSjZPUTg5UTUzM29JTWk2cFVRSzAyLzU0RFU0S1xuQTRhRHFsSktzSjRaL3FQd1hOMEN0c2ZDT0daYnY4VkNqU2JNMEJ6UG1sUWJ6QUZXNVZkWEo1WW1wclZGbngzY1xuUWlpV0xkL09oSW9hY0todnY5ak4yd2QxL2RXdDQ4bm9BM2dQNFNCNm05NGV4cUR1bmJtaU5UNmtCQUx2Q3FFNFxuVXRsdHZQa2ZBZ01CQUFFQ2dnRUFIVFBGOEI2VFRRQlRsYTh0RnlqUGRGOG5OVERaaC9XMnpnOTI5K0JZejFrb1xuUXRXOXJNazlqUWdFWDFKZDhkazBrdFJEVE5WcE5CbzY2RElVczlxR0dQVTRBemJVY1F1WWVuSTVmMUVIQktHTFxuSnZzNlAxNzhGUUZzUG4ybGlLTTdJYVRxTklLdmNzaVhxdFFkRUliMzl0VURjSXlZVnRkcGlQNnBJcENsZFRGM1xuMzR2aGJhQVJSTmcwTm9lbVhMNHAyWDdrcm9mZk1uWnY5TUNhUWFENmtjRkJURE5Rd1hkWENaUzZKRzB0QllhZ1xudllYMlVUZWtidjlHUTE4aDhxcXJjUW45MFNobWxXbnVDRjZjNWdaUFJzRXpTdnlYY2tJM2pENU1PWFpjbWZvcFxuanZxeEhWdll0a21PZ0JWbDZKNlRISkRMYVNNTXhmNUd4eURnU2V6SDZRS0JnUURPRk1ibkJwSkxpZGdnbldQTFxubGhpZXVzd2FkQnkwSVdoNEJ3aWFPNkt2L1RwSlB1dTBwMzB6QzQwQTIzb0tPTmpQS1RRWFNOdWk1Z09zdHJrN1xuTDRrVWRCRGs4aWp0WjdCZ1o2S0RsemdFeERHRHVkeDY5bXZKbXE3VmRTYU9Yc2pHR2RQVE5uUnRLMnBNaHFpSVxuVklmT3RvNzdQVldIVzJ4a1pRdjl1T1FROXdLQmdRQzdSMlFpZkwyQnl6UjhGc1lZMEljU3I2MGxOUExyMzRlSlxuUHNCcnJUL1g0N3Y4Q05sWFJCek96UGhhNnhLcWNxQlZlTm5hK0FDVlByUzAxU2pFMk5TdWYvYUZHVGpqV04yVVxuWnJTN284dDRZc3RCUWtMZy95QzlnZUp0anhkM1lPQncvaFg5bVl5RmN0eTcxdS8zVHArOUxrS25HcStkSkhZQVxuSXhKR3lHLzNHUUtCZ1FDdGFiWm1PWlZwa3prWFdObmRPeFRFblJPYlB4SFlValNDckFpRklLR3B6a0Q1MmNTbVxuWkRwcWRkSFZZdHF6TjFyYUdDWUpZZm5RZmhXaGhMRWlLTUlGUUJYblVnODJsd1pJV1d6YnBxZ3crcGRmN1VxbVxuL2kxOW5IaDZqdlkrMzJ1N3A0Z2tON0tKR3Z0OEllUTN0RW9EbklOOHp3UGx2dnpiRGx5a3lLekJ5UUtCZ0d5UlxucVpwWXFHQWJWcFR0ZXZBZHk0Qm91YjdOSUZyZm5pcFJaNm5FcVROV0FiL052WG5hc2J5dGxQallPRno1MEx0Y1xuTVNmQkNFMTlLYk4vczMvU21CR0ZlM3VUc0tnVDkzaTF4ZWJWd1BwTWc3cVVXRU1waEdoNGFMVE05SFN2ZzgzZFxuYnhBeXVRVVFIcWtDcE9EQkF0ZmdmcUZ6VmdXS2dxSDNzdnUzN1RqWkFvR0FGdGNNTEY1V0t3clNEMGdPL2tQT1xuaSsveWFHZy83ZHpmV3JTNXlkUSs0NHhSRVljQzRmclNXSTJFQ0ZmS2tDZTRXKzBncG1yQXc3THJ0b2NLanBtcFxuRkRpK3VZYnpBb1gxODZGYmdwc2VmOFF3Yk9hSUowWEUvT2ZVRGxjNGd1MEwrVTRVR1JXTWtXcUl5N0VmQm5oWVxueDdsRlo4b2dac3ZJZDhYSWhyMGo2a2M9XG4tLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tXG4iLAogICJjbGllbnRfZW1haWwiOiAiZmlyZWJhc2UtYWRtaW5zZGstdjZueDdAYXNhbmktbW9iaWxlLmlhbS5nc2VydmljZWFjY291bnQuY29tIiwKICAiY2xpZW50X2lkIjogIjEwNzg1MTY2MDc2ODcwOTc2NDMyMyIsCiAgImF1dGhfdXJpIjogImh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwKICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAiYXV0aF9wcm92aWRlcl94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0cyIsCiAgImNsaWVudF94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkvZmlyZWJhc2UtYWRtaW5zZGstdjZueDclNDBhc2FuaS1tb2JpbGUuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICJ1bml2ZXJzZV9kb21haW4iOiAiZ29vZ2xlYXBpcy5jb20iCn0K', 'base64').toString());

    if (!newAdmin.apps.length) {
        newAdmin.initializeApp({
            credential: newAdmin.credential.cert(newServiceAccount),
            name: 'asani-mobile'
        },
        'asani-mobile'
    );
    }

    const getFirestoreInstance = () => {
        if (!newAdmin.apps.find(app => app.name === 'asani-mobile')) {
            throw new Error('Firebase app named "asani-mobile" does not exist');
        }
        return newAdmin.firestore(newAdmin.app('asani-mobile'));
    };

    const db2 = getFirestoreInstance();

    const base64Key = 'ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAiYXNhbmktbW9iaWxlIiwKICAicHJpdmF0ZV9rZXlfaWQiOiAiMjUxMTU5NTEzNTIyMDVlNTIzMzhhYzExNzk0ZmQzMWY2NDg5NGFjNCIsCiAgInByaXZhdGVfa2V5IjogIi0tLS0tQkVHSU4gUFJJVkFURSBLRVktLS0tLVxuTUlJRXZRSUJBREFOQmdrcWhraUc5dzBCQVFFRkFBU0NCS2N3Z2dTakFnRUFBb0lCQVFDV3dxV3B6TDczSzNXOVxubTZYOEd4MmpUdVJQQXl1cWhUYVljUnpsSFYyaXV3aWtITmtVYXFWTzFNTCtjN2dYZE5KQTdwR2tjSWM1alpPOFxuZG80Y0tCYmwrMXgxOHZJTmM1MEdhcjBXM25YVEJUKzV5ZldMdWU1NXNXLzl4NDRsdE1GVXVySWQyQUp6RnVNOVxueTRGVUtoT2VJZ2FnT0tVSFovUlAwbUw2OXRDVGNKQWdSNUprSjZPUTg5UTUzM29JTWk2cFVRSzAyLzU0RFU0S1xuQTRhRHFsSktzSjRaL3FQd1hOMEN0c2ZDT0daYnY4VkNqU2JNMEJ6UG1sUWJ6QUZXNVZkWEo1WW1wclZGbngzY1xuUWlpV0xkL09oSW9hY0todnY5ak4yd2QxL2RXdDQ4bm9BM2dQNFNCNm05NGV4cUR1bmJtaU5UNmtCQUx2Q3FFNFxuVXRsdHZQa2ZBZ01CQUFFQ2dnRUFIVFBGOEI2VFRRQlRsYTh0RnlqUGRGOG5OVERaaC9XMnpnOTI5K0JZejFrb1xuUXRXOXJNazlqUWdFWDFKZDhkazBrdFJEVE5WcE5CbzY2RElVczlxR0dQVTRBemJVY1F1WWVuSTVmMUVIQktHTFxuSnZzNlAxNzhGUUZzUG4ybGlLTTdJYVRxTklLdmNzaVhxdFFkRUliMzl0VURjSXlZVnRkcGlQNnBJcENsZFRGM1xuMzR2aGJhQVJSTmcwTm9lbVhMNHAyWDdrcm9mZk1uWnY5TUNhUWFENmtjRkJURE5Rd1hkWENaUzZKRzB0QllhZ1xudllYMlVUZWtidjlHUTE4aDhxcXJjUW45MFNobWxXbnVDRjZjNWdaUFJzRXpTdnlYY2tJM2pENU1PWFpjbWZvcFxuanZxeEhWdll0a21PZ0JWbDZKNlRISkRMYVNNTXhmNUd4eURnU2V6SDZRS0JnUURPRk1ibkJwSkxpZGdnbldQTFxubGhpZXVzd2FkQnkwSVdoNEJ3aWFPNkt2L1RwSlB1dTBwMzB6QzQwQTIzb0tPTmpQS1RRWFNOdWk1Z09zdHJrN1xuTDRrVWRCRGs4aWp0WjdCZ1o2S0RsemdFeERHRHVkeDY5bXZKbXE3VmRTYU9Yc2pHR2RQVE5uUnRLMnBNaHFpSVxuVklmT3RvNzdQVldIVzJ4a1pRdjl1T1FROXdLQmdRQzdSMlFpZkwyQnl6UjhGc1lZMEljU3I2MGxOUExyMzRlSlxuUHNCcnJUL1g0N3Y4Q05sWFJCek96UGhhNnhLcWNxQlZlTm5hK0FDVlByUzAxU2pFMk5TdWYvYUZHVGpqV04yVVxuWnJTN284dDRZc3RCUWtMZy95QzlnZUp0anhkM1lPQncvaFg5bVl5RmN0eTcxdS8zVHArOUxrS25HcStkSkhZQVxuSXhKR3lHLzNHUUtCZ1FDdGFiWm1PWlZwa3prWFdObmRPeFRFblJPYlB4SFlValNDckFpRklLR3B6a0Q1MmNTbVxuWkRwcWRkSFZZdHF6TjFyYUdDWUpZZm5RZmhXaGhMRWlLTUlGUUJYblVnODJsd1pJV1d6YnBxZ3crcGRmN1VxbVxuL2kxOW5IaDZqdlkrMzJ1N3A0Z2tON0tKR3Z0OEllUTN0RW9EbklOOHp3UGx2dnpiRGx5a3lLekJ5UUtCZ0d5UlxucVpwWXFHQWJWcFR0ZXZBZHk0Qm91YjdOSUZyZm5pcFJaNm5FcVROV0FiL052WG5hc2J5dGxQallPRno1MEx0Y1xuTVNmQkNFMTlLYk4vczMvU21CR0ZlM3VUc0tnVDkzaTF4ZWJWd1BwTWc3cVVXRU1waEdoNGFMVE05SFN2ZzgzZFxuYnhBeXVRVVFIcWtDcE9EQkF0ZmdmcUZ6VmdXS2dxSDNzdnUzN1RqWkFvR0FGdGNNTEY1V0t3clNEMGdPL2tQT1xuaSsveWFHZy83ZHpmV3JTNXlkUSs0NHhSRVljQzRmclNXSTJFQ0ZmS2tDZTRXKzBncG1yQXc3THJ0b2NLanBtcFxuRkRpK3VZYnpBb1gxODZGYmdwc2VmOFF3Yk9hSUowWEUvT2ZVRGxjNGd1MEwrVTRVR1JXTWtXcUl5N0VmQm5oWVxueDdsRlo4b2dac3ZJZDhYSWhyMGo2a2M9XG4tLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tXG4iLAogICJjbGllbnRfZW1haWwiOiAiZmlyZWJhc2UtYWRtaW5zZGstdjZueDdAYXNhbmktbW9iaWxlLmlhbS5nc2VydmljZWFjY291bnQuY29tIiwKICAiY2xpZW50X2lkIjogIjEwNzg1MTY2MDc2ODcwOTc2NDMyMyIsCiAgImF1dGhfdXJpIjogImh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwKICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAiYXV0aF9wcm92aWRlcl94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0cyIsCiAgImNsaWVudF94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkvZmlyZWJhc2UtYWRtaW5zZGstdjZueDclNDBhc2FuaS1tb2JpbGUuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICJ1bml2ZXJzZV9kb21haW4iOiAiZ29vZ2xlYXBpcy5jb20iCn0K'; // Replace this with your actual base64-encoded key

    // Decode the base64 string to get the JSON key
    const jsonKey = Buffer.from(base64Key, 'base64').toString('utf8');
    const keyObject = JSON.parse(jsonKey);

    // Firestore and Google Drive configurations
    const firestore = new Firestore({
        credentials: keyObject,
        projectId: keyObject.project_id, // Ensure this matches the Project ID in your key
    });

    const drive = google.drive('v3');

app.timer('D542_Data_Bridge', {
    schedule: '*/40 * * * * *',
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

        if (
            (currentHour === 9 && currentMinute === 59 && currentSec >= 31) || 
            (currentHour === 10 && currentMinute === 0 && currentSec <= 29)
        ){
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
                                hLineWidth: function(i, node) {
                                    return (i === 0 || i === node.table.body.length) ? 1 : 0.5;
                                },
                                vLineWidth: function(i, node) {
                                    return 0.5; // Defines the width of vertical lines
                                },
                                hLineColor: function(i, node) {
                                    return '#aaaaaa'; // Horizontal line color
                                },
                                vLineColor: function(i, node) {
                                    return '#aaaaaa'; // Vertical line color
                                },
                                paddingLeft: function(i, node) { return 10; },
                                paddingRight: function(i, node) { return 4; },
                                paddingTop: function(i, node) { return 2; },
                                paddingBottom: function(i, node) { return 2; },
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
                
                            const credentials = JSON.parse("{\"type\":\"service_account\",\"project_id\":\"asani-23d6e\",\"private_key_id\":\"9efbe955c867dfb887c48333caf48c66d38980f8\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQClhbtJMrApWZOC\\nnUpfuB/BtljRdvRYuKt3mFMfX/vfnlqciUOB0EDqP5poeM9G/+wg1xgBXUe2GvFw\\nRcZK0Vopvxfr3PC9nAiF1lStXfUTzI1BePMm90fXtk7242iq8KP0vqwT4CIiOYXn\\nkqIgivwB4+S8uw5jQeUB8Y56fpT6AVHuyOi8XfR+pohvitSo+mTfPVAGK+SRWN4P\\n+DAq4zGwWytjGJopEGvufZFXJfM1tID3KXKyVhR5jwQ9+tOe3uQXjuuSvaaFcg6C\\nDlWyPVdi2cCr8DoDqhSCSDi/gykSWbtR/l3u4sbgkXjV5JoFpO/xk4mL7bGV4IGt\\nNF7sWsSdAgMBAAECgf8G5YuS1onV2eWg4OzjEB5Cnqu/Y+Af0jyoFFtiSJ2LXmMS\\nzVACw1VGbWABpQb97laTB7ijAJVRq82439oN7qsoZ3iXsJ0B9CkRaodyByLZsFbd\\nDi0Tco+LwreHkfIf1+b89ko1NHbiS4NIzX3z3Sv4PYafq9ZC9yaTmNJW8u9CJhAb\\nrv3Eh8ar4c049VMieRaEDA5qFZ5Rj3wK/i0SAQ3u2tKzRSEb+tjzyslMsN7rt9Fx\\nbiHzBDoOxJM4DIRakuMq4lg+FsuAoePi+Sr5Dx5La7YFzD0/LpRKdJ7ReG/6cofb\\ngmI73SPsnbdPk0ZIT8dlmrCsJ8fVEBwAI4oxEAECgYEA0bfzv/LGdf0LfEi/V8mQ\\nUuliGBBhKz0T6sqo184jO8Ip5MlD73YxIv6E3gRnRl93K+fg428FN6HK1AwpsYW9\\nwpHfH1FdkAfyYFR6Q8m8vFh3cC2/NM8OO1d6RoqQTOKzpNdulLxjSrbXRZuSR08+\\nABxN+FAUewGRmPu2JRZdE30CgYEAygzqxfUScanPsaSOWqREujqdfOJp76f5rMna\\nFths93BfIUeYwZhAUpCe+R9KligbmTFSvie8ow/lAILXeG7zV7srgTb3MEs+Qbja\\nwzMOao1yLdn6vCKABdH4E6LEuWpp3ymZ/1yOgT+S1PtURiBVdtQTepeOVuAQTyuJ\\nQeeI/6ECgYEAp6x5mChVAJTWkAHh6iBf4cpzAWZnKhjlSb3KjPBlPywYLrG0PUq0\\nMpRoStIeeCdvsozsQyrKcxZKgotO8n5Jn7zdNb7qHXQdF2OzdWtgGP5qUChjTaeW\\na1+fhbLXeIFwvAT6hrSwdlYFe8PMinMS0SfQNw5fsZEphbUKhlCBDkkCgYEAkO6H\\nEYxjuIiYZNKnEjm22ubkxQob9z7Eh78a4zxHnY5LjrGuz1+I9DCs+AIMHH2UnmSU\\n97XFSCpEmANC0C61+v5VjJCC629trvMMaOycsK3Zcy5i/sS4lHQywNMGzgGZA+zx\\nfA1GY0vY5VGK9+qFo4Eon81K2uJKkJ+oC1AsI0ECgYBPMiFSWiSoeqNn581Mx1vj\\nBu4WEODmswttz3Swra94IjGevIZqZ+YXDmniJY9535nBrfu30Effv096oe3QC+v/\\nonUhXoRIfUQPf8Czd/GeJR6XMR4YFfLjVSyF/rv6kcLMbUDfx65KEqYTdNKhw1o5\\nsXEBxbu1ldyRqBF/Q0dBxQ==\\n-----END PRIVATE KEY-----\\n\",\"client_email\":\"firebase-adminsdk-osccb@asani-23d6e.iam.gserviceaccount.com\",\"client_id\":\"108725706610666051231\",\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\",\"token_uri\":\"https://oauth2.googleapis.com/token\",\"auth_provider_x509_cert_url\":\"https://www.googleapis.com/oauth2/v1/certs\",\"client_x509_cert_url\":\"https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-osccb%40asani-23d6e.iam.gserviceaccount.com\",\"universe_domain\":\"googleapis.com\"}");
                
                            const auth = new google.auth.GoogleAuth({
                                credentials: credentials,
                                scopes: ['https://www.googleapis.com/auth/drive.file'],
                            });
                
                            console.log("PharmevoPDFGenerator: Authenticated successfully");
                
                            const driveService = google.drive({ version: 'v3', auth });
                
                            const pktFormattedDate = pkNow.toISOString().replace(/T/, '_').replace(/\..+/, '') + '_PKT';
                
                            const fileMetadata = {
                                name: `Report_${pktFormattedDate}.pdf`,
                                parents: ['1yUKqYsai_xBWcKdOvtoazBEwcYOfGNvg'], // Ensure this is the correct folder ID
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
                        } catch (uploadError) {
                            console.error('PharmevoPDFGenerator: Error during PDF upload', uploadError);
                        }
                    });
                
                    console.log("PharmevoPDFGenerator: PDF generation crossed successfully");
                } catch (error) {
                    console.error('PharmevoPDFGenerator: Error occurred while generating the PDF', error);
                }

            } catch (error) {
                context.log('PharmevoPDFGenerator: Error occurred', error);
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
            
                        if(minutes !== 0 || minutes !== 15 || minutes !== 30 || minutes !== 45){
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
                } catch (e) {
                    console.log('Exception:', e);
                }
            
                return body;    
            }
        }
        else{
            console.log('PharmevoPDFGenerator: NOT THE RIGHT TIME! : ', currentHour, currentMinute, currentSec);
        }

        if (
            (currentMinute%5 === 4 && currentSec >=50) || 
            (currentMinute%5 === 0 && currentSec <= 10) 
        ){

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
                messages.forEach(({ topic, message }) => {
                    try {
                        const data = JSON.parse(message.toString());

                        if (data.TDS210C && data.TDS210C.Conductivity && data.TDS210C.Temperature) {
                            const fieldId = 'sharedKey'; // Use a shared key to store all topics together.

                            // Retrieve the existing entry if it exists, otherwise create a new one
                            let entry = tempCondDataMap.get(fieldId) || {
                                time: newAdmin.firestore.Timestamp.now(),
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
                    data: newAdmin.firestore.FieldValue.arrayUnion(...tempCondDataArray),
                };

                await db2.collection('PdlRtsNxcOQGzNwZHYIqnfk3fQL2').doc(`tempconductivity`).set(nestedData, { merge: true });
                console.log(`D542_Data_Bridge: Updated temperature and conductivity data`);
            }
        } else {
            console.log('D542_Data_Bridge: NOT THE RIGHT TIME! : ', currentHour, currentMinute, currentSec);
        }
    }
});
