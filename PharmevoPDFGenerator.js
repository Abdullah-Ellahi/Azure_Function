// const { app } = require('@azure/functions');
// const { Firestore, Timestamp } = require('@google-cloud/firestore');
// const { google } = require('googleapis');
// const fs = require('fs');
// const path = require('path');
// const pdfMake = require('pdfmake/build/pdfmake');
// const pdfFonts = require('pdfmake/build/vfs_fonts');

// pdfMake.vfs = pdfFonts.pdfMake.vfs;

// const base64Key = 'ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAiYXNhbmktZDU0MiIsCiAgInByaXZhdGVfa2V5X2lkIjogImVkNDA1OWFkOTkwNDY4MmU1NjM2ZDI2ZGU5NTJkNzRjZDkyNDI4M2QiLAogICJwcml2YXRlX2tleSI6ICItLS0tLUJFR0lOIFBSSVZBVEUgS0VZLS0tLS1cbk1JSUV2UUlCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktjd2dnU2pBZ0VBQW9JQkFRQ1E0M25sV1pUcHRjdDhcbnZYMDVHdXdWRWJWK1BwUldHYTdWbyswSU5yWnAzRnJNaU5BRWRFbVAxN2tTdUlZRmswYlN4cVV6OCtXaDlWaVVcbitjRk9KY1FjTWg4YW8wRmtpSjhWWHNhVlJQZGQvdGlmK3FySXFSZWhKaEVNdVlMRUJKRko1SUVuNWlRcFgzbFZcbitUdXNMak8xbWdUdndxOVNna3U5bjQwd0xuYkY5Rmw5bTZ6dkg2Tm4yU0pMQXZFK2Z4d1ZXeHhsaHFtQy9RQ2ZcbnB5QnhOZllVOE1sa2c5U0lWSXBNM2tucE1VV1kvSXpSSFV4cmtqNDNTRzVmUW1vSzNjWUtEV3Znd0I5cWE0MXhcbnI4Z0x4YnRDektQSGxxK01Xa1VSbmpVb2xwZ21HZVdtanNIUUxJVU9aM1hIdHdXUzNOZno3aTB6RjZGZ0JGcWhcbkpGdjl4NTMxQWdNQkFBRUNnZ0VBQWo1enFkaXNWTDVMQzRDSHl2V2V5R2pBemRhOFIyVVlieGlkMkMyNHY0VDVcbkp3ZkxhUFpBR1drMjVaSlVDUXdSNGJ2UDNURWUwbFpSYStVK1dDNS9qZ3c4clJYd01sVm5XVlpoYnZobGVHQTFcbkRLMCtMZU5vaVVka3BqVE5VS25kY2k2R3pZNWo2dndlamZWUk5rWjA2SkFkR1hXRGI1eXkvWm5WL3ZRSlJPR1FcbjlkYUY0Tk12ei8vd09VWXZ2T3hRcmNDdjZyRHJ2aWZRd1h6aHNtYUxPWC96V2lDeWMwMDIrNkp4dXVjNlhTSFBcbmdCaXFtUXpPWjNJOU9yaHdWZkg4aWRUMkkwOUhmODc4RnVYc3c1SlRRd1RieENuTGlGQUwvbkRBWFJYS05lVFBcbkRsajZlc29nK1lpZ0pUcGYxaHB2NTFkUzl0cFQ5Tzd5VFhUNUI0VS9BUUtCZ1FERDUxV1FOUnpSYkNyOGV2SHlcbjFOYnRHaHpKajVobGxtcXNTclRQY2JKT0lKOW5RRExrTDBVUDZsL3ZiejM1ZWZIR2luUC9scThSYlRKK1dzUTNcbkNQOHJuTVNWdDdRbXZZckF3MXRlNTRaUzgvT1RQdGNsaVNQYk95MVd2ZG94Q2g3Zk00Smk0NFg3R2VOdFdiOUZcbkNJSFdzejhXK0lKRjM4ZWUwQ0JMU3FIVnRRS0JnUUM5VmRZekZnM0J5MUZHakIzYWtmK05Db2xRdVBkODUrUVJcbjdCMm1yajFoMDd6ZGRNTExoa3F0WHRyVE0xY2JtdFluWnllODR5bHR6REdDMFdCc2FuVk9qakxtdlA0bHpNbWZcbkR5MW0vNmRjM3hYbktrY09aakFIMnNyMXFYL0wwdFUwa00wYnBCeGlNS1RtNi9IdlZSR2JxazRvbUpZcGZ3QWtcbllubTZVVGZQUVFLQmdRQ1N1eSswUG9QaVBCR0tZSS9lSVJzR0psdlBnTFZWeWE4R2MwSXdhbWx4NlM5Yng1TVdcbnBHMG0rWlozb1N1Yk00SXJBa0xWK2tnUVZtak9pUytVUlRIU3VhM29zZHJHZFl0NnpCZ2c0bXNTc05RTXdyR2tcbjFLNDlvM3BYRVFaZldzUTZ5N3JxVko1aXNFWk8vanlyK1M0KzRLenRpSjhTdFFVd1o1VDRyK0s5VFFLQmdCRlNcbmhTSnZaaHJRNjBLMUZ4WWtQV2srQ1lvbEloOS9aOUpyODlxSUhuMlF6YnJGTC8xRzhCWWtrNGkzTGNVTXlZTVpcbllQMWJHek04ZDFzc3BOSEdlNjBRUFgvMjBwbkt2cmVhditDb2l6dG4vYTBFcFRPZ0RjenFLNStHUG5iN2R4NDBcbkVVL3lGOWF6OUtKYisrcVZzZUs0RmlhSXIxUmtsaks5LzcwWUtTOEJBb0dBVWluK0xIUmIvem55K0NWdjM4WVJcbnQzd01INnVBWW1zYnlVYzVhK1RZM3poYzg0anZLL1ZVZFFwcEh4TmlCelo0TzdoNy9IRXpxTlFmSjd4S2NJNUNcbjVNNHpDb0J6aU93ZWhnNHM2S2Z0d05RL1NCMGpLUXVndEtvblR0OWZIYXJqdjkrMjB5R3ZLbG9Ma2RONW5IekJcbjB5WExFYzhwUUw5UktYOElCVWR6MnJvPVxuLS0tLS1FTkQgUFJJVkFURSBLRVktLS0tLVxuIiwKICAiY2xpZW50X2VtYWlsIjogImZpcmViYXNlLWFkbWluc2RrLTVzMDZ5QGFzYW5pLWQ1NDIuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICJjbGllbnRfaWQiOiAiMTAzMjU4NzI2MzE4MzQ3MjAzNzIxIiwKICAiYXV0aF91cmkiOiAiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tL28vb2F1dGgyL2F1dGgiLAogICJhdXRoX3Byb3ZpZGVyX3hhNjYiOiAiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tL29hdXRoMi92MS9wcm9maWxlcy9hZG1pbnNkay01czA2ei1zZXJ2aWNlLXNlcnZpY2UtY2VydCIsCiAgImF1dGhfY2VydF92YWx1ZCI6ICJhbGxhbmNvIiwiY2xpZW50X2lkIjogIjEwMzI1ODcyNjMxODM0NzIwMzcwMSIsCiAgInZpZGVvX2VtYWlsIjogImZpcmViYXNlLWFkbWluc2RrLTVzMDZ5QGFzYW5pLWQ1NDIuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICJjZXJ0X2lkX2NoYWxsZW5nZXMiOiB7CiAgICAiY2VydF9uYW1lIjogIkFhcmFkdW9nIE5ldHdvcmsgU2FjZXMiLAogICAgImNsaWVudF9uYW1lIjogIkFhcmFkdW9nIEluYyIsCiAgICAiY2VydF9vdmVydmlldyI6ICJwYXNzd29yZCIsCiAgICAiY2VydF9ub3RlX3NvcnQiOiB7CiAgICAgICJwcm9qZWN0X2lkIjogIkFhcmFkdW9nIiwKICAgICAgImF1dGhfY2VydF9pZCI6ICJjZXJ0X2lkX2NoYWxsZW5nZXMiLAogICAgICAib3V0cmVhX2lkIjogIkFhcmFkdW9nX2NsaWVudCIsCiAgICAgICJpbmNsdWRlbnQiOiB7CiAgICAgICAgImlzc3VpbmciOiAiYXV0aCJ9fQp9fQ=='; // Replace this with your actual base64-encoded key

// // Decode the base64 string to get the JSON key
// const jsonKey = Buffer.from(base64Key, 'base64').toString('utf8');
// const keyObject = JSON.parse(jsonKey);

// // Firestore and Google Drive configurations
// const firestore = new Firestore({
//     credentials: keyObject,
//     projectId: keyObject.project_id, // Ensure this matches the Project ID in your key
// });

// const drive = google.drive('v3');

// app.timer('PharmevoPDFGenerator', {
//     schedule: '*/30 * * * * *', // Run every 30 seconds
//     handler: async (myTimer, context) => {
//         context.log('PharmevoPDFGenerator: Timer function processed request.');


//         // // Set the date and time for the test purposes
//         // const date = new Date(Date.UTC(2024, 7, 16, 5, 0, 0)); // 6 PM UTC
//         // // Extract the current hour, minute, and second
//         // const currentHour = date.getHours(); // 23 (11 PM in Pakistan)
//         // const currentMinute = date.getMinutes(); // 0
//         // const currentSec = date.getSeconds(); // 0

//         const currentHour = new Date().getHours();
//         const currentMinute = new Date().getMinutes();
//         const currentSec = new Date().getSeconds();

//         if (
//             (currentHour === 9 && currentMinute === 59 && currentSec >=55) || 
//             (currentHour === 10 && currentMinute === 0 && currentSec < 30) 
//             // || 
//             // (currentMinute === 29 && currentSec >=50) || 
//             // (currentMinute == 30 && currentSec <= 30)
//         ){

//             let pages = 1;

//             try {
//                 // Calculate the timestamp for the past 24 hours in PKT
//                 const utcNow = new Date();
//                 const pkNow = new Date(utcNow.getTime() + 5 * 60 * 60 * 1000); // PKT is UTC+5

//                 // const pkNow = new Date(Date.UTC(2024, 7, 13, 5, 0, 0) + 5 * 60 * 60 * 1000); // Month is zero-indexed (7 = August)

//                 // // Calculate utcNow by subtracting 5 hours from pkNow (PKT is UTC+5)
//                 // const utcNow = new Date(pkNow.getTime() - 5 * 60 * 60 * 1000);

//                 const past24Hours = new Date(utcNow.getTime() - 24 * 60 * 60 * 1000);

//                 console.log('utcNow', utcNow);
//                 console.log('pkNow', pkNow);
//                 console.log('past24Hours', past24Hours);

//                 let currentTime = new Date(past24Hours.getTime());// current time a day ago

//                 // Get the date components for the current date for the pdf header
//                 let currentYear = pkNow.getUTCFullYear();
//                 let currentMonth = (pkNow.getUTCMonth() + 1).toString().padStart(2, '0'); // Months are zero-indexed
//                 let currentDay = pkNow.getUTCDate().toString().padStart(2, '0');

//                 // Get the date components for the previous date
//                 let previousYear = currentTime.getUTCFullYear();
//                 let previousMonth = (currentTime.getUTCMonth() + 1).toString().padStart(2, '0'); // Months are zero-indexed
//                 let previousDay = currentTime.getUTCDate().toString().padStart(2, '0');

//                 // Construct the date strings
//                 let currentDateString = `${currentYear}/${currentMonth}/${currentDay}`;
//                 let previousDateString = `${previousYear}/${previousMonth}/${previousDay}`;

//                 // Construct the final date string in the format "todaysdate - previousdate"
//                 let dateString = `${currentDateString} - ${previousDateString}`;

//                 // Fetch the specific document 'tempconductivity' inside the 'D542' collection
//                 const docRef = firestore.collection('D542').doc('tempconductivity');
//                 const doc = await docRef.get();

//                 if (!doc.exists) {
//                     context.log('PharmevoPDFGenerator: Document "tempconductivity" not found.');
//                     return;
//                 }

//                 // Get the array named 'data' from the document
//                 const arrayField = doc.data()['data'];

//                 if (!Array.isArray(arrayField)) {
//                     context.log('PharmevoPDFGenerator: Field "data" is not an array.');
//                     return;
//                 }

//                 // Initialize variables
//                 const entries = [];
//                 const intervalMinutes = 15;
//                 const intervalMillis = intervalMinutes * 60 * 1000;

//                 // Iterate over each interval
//                 while (currentTime <= pkNow) {
//                     const currentTimeStamp = Timestamp.fromDate(currentTime);
//                     const nextTimeStamp = Timestamp.fromDate(new Date(currentTime.getTime() + intervalMillis));

//                     // Find an entry for the current interval
//                     const matchingEntries = arrayField.filter((item) => {
//                         const itemTime = item.time instanceof Timestamp ? item.time.toDate() : new Date(item.time);
//                         return (
//                             itemTime >= new Date(currentTime.getTime() - 2 * 60 * 1000) && itemTime <= new Date(currentTime.getTime() + 2 * 60 * 1000)
//                         );
//                     });

//                     // If no entry found, try to find nearby entries
//                     if (matchingEntries.length === 0) {
//                         const nearbyEntries = arrayField.filter((item) => {
//                             const itemTime = item.time instanceof Timestamp ? item.time.toDate() : new Date(item.time);
//                             return (
//                                 itemTime >= new Date(currentTime.getTime() - 5 * 60 * 1000) && itemTime <= new Date(currentTime.getTime() + 5 * 60 * 1000)
//                             );
//                         });

//                         if (nearbyEntries.length > 0) {
//                             entries.push(nearbyEntries[0]); // Choose the closest entry
//                             console.log('A nearby entry', nearbyEntries[0]);
//                         }
//                     } else {
//                         entries.push(matchingEntries[0]); // Choose the first entry for the interval
//                         console.log('A matching entry', matchingEntries[0]);
//                     }

//                     // Move to the next interval
//                     currentTime = new Date(currentTime.getTime() + intervalMillis);
//                 }

//                 context.log(`PharmevoPDFGenerator: Completed processing. Total entries found: ${entries.length}.`);

//                 if (entries.length === 0) {
//                     context.log('PharmevoPDFGenerator: No data found in the array "data" for the past 24 hours.');
//                     return;
//                 }

//                 const docDefinition = {
//                     content: [
//                         { 
//                             text: `Report for ${dateString}`, 
//                             style: 'header', 
//                             margin: [0, 0, 0, 20] // Adds 20 units of space after the header
//                         },
//                         {
//                             table: {
//                                 headerRows: 2,
//                                 widths: [60, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30],
//                                 body: buildTableBody(entries),
//                             },
//                             alignment: 'center', // Center-aligns the table within the document
//                             fontSize: 8, // Reduced font size
//                             layout: {
//                                 hLineWidth: function(i, node) {
//                                     return (i === 0 || i === node.table.body.length) ? 1 : 0.5;
//                                 },
//                                 vLineWidth: function(i, node) {
//                                     return 0.5; // Defines the width of vertical lines
//                                 },
//                                 hLineColor: function(i, node) {
//                                     return '#aaaaaa'; // Horizontal line color
//                                 },
//                                 vLineColor: function(i, node) {
//                                     return '#aaaaaa'; // Vertical line color
//                                 },
//                                 paddingLeft: function(i, node) { return 10; },
//                                 paddingRight: function(i, node) { return 4; },
//                                 paddingTop: function(i, node) { return 2; },
//                                 paddingBottom: function(i, node) { return 2; },
//                             },
//                         },
//                     ],
//                     styles: {
//                         header: {
//                             fontSize: 18,
//                             bold: true,
//                             alignment: 'center', // Center-aligns the header
//                         },
//                         tableContent: {
//                             alignment: 'center', // Center-aligns the text in each cell
//                         },
//                     },
//                     defaultStyle: {
//                         alignment: 'center' // Center-aligns all text by default
//                     }
//                 };
                                                                                
//                 // Generate the PDF and save it to a file
//                 const pdfPath = path.join(__dirname, 'output.pdf');
//                 const pdfDoc = pdfMake.createPdf(docDefinition);

//                 pdfDoc.getBuffer(async (buffer) => {
//                     fs.writeFileSync(pdfPath, buffer);

//                     // Upload the PDF to Google Drive
//                     const auth = new google.auth.GoogleAuth({
//                         keyFile: 'service-account-key.json', // Replace with your service account file path
//                         scopes: ['https://www.googleapis.com/auth/drive.file'],
//                     });

//                     const driveService = google.drive({ version: 'v3', auth });

//                     const pktFormattedDate = pkNow.toISOString().replace(/T/, '_').replace(/\..+/, '') + '_PKT';

//                     const fileMetadata = {
//                         name: `Report_${pktFormattedDate}.pdf`,
//                         parents: ['1yUKqYsai_xBWcKdOvtoazBEwcYOfGNvg'], // Replace with your Google Drive folder ID
//                     };

//                     const media = {
//                         mimeType: 'application/pdf',
//                         body: fs.createReadStream(pdfPath),
//                     };

//                     const response = await driveService.files.create({
//                         resource: fileMetadata,
//                         media: media,
//                         fields: 'id',
//                     });

//                     context.log('PharmevoPDFGenerator: PDF uploaded successfully, File ID:', response.data.id);
//                 });
//             } catch (error) {
//                 context.log('PharmevoPDFGenerator: Error occurred', error);
//             }

//             function buildTableBody(entries) {
//                 // Define the header rows
//                 const headerRow1 = [
//                     { text: 'Timestamp', rowSpan: 2, alignment: 'center' },
//                     { text: 'After Mix-Bed Conductivity', colSpan: 2, alignment: 'center' }, {}, 
//                     { text: 'Non-Ceph Loop (Supply)', colSpan: 2, alignment: 'center' }, {}, 
//                     { text: 'Non-Ceph Loop (Return)', colSpan: 2, alignment: 'center' }, {}, 
//                     { text: 'Ceph Loop (Supply)', colSpan: 2, alignment: 'center' }, {}, 
//                     { text: 'Ceph Loop (Return)', colSpan: 2, alignment: 'center' }, {},
//                 ];
            
//                 const headerRow2 = [
//                     { text: 'Timestamp', alignment: 'center' }, 
//                     { text: 'Temp.', alignment: 'center' }, 
//                     { text: 'Cond.', alignment: 'center' }, 
//                     { text: 'Temp.', alignment: 'center' }, 
//                     { text: 'Cond.', alignment: 'center' }, 
//                     { text: 'Temp.', alignment: 'center' }, 
//                     { text: 'Cond.', alignment: 'center' }, 
//                     { text: 'Temp.', alignment: 'center' }, 
//                     { text: 'Cond.', alignment: 'center' }, 
//                     { text: 'Temp.', alignment: 'center' }, 
//                     { text: 'Cond.', alignment: 'center' },
//                 ];
            
//                 // Combine the headers with the data
//                 const body = [];
//                 body.push(headerRow1);
//                 body.push(headerRow2);
            
//                 try {
//                     // Add data rows
//                     entries.forEach((item) => {
//                         let adjusted = false; // Initialize adjusted flag
            
//                         let itemTime = item.time instanceof Timestamp ? item.time.toDate() : new Date(item.time);
//                         itemTime = new Date(itemTime.setHours(itemTime.getHours() + 5)); // Add five hours
            
//                         const minutes = itemTime.getMinutes();
//                         let adjustedMinutes;
            
//                         if(minutes !== 0 || minutes !== 15 || minutes !== 30 || minutes !== 45){
//                             // Adjust minutes to nearest 0, 15, 30, or 45
//                             if (minutes >= 0 && minutes < 10) {
//                                 adjustedMinutes = 0;
//                             } else if (minutes >= 10 && minutes < 25) {
//                                 adjustedMinutes = 15;
//                             } else if (minutes >= 25 && minutes < 40) {
//                                 adjustedMinutes = 30;
//                             } else if (minutes >= 40 && minutes < 55) {
//                                 adjustedMinutes = 45;
//                             } else if (minutes >= 55) {
//                                 adjustedMinutes = 0;
//                                 itemTime.setHours(itemTime.getHours() + 1); // Adjust hour if rolling over
//                             }
                
//                             if (adjustedMinutes !== minutes) {
//                                 adjusted = true;
//                                 itemTime.setMinutes(adjustedMinutes); // Set adjusted minutes
//                             }
//                         }        
//                         // // Log the original time
//                         console.log("Original time:", itemTime);
            
//                         // Get hours, minutes, and seconds
//                         let hours = itemTime.getUTCHours();
//                         let minutesStr = itemTime.getUTCMinutes().toString().padStart(2, '0');
//                         // let seconds = itemTime.getUTCSeconds().toString().padStart(2, '0');
            
//                         // Determine AM or PM
//                         let ampm = hours >= 12 ? 'PM' : 'AM';
            
//                         // Convert 24-hour time to 12-hour time
//                         hours = hours % 12;
//                         hours = hours ? hours : 12; // the hour '0' should be '12'
            
//                         // Construct the date and time string
//                         let timeString = `${hours.toString().padStart(2, '0')}:${minutesStr} ${ampm}`;
            
//                         if (adjusted) {
//                             timeString = timeString + '.';
//                         }
            
//                         let row = [
//                             timeString,
//                             item.t1 || '',
//                             item.c1 || '',
//                             item.t2 || '',
//                             item.c2 || '',
//                             item.t3 || '',
//                             item.c3 || '',
//                             item.t4 || '',
//                             item.c4 || '',
//                             item.t5 || '',
//                             item.c5 || '',
//                         ];
            
//                         body.push(row);
//                         console.log(row);
//                     });
//                 } catch (e) {
//                     console.log('Exception:', e);
//                 }
            
//                 return body;    
//             }
//         }
//         else{
//             console.log('PharmevoPDFGenerator: NOT THE RIGHT TIME!');
//         }
//     }
// });
