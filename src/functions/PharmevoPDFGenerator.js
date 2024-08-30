// const { app } = require('@azure/functions');
// const mqtt = require('mqtt');
// const admin = require('firebase-admin');
// const { Firestore, Timestamp } = require('@google-cloud/firestore');
// const { PassThrough } = require('stream');
// const { google } = require('googleapis');
// const fs = require('fs');
// const path = require('path');
// const pdfMake = require('pdfmake/build/pdfmake');
// const pdfFonts = require('pdfmake/build/vfs_fonts');
// const { TableClient, AzureNamedKeyCredential } = require('@azure/data-tables');

// pdfMake.vfs = pdfFonts.pdfMake.vfs;

// const base64Key = process.env.FIRESTORE_KEY; 
// const serviceAccount = JSON.parse(Buffer.from(base64Key, 'base64').toString());

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
//             true ||
//             (currentHour === 9 && currentMinute === 59 && currentSec >=55) || 
//             (currentHour === 10 && currentMinute === 0 && currentSec < 30) 
//             // || 
//             // (currentMinute === 29 && currentSec >=50) || 
//             // (currentMinute == 30 && currentSec <= 30)
//         ){
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
//                 const docRef = firestore.collection('PdlRtsNxcOQGzNwZHYIqnfk3fQL2').doc('tempconductivity');
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
//                             // console.log('A nearby entry', nearbyEntries[0]);
//                         }
//                     } else {
//                         entries.push(matchingEntries[0]); // Choose the first entry for the interval
//                         // console.log('A matching entry', matchingEntries[0]);
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
                
//                 console.log("PharmevoPDFGenerator: Generating PDF Starting...");

//                 try {
//                     // Generate the PDF and stream it directly to Google Drive
//                     const pdfDoc = pdfMake.createPdf(docDefinition);
                                
//                     // Convert the PDF document to a buffer and then create a readable stream from it
//                     pdfDoc.getBuffer(async (buffer) => {
//                         try {
//                             const stream = new PassThrough();
//                             stream.end(buffer); // Write the buffer to the stream
                
//                             stream.on('end', () => {
//                                 console.log("PharmevoPDFGenerator: Stream ended successfully");
//                             });
                
//                             stream.on('error', (err) => {
//                                 console.error("PharmevoPDFGenerator: Stream error", err);
//                             });
                
//                             console.log("PharmevoPDFGenerator: Stream created successfully");
                
//                             const credentials = JSON.parse(
//                                 // serviceAccountKey,
//                             );
                
//                             const auth = new google.auth.GoogleAuth({
//                                 credentials: credentials,
//                                 scopes: ['https://www.googleapis.com/auth/drive.file'],
//                             });
                
//                             console.log("PharmevoPDFGenerator: Authenticated successfully");
                
//                             const driveService = google.drive({ version: 'v3', auth });
                
//                             const pktFormattedDate = pkNow.toISOString().replace(/T/, '_').replace(/\..+/, '') + '_PKT';
                
//                             const fileMetadata = {
//                                 name: `Report_${pktFormattedDate}.pdf`,
//                                 parents: ['1yUKqYsai_xBWcKdOvtoazBEwcYOfGNvg'], // Ensure this is the correct folder ID
//                             };
                
//                             console.log("PharmevoPDFGenerator: Uploading PDF to Google Drive");
                
//                             const media = {
//                                 mimeType: 'application/pdf',
//                                 body: stream, // Streaming the PDF directly
//                             };
                
//                             const response = await driveService.files.create({
//                                 resource: fileMetadata,
//                                 media: media,
//                                 fields: 'id',
//                             });
                
//                             console.log('PharmevoPDFGenerator: PDF uploaded successfully, File ID:', response.data.id);
//                         } catch (uploadError) {
//                             console.error('PharmevoPDFGenerator: Error during PDF upload', uploadError);
//                         }
//                     });
                
//                     console.log("PharmevoPDFGenerator: PDF generation crossed successfully");
//                 } catch (error) {
//                     console.error('PharmevoPDFGenerator: Error occurred while generating the PDF', error);
//                 }

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
//                         // console.log(row);
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
