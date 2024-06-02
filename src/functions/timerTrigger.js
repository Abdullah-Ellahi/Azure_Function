const { app } = require('@azure/functions');
const mqttFunction = require('./mqttFunction.js');

app.timer('timerTrigger', {
    schedule: '0 0 0,12 * * *',
    handler: async (myTimer, context) => {
        context.log('Timer function processed request.');
        const currentHour = new Date().getUTCHours();
        if (currentHour === 0 || currentHour === 12) {
            await mqttFunction(context, myTimer);
        }
    }
}
);
// const currentTime = new Date();
// const currentHour = currentTime.getUTCHours();
// const currentMinute = currentTime.getUTCMinutes();
// const currentSecond = currentTime.getUTCSeconds();

// if ((currentHour === 0 || currentHour === 12) &&
//     currentMinute === 59 &&
//     currentSecond >= 55 && currentSecond <= 60) {
//     await mqttFunction(context, myTimer);
// }        