const {
    connectToBlizzard,
    initDb,
    lambdaTry200Catch500
} = require('@dungeoneer-io/nodejs-utils');
const getSnapshot = require('./get-snapshot');
const sendToDatabase = require('./send-to-database');

const harvestAndUpsertEnumData = async (lambdaEvent) => {
    await initDb();
    await connectToBlizzard();
    
    const snapshot = await getSnapshot(lambdaEvent);
    await sendToDatabase(snapshot);

    return snapshot;
};


exports.handler = async (event = {}, context) => {
    console.log('Dungeoneer.io');
    console.log('lmda-enum-scanner');
    console.log('================');

    await lambdaTry200Catch500({
        context,
        event,
        notifyOn200: true,
        fn200: harvestAndUpsertEnumData,
        fn500: (e) => console.log('error', e)
    });
};
