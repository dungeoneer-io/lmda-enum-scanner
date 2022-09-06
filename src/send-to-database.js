const { getDb } = require('@dungeoneer-io/nodejs-utils');
const {
    BEE_TYPES,
    COLLECTIONS,
    DATABASES
} = require('./entity-enums');

const insertBlizzardEntityEventArray = async (idArray, type, event = 'ADDED') => {
    console.log(`transmitting ${idArray.length} identified entity events..`);
    const eventColl = await getDb()
        .db(DATABASES.DEFAULT)
        .collection(COLLECTIONS.BLIZZARDENTITYEVENTS);

    await eventColl.insertMany(
        idArray.map((o) => ({
            stamp: Date.now(),
            entity: {
                id: o,
                type
            },
            event
        }))
    );
};

const upsertClassEntitiesFromSnapshot = async (snapshot) => {
    const classCollection = await getDb()
        .db(DATABASES.DEFAULT)
        .collection(COLLECTIONS.CLASSES);
    const batch = classCollection.initializeUnorderedBulkOp();

    snapshot.classes.forEach(({ id, name }) => {
    batch.find({ _id: `${id}` })
        .upsert()
        .updateOne({
            $setOnInsert: {
                first: Date.now(),
                name
            },
            $set: {
                last: Date.now(),
            }
        });
    });
    const results = await batch.execute();

    if (results.result && results.result.nUpserted > 0) {
        const upsertedIds = results.result.upserted.map(({ _id }) => _id);
        await insertBlizzardEntityEventArray(upsertedIds, BEE_TYPES.CLASS);
    }
};

const upsertRaceEntitiesFromSnapshot = async (snapshot) => {
    const raceCollection = await getDb()
        .db(DATABASES.DEFAULT)
        .collection(COLLECTIONS.RACES);
    const batch = raceCollection.initializeUnorderedBulkOp();

    snapshot.races.forEach(({ id, ...rest }) => {
    batch.find({ _id: `${id}` })
        .upsert()
        .updateOne({
            $setOnInsert: {
                first: Date.now(),
                ...rest
            },
            $set: {
                last: Date.now(),
            }
        });
    });
    const results = await batch.execute();

    if (results.result && results.result.nUpserted > 0) {
        const upsertedIds = results.result.upserted.map(({ _id }) => _id);
        await insertBlizzardEntityEventArray(upsertedIds, BEE_TYPES.RACE);
    }
};

const upsertSpecEntitiesFromSnapshot = async (snapshot) => {
    const specCollection = await getDb()
        .db(DATABASES.DEFAULT)
        .collection(COLLECTIONS.SPECIALIZATIONS);
    const batch = specCollection.initializeUnorderedBulkOp();

    snapshot.specs.forEach(({ id, ...rest }) => {
    batch.find({ _id: `${id}` })
        .upsert()
        .updateOne({
            $setOnInsert: {
                first: Date.now(),
                ...rest
            },
            $set: {
                last: Date.now(),
            }
        });
    });
    const results = await batch.execute();

    if (results.result && results.result.nUpserted > 0) {
        const upsertedIds = results.result.upserted.map(({ _id }) => _id);
        await insertBlizzardEntityEventArray(upsertedIds, BEE_TYPES.SPECIALIZATION);
    }
};

const sendToDatabase = async (snapshot) => {
    console.log('transmitting unique classes...');
    await upsertClassEntitiesFromSnapshot(snapshot);
    
    console.log('transmitting unique races...');
    await upsertRaceEntitiesFromSnapshot(snapshot);

    console.log('transmitting unique specs...');
    await upsertSpecEntitiesFromSnapshot(snapshot);
};

module.exports = sendToDatabase;

