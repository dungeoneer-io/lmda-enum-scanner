const { ObjectId } = require('mongodb');
const {
    queueUntilResolved,
    getDb,
    getWowPlayableRace,
    getWowPlayableSpec
} = require('@dungeoneer-io/nodejs-utils');

const {
    mapApiToRaces,
    mapApiToSpecs
} = require('./bapi-mapper/enum-snapshot');
const {
    COLLECTIONS,
    DATABASES
} = require('./entity-enums');

const ENUM_SNAPSHOT_TYPE = 'GameEnums';

const getSnapshot = async (lambdaEvent) => {
    const { snapshotId } = lambdaEvent;
    let snapshot;
    if (!snapshotId) {
        snapshot = await procureLiveSnapshot();
        const snapshotCollection = await getDb()
            .db(DATABASES.DEFAULT)
            .collection(COLLECTIONS.SNAPSHOTS);
        await snapshotCollection.insertOne({
            stamp: Date.now(),
            type: ENUM_SNAPSHOT_TYPE,
            data: snapshot
        });
    } else {
        snapshot = await fetchSnapshotById(snapshotId);
    }

    return snapshot;
};

const fetchSnapshotById = async (snapshotId) => {
    const snapshotCollection = await getDb()
        .db(DATABASES.DEFAULT)
        .collection(COLLECTIONS.SNAPSHOTS);

    debugLog(`retrieving snapshot id ${ snapshotId }`);
    const snapshot = await snapshotCollection.findOne({ _id: new ObjectId(snapshotId) });

    return snapshot.data;
};

const procureLiveSnapshot = async () => {
    // GET RACE INDEX
    const raceIndex = await getWowPlayableRace();
    const racesToProcess = raceIndex.races.map(({ id }) => ({ id }));

    let raceResults = await queueUntilResolved(
        getWowPlayableRace,
        racesToProcess,
        15,
        3,
        { showBar: true, debug: true }
    )
    .catch(o => console.log('uncaught exception deep within QUR'));

    const races = raceResults.results.map(mapApiToRaces);

    // GET SPECIALIZATION INDEX
    const specIndex = await getWowPlayableSpec();
    const specsToProcess = specIndex.character_specializations.map(({ id }) => ({ id }));

    let specResults = await queueUntilResolved(
        getWowPlayableSpec,
        specsToProcess,
        15,
        3,
        { showBar: true, debug: true }
    )
    .catch(o => console.log('uncaught exception deep within QUR'));

    const specs = specResults.results.map(mapApiToSpecs);

    // SCRAPE CLASSES FROM SPECS
    const clsListWithDupes = specResults.results
        .map(({ playable_class }) => playable_class)
        .flatMap(({ id, name }) => ({ id, name }));
    const classes = [...new Map(clsListWithDupes.map((item) => [item["id"], item])).values()];

    
    return {
        created: Date.now(),
        classes,
        races,
        specs
    };

};

module.exports = getSnapshot;
