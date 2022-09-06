const ROLE_NAME_TO_ID = {
    'TANK': 1,
    'HEALER': 2,
    'DAMAGE': 3
};

const RACE_NAME_TO_ID = {
    'NEUTRAL': 2,
    'HORDE': 0,
    'ALLIANCE': 1
};

const mapApiToRaces = ({ id, name, faction, is_allied_race: isAllied }) => ({
    id,
    name,
    faction: RACE_NAME_TO_ID[faction.type],
    isAllied
});

const mapApiToSpecs = ({ id, playable_class, name, role }) => ({
    id,
    name,
    class: playable_class.id,
    role: ROLE_NAME_TO_ID[role.type]
});

module.exports = {
    mapApiToRaces,
    mapApiToSpecs
};
