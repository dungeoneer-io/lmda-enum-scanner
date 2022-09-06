const { describe, expect, test } = require('@jest/globals');

const {
    mapApiToRaces,
    mapApiToSpecs
} = require('../enum-snapshot');

const ArrayOfRaces = require('./__fixtures__/array-of-races.json');
const ArrayOfSpecs = require('./__fixtures__/array-of-specs.json');

describe('transformers', () => {
    test('mapApiToRaces', () => {
        const actual = ArrayOfRaces.map(mapApiToRaces);
        expect(actual).toMatchSnapshot();
    });

    test('mapApiToSpecs', () => {
        const result = ArrayOfSpecs.map(mapApiToSpecs);
        expect(result).toMatchSnapshot();
    });
});