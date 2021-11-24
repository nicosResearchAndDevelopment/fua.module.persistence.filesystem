const
    {describe, test, before, after} = require('mocha'),
    expect                          = require('expect'),
    path                            = require('path'),
    fs                              = require('fs/promises'),
    context                         = require('./data/context.json'),
    {DataFactory}                   = require('@nrd/fua.module.persistence'),
    FilesystemStore                 = require('../src/module.persistence.filesystem.js'),
    sleep                           = (ms) => new Promise(resolve => setTimeout(resolve, ms));

describe('module.persistence.filesystem', function () {

    let store, quad_1, quad_2;
    before('construct a FilesystemStore and two quads', async function () {
        const
            factory   = new DataFactory(context),
            data_path = path.join(__dirname, 'data/empty.ttl');
        await fs.writeFile(data_path, '');

        store  = new FilesystemStore({
            default: 'file://empty.ttl',
            load:    {
                '@id':            'file://empty.ttl',
                'dct:identifier': data_path,
                'dct:format':     'text/turtle'
            }
        }, factory);
        quad_1 = factory.quad(
            factory.namedNode('http://example.com/subject'),
            factory.namedNode('http://example.com/predicate'),
            factory.namedNode('http://example.com/object')
        );
        quad_2 = factory.quad(
            quad_1.subject,
            quad_1.predicate,
            factory.literal('Hello World', 'en')
        );
    });

    test('should have an initial size of 0', async function () {
        await sleep(50);
        expect(await store.size()).toBe(0);
    });

    test('should add the two quads to the store once', async function () {
        await sleep(50);
        expect(await store.add(quad_1)).toBeTruthy();
        expect(await store.add(quad_2)).toBeTruthy();
        expect(await store.add(quad_1)).toBeFalsy();
        expect(await store.add(quad_2)).toBeFalsy();
    });

    test('should match the two added quads by their subject', async function () {
        await sleep(50);
        /** @type {Dataset} */
        const result = await store.match(quad_1.subject);
        expect(result.has(quad_1)).toBeTruthy();
        expect(result.has(quad_2)).toBeTruthy();
    });

    test('should currently have a size of 2', async function () {
        await sleep(50);
        expect(await store.size()).toBe(2);
    });

    test('should delete the first quad once', async function () {
        await sleep(50);
        expect(await store.delete(quad_1)).toBeTruthy();
        expect(await store.delete(quad_1)).toBeFalsy();
    });

    test('should only have the second quad stored', async function () {
        await sleep(50);
        expect(await store.has(quad_1)).toBeFalsy();
        expect(await store.has(quad_2)).toBeTruthy();
    });

    test('should match the remaining quad by its object', async function () {
        await sleep(50);
        /** @type {Dataset} */
        const result = await store.match(null, null, quad_2.object);
        expect(result.has(quad_1)).toBeFalsy();
        expect(result.has(quad_2)).toBeTruthy();
    });

    test('should have a size of 0, after it deleted the second quad', async function () {
        await sleep(50);
        await store.delete(quad_2);
        expect(await store.size()).toBe(0);
    });

    after('wait a sec before finishing', async function () {
        this.timeout(3e3);
        await sleep(2e3);
    });

});
