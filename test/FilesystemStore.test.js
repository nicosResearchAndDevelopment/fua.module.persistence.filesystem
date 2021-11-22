const
    {describe, test, before} = require('mocha'),
    expect                   = require('expect'),
    path                     = require('path'),
    FilesystemStore          = require('../src/module.persistence.filesystem.js');

describe('module.persistence.filesystem', function () {

    let store, quad_1, quad_2;
    before('construct a FilesystemStore and two quads', function () {
        store  = new FilesystemStore({
            '@id':            'file://empty.ttl',
            'dct:identifier': path.join(__dirname, 'data/empty.ttl'),
            'dct:format':     'text/turtle'
        });
        quad_1 = store.factory.quad(
            store.factory.namedNode('http://example.com/subject'),
            store.factory.namedNode('http://example.com/predicate'),
            store.factory.namedNode('http://example.com/object'),
            store.factory.namedNode('file://empty.ttl')
        );
        quad_2 = store.factory.quad(
            quad_1.subject,
            quad_1.predicate,
            store.factory.literal('Hello World', 'en'),
            store.factory.namedNode('file://empty.ttl')
        );
    });

    test('should have an initial size of 0', async function () {
        expect(await store.size()).toBe(0);
    });

    test('should add the two quads to the store once', async function () {
        expect(await store.add(quad_1)).toBeTruthy();
        expect(await store.add(quad_2)).toBeTruthy();
        expect(await store.add(quad_1)).toBeFalsy();
        expect(await store.add(quad_2)).toBeFalsy();
    });

    // TODO

});
