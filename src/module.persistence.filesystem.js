throw new Error('module.persistence.filesystem : not compatible with current module.persistence');

const
    // dataFactory = require('../../module.persistence/src/module.persistence.js'),
    // datasetFactory = require('../../module.persistence.inmemory/src/module.persistence.inmemory.js'),
    FileSystemStore = require('./FileSystemStore.js');

/**
 * @param {NamedNode} graph
 * @para {string} directory
 * @returns {FileSystemStore}
 */
exports.store = function (graph, directory) {
    throw new Error('currently not implemented');
    // return new FileSystemStore(graph, directory);
};