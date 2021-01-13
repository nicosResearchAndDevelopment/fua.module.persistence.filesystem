const
	Fs = require('fs'),
	Path = require('path'),
	promify = (fn, ...args) => new Promise((resolve, reject) => fn(...args, (err, result) => err ? reject(err) : resolve(result))),
	module_persistence_fs = require('./module.persistence.filesystem.beta.js');

// REM Only ever run on a test database. Queries might destroy active data.
// I would advice to download redis stable from https://redis.io/download/  and run it locally.

(async (/* async IIFE */) => {

	const fs_persistence_adapter = module_persistence_fs({
		fs: Fs,
		path: Path,
		root: Path.join(__dirname, 'data')
	});

	await fs_persistence_adapter.CREATE('test:hello_world');
	await fs_persistence_adapter.UPDATE('test:hello_world', '@type', ['rdfs:Resource', 'ldp:NonRDFSource', 'xsd:string']);
	await fs_persistence_adapter.UPDATE('test:hello_world', '@value', 'Hello World!');
	await fs_persistence_adapter.CREATE('test:lorem_ipsum');
	await fs_persistence_adapter.UPDATE('test:lorem_ipsum', 'rdf:label', 'Lorem Ipsum');
	await fs_persistence_adapter.UPDATE('test:lorem_ipsum', 'test:property', 'test:hello_world');
	await fs_persistence_adapter.UPDATE('test:hello_world', 'test:marzipan', 'test:lorem_ipsum');
	console.log('READ(test:hello_world) =>', await fs_persistence_adapter.READ('test:hello_world'), '\n');
	console.log('LIST(test:lorem_ipsum, test:property) =>', await fs_persistence_adapter.LIST('test:lorem_ipsum', 'test:property'), '\n');
	await fs_persistence_adapter.DELETE('test:hello_world', 'test:marzipan', 'test:lorem_ipsum');

})(/* async IIFE */).catch(console.error);
