const
    util        = require('@nrd/fua.core.util'),
    assert      = new util.Assert('module.persistence.filesystem'),
    path        = require('path'),
    fs          = require('fs/promises'),
    rdf         = require('@nrd/fua.module.rdf'),
    {DataStore} = require('@nrd/fua.module.persistence');

/**
 * @typedef {object} FileDescription
 * @property {string} id An explicitly defined or randomly generated id for the graph reference.
 * @property {string} identifier The absolute file path of the source file to enable content updates.
 * @property {string} format The mime type of the source file to update the content in the correct format.
 * @property {fua.module.persistence.Dataset} dataset The content data as a dataset.
 */

class FilesystemStore extends DataStore {

    #ready         = false;
    #readyPromise  = null;
    /** @type {Map<string, FileDescription>} */
    #files         = new Map();
    #defaultFile   = '';
    /** @type {Map<string, number>} */
    #updateTimesMS = new Map();
    #updateDelayMS = 1e3;

    constructor(options, factory) {
        super(options, factory);

        const {defaultFile, loadFiles} = options;
        assert(util.isString(defaultFile), 'FilesystemStore#constructor : expected defaultFile to be a string', TypeError);
        assert(util.isObject(loadFiles), 'FilesystemStore#constructor : expected loadFiles to be an object', TypeError);

        this.#defaultFile  = defaultFile;
        this.#readyPromise = (async () => {
            const resultArr = await rdf.loadDataFiles(loadFiles, factory);
            for (let file of resultArr) {
                if (file.dataset) {
                    assert(!this.#files.has(file.id), 'FilesystemStore#constructor : expected file IDs to be unique');
                    this.#files.set(file.id, file);
                }
            }
            assert(this.#files.size > 0, 'FilesystemStore#constructor : expected at least one file to be loaded');
            assert(this.#files.has(defaultFile), 'FilesystemStore#constructor : expected files to contain the default');
        })().then(() => {
            this.#ready        = true;
            this.#readyPromise = null;
        });
    } // FilesystemStore#constructor

    #scheduleFileUpdate(fileId) {
        console.log('FilesystemStore##scheduleFileUpdate for ' + fileId);
        if (this.#updateTimesMS.has(fileId)) {
            // If the updateTimes already contains the fileId, it means that the updater is already started.
            // In that case just update the time, the updater will do the rest.
            this.#updateTimesMS.set(fileId, Date.now() + this.#updateDelayMS);
        } else {
            // If the updateTimes does not contain the fileId, the time has to be set regardless.
            // After that, a writable fileHandler needs to be acquired and the startFileUpdater method will do the rest.
            // This fileHandler will be used as long as the updater is not finished to block the process from exiting early.
            // After the updater is finished, the fileHandler will be closed be the updater itself to free resources.
            const file = this.#files.get(fileId);
            this.#updateTimesMS.set(fileId, Date.now() + this.#updateDelayMS);
            fs.open(file.identifier, 'w')
                .then(fileHandle => this.#startFileUpdater(file, fileHandle))
                .catch(err => this.emit('error', err));
        }
    } // FilesystemStore##scheduleFileUpdate

    #startFileUpdater(file, fileHandle) {
        console.log('FilesystemStore##startFileUpdater for ' + file.id);
        // Get the current updateTime and timeout for the rest of the time.
        let updateTimeMS = this.#updateTimesMS.get(file.id);
        setTimeout(async () => {
            try {
                // Check the updateTime again after waiting and start a new updater if necessary.
                updateTimeMS = this.#updateTimesMS.get(file.id);
                if (Date.now() < updateTimeMS) return this.#startFileUpdater(file, fileHandle);

                // Serialize the files dataset in the correct format and check if the updateTime has changed in the meantime.
                const fileContent = await rdf.serializeDataset(file.dataset, file.format);
                updateTimeMS      = this.#updateTimesMS.get(file.id);
                if (Date.now() < updateTimeMS) return this.#startFileUpdater(file, fileHandle);

                // Write the serialized dataset back to the file and check if the updateTime has changed in the meantime.
                await fileHandle.writeFile(fileContent);
                updateTimeMS = this.#updateTimesMS.get(file.id);
                if (Date.now() < updateTimeMS) return this.#startFileUpdater(file, fileHandle);

                // At this point, no changes to the updateTime has occurred so the updateTime can be deleted
                // and the fileHandler be closed to let scheduleFileUpdate start a fresh updater again.
                this.#updateTimesMS.delete(file.id);
                await fileHandle.close();
            } catch (err) {
                // In case of an error in the serialization or the file write, the fileHandler still needs to be closed.
                this.emit('error', err);
                await fileHandle.close();
            }
        }, Math.max(0, updateTimeMS - Date.now()));
    } // FilesystemStore##startFileUpdater

    async size() {
        this.#ready || await this.#readyPromise;
        let size = 0;
        for (let file of this.#files.values()) {
            size += file.dataset.size;
        }
        return size;
    } // FilesystemStore#size

    async match(subject, predicate, object, graph) {
        this.#ready || await this.#readyPromise;
        const dataset = await super.match(subject, predicate, object, graph);
        let fileIterable;

        if (graph) {
            if (this.factory.isDefaultGraph(graph)) {
                fileIterable = [this.#files.get(this.#defaultFile)];
            } else {
                assert(this.factory.isNamedNode(graph), 'FilesystemStore#match : expected graph to be a NamedNode');
                fileIterable = [this.#files.get(graph.value)];
            }
        } else {
            fileIterable = this.#files.values();
        }

        for (let file of fileIterable) {
            const fileGraph = (file.id === this.#defaultFile)
                ? this.factory.defaultGraph()
                : this.factory.namedNode(file.id);
            for (let fileQuad of file.dataset.match(subject, predicate, object)) {
                const quad = this.factory.quad(
                    fileQuad.subject,
                    fileQuad.predicate,
                    fileQuad.object,
                    fileGraph
                );
                dataset.add(quad);
            }
        }

        return dataset;
    } // FilesystemStore#match

    async add(quads) {
        this.#ready || await this.#readyPromise;
        const
            quadArr    = await super.add(quads),
            quadArrMap = new Map();

        for (let quad of quadArr) {
            let file;
            if (this.factory.isDefaultGraph(quad.graph)) {
                file = this.#files.get(this.#defaultFile);
            } else {
                assert(this.factory.isNamedNode(quad.graph), 'FilesystemStore#add : expected quad.graph to be a NamedNode');
                file = this.#files.get(quad.graph.value);
            }
            assert(file, 'FilesystemStore#add : expected quad to contain a known graph');
            if (quadArrMap.has(file)) {
                quadArrMap.get(file).push(quad);
            } else {
                quadArrMap.set(file, [quad]);
            }
        }

        let added = 0;
        for (let [file, fileQuadArr] of quadArrMap.entries()) {
            let fileEdited = false;
            for (let quad of fileQuadArr) {
                const fileQuad = this.factory.quad(quad.subject, quad.predicate, quad.object);
                if (!file.dataset.has(fileQuad)) {
                    file.dataset.add(fileQuad);
                    this.emit('added', quad);
                    added++;
                    fileEdited = true;
                }
            }
            if (fileEdited) this.#scheduleFileUpdate(file.id);
        }

        return added;
    } // FilesystemStore#add

    async addStream(stream) {
        this.#ready || await this.#readyPromise;
        const quadStream = await super.addStream(stream);
        let added        = 0;
        quadStream.on('data', (quad) => {
            let file;
            if (this.factory.isDefaultGraph(quad.graph)) {
                file = this.#files.get(this.#defaultFile);
            } else if (this.factory.isNamedNode(quad.graph)) {
                file = this.#files.get(quad.graph.value);
            } else {
                return;
            }
            const fileQuad = this.factory.quad(quad.subject, quad.predicate, quad.object);
            if (!file.dataset.has(fileQuad)) {
                file.dataset.add(fileQuad);
                this.emit('added', quad);
                added++;
                this.#scheduleFileUpdate(file.id);
            }
        });
        await new Promise(resolve => quadStream.on('end', resolve));
        return added;
    } // FilesystemStore#addStream

    async delete(quads) {
        this.#ready || await this.#readyPromise;
        const
            quadArr    = await super.add(quads),
            quadArrMap = new Map();

        for (let quad of quadArr) {
            let file;
            if (this.factory.isDefaultGraph(quad.graph)) {
                file = this.#files.get(this.#defaultFile);
            } else {
                assert(this.factory.isNamedNode(quad.graph), 'FilesystemStore#delete : expected quad.graph to be a NamedNode');
                file = this.#files.get(quad.graph.value);
            }
            assert(file, 'FilesystemStore#delete : expected quad to contain a known graph');
            if (quadArrMap.has(file)) {
                quadArrMap.get(file).push(quad);
            } else {
                quadArrMap.set(file, [quad]);
            }
        }

        let deleted = 0;
        for (let [file, fileQuadArr] of quadArrMap.entries()) {
            let fileEdited = false;
            for (let quad of fileQuadArr) {
                const fileQuad = this.factory.quad(quad.subject, quad.predicate, quad.object);
                if (file.dataset.has(fileQuad)) {
                    file.dataset.delete(fileQuad);
                    this.emit('deleted', quad);
                    deleted++;
                    fileEdited = true;
                }
            }
            if (fileEdited) this.#scheduleFileUpdate(file.id);
        }

        return deleted;
    } // FilesystemStore#delete

    async deleteStream(stream) {
        this.#ready || await this.#readyPromise;
        const quadStream = await super.addStream(stream);
        let deleted      = 0;
        quadStream.on('data', (quad) => {
            let file;
            if (this.factory.isDefaultGraph(quad.graph)) {
                file = this.#files.get(this.#defaultFile);
            } else if (this.factory.isNamedNode(quad.graph)) {
                file = this.#files.get(quad.graph.value);
            } else {
                return;
            }
            const fileQuad = this.factory.quad(quad.subject, quad.predicate, quad.object);
            if (file.dataset.has(fileQuad)) {
                file.dataset.delete(fileQuad);
                this.emit('deleted', quad);
                deleted++;
                this.#scheduleFileUpdate(file.id);
            }
        });
        await new Promise(resolve => quadStream.on('end', resolve));
        return deleted;
    } // FilesystemStore#deleteStream

    async deleteMatches(subject, predicate, object, graph) {
        this.#ready || await this.#readyPromise;
        await super.deleteMatches(subject, predicate, object, graph);
        let fileIterable;

        if (graph) {
            if (this.factory.isDefaultGraph(graph)) {
                fileIterable = [this.#files.get(this.#defaultFile)];
            } else {
                assert(this.factory.isNamedNode(graph), 'FilesystemStore#deleteMatches : expected graph to be a NamedNode');
                fileIterable = [this.#files.get(graph.value)];
            }
        } else {
            fileIterable = this.#files.values();
        }

        let deleted = 0;
        for (let file of fileIterable) {
            const fileGraph = (file.id === this.#defaultFile)
                ? this.factory.defaultGraph()
                : this.factory.namedNode(file.id);

            let fileEdited = false;
            for (let fileQuad of file.dataset.match(subject, predicate, object)) {
                const quad = this.factory.quad(
                    fileQuad.subject,
                    fileQuad.predicate,
                    fileQuad.object,
                    fileGraph
                );
                file.dataset.delete(fileQuad);
                this.emit('deleted', quad);
                deleted++;
                fileEdited = true;
            }
            if (fileEdited) this.#scheduleFileUpdate(file.id);
        }

        return deleted;
    } // FilesystemStore#deleteMatches

    async has(quads) {
        this.#ready || await this.#readyPromise;
        const
            quadArr    = await super.add(quads),
            quadArrMap = new Map();

        for (let quad of quadArr) {
            let file;
            if (this.factory.isDefaultGraph(quad.graph)) {
                file = this.#files.get(this.#defaultFile);
            } else if (this.factory.isNamedNode(quad.graph)) {
                file = this.#files.get(quad.graph.value);
            }
            if (!file) return false;
            if (quadArrMap.has(file)) {
                quadArrMap.get(file).push(quad);
            } else {
                quadArrMap.set(file, [quad]);
            }
        }

        for (let [file, fileQuadArr] of quadArrMap.entries()) {
            for (let quad of fileQuadArr) {
                const fileQuad = this.factory.quad(quad.subject, quad.predicate, quad.object);
                if (!file.dataset.has(fileQuad)) return false;
            }
        }

        return true;
    } // FilesystemStore#has

} // FilesystemStore

module.exports = FilesystemStore;
