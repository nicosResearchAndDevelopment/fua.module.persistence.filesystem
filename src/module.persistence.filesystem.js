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

    #ready        = false;
    #readyPromise = null;
    /** @type {Map<string, FileDescription>} */
    #files        = new Map();

    // #update(id) {
    //     const file = this.#files.get(id);
    //     if (file.updater) clearTimeout(file.updater);
    //     file.updater = setTimeout(async () => {
    //         try {
    //             const content = await rdf.serializeDataset(file.dataset, file.format);
    //             await fs.writeFile(file.identifier, content);
    //         } catch (err) {
    //             this.emit('error', err);
    //         } finally {
    //             clearTimeout(file.updater);
    //             delete file.updater;
    //         }
    //     }, 1e3);
    // } // FilesystemStore##update

    constructor(options, factory) {
        super(options, factory);
        this.#readyPromise = (async () => {
            const resultArr = await rdf.loadDataFiles(options, factory);
            for (let file of resultArr) {
                if (file.dataset) {
                    this.#files.set(file.id, file);
                }
            }
        })();
        this.#readyPromise.then(() => {
            this.#ready        = true;
            this.#readyPromise = null;
        });
    } // FilesystemStore#constructor

    async size() {
        this.#ready || await this.#readyPromise;
        let size = 0;
        for (let file of this.#files.values()) {
            size += file.dataset.size;
        }
        return size;
    } // FilesystemStore#size

    async match(subject, predicate, object, graph) {
        const dataset = await super.match(subject, predicate, object, graph);
        if (graph) {
            assert(this.factory.isNamedNode(graph), 'FilesystemStore#add : expected graph to be a NamedNode');
            const file = this.#files.get(graph.value);
            if (file) {
                for (let fileQuad of file.dataset.match(subject, predicate, object)) {
                    const quad = this.factory.quad(fileQuad.subject, fileQuad.predicate, fileQuad.object, graph)
                    dataset.add(quad);
                }
            }
        } else {
            for (let file of this.#files.values()) {
                const graph = this.factory.namedNode(file.id);
                for (let fileQuad of file.dataset.match(subject, predicate, object)) {
                    const quad = this.factory.quad(fileQuad.subject, fileQuad.predicate, fileQuad.object, graph)
                    dataset.add(quad);
                }
            }
        }
        return dataset;
    } // FilesystemStore#match

    async add(quads) {
        const
            quadArr    = await super.add(quads),
            quadArrMap = new Map();
        for (let quad of quadArr) {
            assert(this.factory.isNamedNode(quad.graph), 'FilesystemStore#add : expected quad.graph to be a NamedNode');
            const file = this.#files.get(quad.graph.value);
            assert(file, 'FilesystemStore#add : expected quad to contain a known graph');
            if (quadArrMap.has(file)) {
                quadArrMap.get(file).push(quad);
            } else {
                quadArrMap.set(file, [quad]);
            }
        }
        let added = 0;
        for (let [file, fileQuadArr] of quadArrMap.entries()) {
            for (let quad of fileQuadArr) {
                const fileQuad = this.factory.quad(quad.subject, quad.predicate, quad.object);
                if (!file.dataset.has(fileQuad)) {
                    file.dataset.add(fileQuad);
                    this.emit('added', quad);
                    added++;
                }
            }
            // TODO schedule file for update
        }
        return added;
    } // FilesystemStore#add

    async addStream(stream) {
        assert(false, 'TODO : not implemented');
        // const quadStream = await super.addStream(stream);
        // TODO
    } // FilesystemStore#addStream

    async delete(quads) {
        assert(false, 'TODO : not implemented');
        // const quadArr = await super.delete(quads);
        // TODO
    } // FilesystemStore#delete

    async deleteStream(stream) {
        assert(false, 'TODO : not implemented');
        // const quadStream = await super.deleteStream(stream);
        // TODO
    } // FilesystemStore#deleteStream

    async deleteMatches(subject, predicate, object, graph) {
        assert(false, 'TODO : not implemented');
        // await super.deleteMatches(subject, predicate, object, graph);
        // TODO
    } // FilesystemStore#deleteMatches

    async has(quads) {
        assert(false, 'TODO : not implemented');
        // const quadArr = await super.has(quads);
        // TODO
    } // FilesystemStore#has

} // FilesystemStore

module.exports = FilesystemStore;
