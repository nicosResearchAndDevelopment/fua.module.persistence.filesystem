const
    assert = require("assert"),
    fs = require("fs"),
    regex_semantic_id = /^https?:\/\/\S+$|^\w+:\S+$/,
    regex_nonempty_key = /\S/,
    array_primitive_types = Object.freeze(["boolean", "number", "string"]);

/**
 * This is the general concept of a persistence adapter.
 * @typedef {Object} PersistenceAdapter 
 * @property {Function} CREATE Create a resource.
 * @property {Function} READ Return a resource or some properties.
 * @property {Function} UPDATE Update a property or a reference.
 * @property {Function} DELETE Delete a resource or a reference.
 * @property {Function} LIST List targets of a reference on a resource.
 * 
 * This is a persistent adapter with build in methods for the filesystem.
 * @typedef {PersistenceAdapter} FilesystemAdapter
 * 
 * This is the factory method to build a persistence adapter for the filesystem.
 * @param {Object} config 
 * @param {String} config.root 
 * @returns {FilesystemAdapter}
 */
module.exports = function (config) {

    assert(typeof config === "object" && config !== null,
        "The config for a persistence adapter must be a nonnull object.");
    assert(is_nonempty_key(config["root"]),
        "The config.root must contain a filesystem path.");

    /** @type {String} */
    const root_folder = config["root"];

    /**
     * Returns true, if the value does include at least one nonspace character.
     * @param {String} value 
     * @returns {Boolean}
     */
    function is_nonempty_key(value) {
        return regex_nonempty_key.test(value);
    } // is_nonempty_key

    /**
     * This is an IRI or a prefixed IRI.
     * @typedef {String|IRI} SemanticID
     * 
     * Returns true, if the value is a complete or prefixed IRI.
     * This function is important to distinct values from IRIs and
     * to make sure, subject, predicate and object have valid ids.
     * @param {SemanticID} value 
     * @returns {Boolean}
     */
    function is_semantic_id(value) {
        return regex_semantic_id.test(value);
    } // is_semantic_id

    /**
     * This are the only values neo4j can store on a node.
     * @typedef {null|Boolean|Number|String|Array<Boolean>|Array<Number>|Array<String>} PrimitiveValue 
     * 
     * Returns true, if the value is primitive. This function
     * is important to make sure, a value can be stored in neo4j.
     * @param {PrimitiveValue} value 
     * @returns {Boolean}
     */
    function is_primitive_value(value) {
        return value === null
            || array_primitive_types.includes(typeof value)
            || (Array.isArray(value) && array_primitive_types.some(
                type => value.every(arrValue => typeof arrValue === type)
            ));
    } // is_primitive_value

    /**
     * Uses the filesystem to make a method call and returns
     * the result as promise instead of using callbacks.
     * @async
     * @param {string} method 
     * @param  {...*} args 
     * @returns {*}
     */
    function request_filesystem(method, ...args) {
        return new Promise((resolve, reject) =>
            fs[method](...args, (err, result) =>
                err ? reject(err) : resolve(result)
            )
        );
    } // request_filesystem

    /**
     * TODO describe operation EXIST
     * @async
     * @param {SemanticID} subject 
     * @returns {Boolean}
     */
    async function operation_fs_exist(subject) {

        assert(is_semantic_id(subject),
            `filesystem_adapter - operation_exist - invalid {SemanticID} subject <${subject}>`);

        // /** @type {Number} */
        // const existsRecord = await request_filesystem("EXISTS", subject);
        // return !!existsRecord;

    } // operation_fs_exist

    /**
     * TODO describe operation CREATE
     * @async
     * @param {SemanticID} subject 
     * @returns {Boolean}
     */
    async function operation_fs_create(subject) {

        assert(is_semantic_id(subject),
            `filesystem_adapter - operation_create - invalid {SemanticID} subject <${subject}>`);

        // /** @type {Number} */
        // const createRecord = await request_filesystem("HSETNX", subject, "@id", JSON.stringify(subject));
        // if (createRecord) await request_filesystem("HSETNX", subject, "@type", JSON.stringify(["rdfs:Resource"]));
        // return !!createRecord;

    } // operation_fs_create

    /**
     * TODO describe operation READ_subject
     * @async
     * @param {SemanticID} subject 
     * @returns {Object|null}
     */
    async function operation_fs_read_subject(subject) {

        assert(is_semantic_id(subject),
            `filesystem_adapter - operation_read_subject - invalid {SemanticID} subject <${subject}>`);

        // /** @type {Object<String>|null} */
        // const readRecord = await request_filesystem("HGETALL", subject);
        // if (readRecord) for (let key in readRecord) {
        //     readRecord[key] = JSON.parse(readRecord[key]);
        // } // if-for

        // return readRecord;

    } // operation_fs_read_subject

    /**
     * TODO describe operation READ_type
     * @async
     * @param {SemanticID} subject 
     * @returns {Array<SemanticID>}
     */
    async function operation_fs_read_type(subject) {

        assert(is_semantic_id(subject),
            `filesystem_adapter - operation_read_type - invalid {SemanticID} subject <${subject}>`);

        // /** @type {String|null} */
        // const readRecord = await request_filesystem("HGET", subject, "@type");
        // return readRecord ? JSON.parse(readRecord) : null;

    } // operation_fs_read_type

    /**
     * TODO describe operation READ
     * @async
     * @param {SemanticID} subject 
     * @param {String|Array<String>} [key] 
     * @returns {Object|null|PrimitiveValue|Array<PrimitiveValue>}
     */
    async function operation_fs_read(subject, key) {

        if (!key) return await operation_fs_read_subject(subject);
        if (key === "@type") return await operation_fs_read_type(subject);

        assert(is_semantic_id(subject),
            `filesystem_adapter - operation_read - invalid {SemanticID} subject <${subject}>`);

        const isArray = Array.isArray(key);
        /** @type {Array<String>} */
        const keyArr = isArray ? key : [key];

        assert(keyArr.every(is_nonempty_key),
            `filesystem_adapter - operation_read - {String|Array<String>} ${isArray ? "some " : ""}key <${key}> is empty`);

        // if (!(await operation_fs_exist(subject)))
        //     return null;

        // /** @type {Array<String|null>} */
        // const readRecords = await request_filesystem("HMGET", subject, ...keyArr);
        // const valueArr = readRecords.map(val => val ? JSON.parse(val) : null);

        // return isArray ? valueArr : valueArr[0];

    } // operation_fs_read

    /**
     * TODO describe operation UPDATE_predicate
     * @async
     * @param {SemanticID} subject 
     * @param {SemanticID} predicate 
     * @param {SemanticID} object 
     * @returns {Boolean}
     */
    async function operation_fs_update_predicate(subject, predicate, object) {

        assert(is_semantic_id(subject),
            `filesystem_adapter - operation_update_predicate - invalid {SemanticID} subject <${subject}>`);
        assert(is_semantic_id(predicate),
            `filesystem_adapter - operation_update_predicate - invalid {SemanticID} predicate <${predicate}>`);
        assert(is_semantic_id(object),
            `filesystem_adapter - operation_update_predicate - invalid {SemanticID} object <${object}>`);

        // if (!(await operation_fs_exist(subject)))
        //     return false;

        // /** @type {Array<SemanticID>|null} */
        // const prevObjects = await operation_fs_list(subject, predicate);

        // const nextObjects = prevObjects
        //     ? prevObjects.includes(object)
        //         ? null
        //         : [...prevObjects, object]
        //     : [object];

        // if (nextObjects)
        //     await request_filesystem("HSET", subject, predicate, JSON.stringify(nextObjects));
        // return true;

    } // operation_fs_update_predicate

    /**
     * TODO describe operation UPDATE_type
     * @async
     * @param {SemanticID} subject 
     * @param {SemanticID|Array<SemanticID>} type 
     * @returns {Boolean}
     */
    async function operation_fs_update_type(subject, type) {

        assert(is_semantic_id(subject),
            `filesystem_adapter - operation_update_type - invalid {SemanticID} subject <${subject}>`);

        /** @type {Array<SemanticID>} */
        const typeArr = Array.isArray(type) ? type : [type];

        assert(typeArr.every(is_semantic_id),
            `filesystem_adapter - operation_update_type - invalid {SemanticID|Array<SemanticID>} type <${type}>`);
        if (!typeArr.includes("rdfs:Resource"))
            typeArr.push("rdfs:Resource");

        // /** @type {Array<SemanticID>} */
        // const prevTypes = await operation_fs_read_type(subject);
        // if (!prevTypes) return false;

        // if (typeArr.some(val => !prevTypes.includes(val)) || prevTypes.some(val => !typeArr.includes(val)))
        //     await request_filesystem("HSET", subject, "@type", JSON.stringify(typeArr));
        // return true;

    } // operation_fs_update_type

    /**
     * TODO describe operation UPDATE
     * @async
     * @param {SemanticID} subject 
     * @param {String|SemanticID} key 
     * @param {PrimitiveValue|SemanticID} value 
     * @returns {Boolean}
     */
    async function operation_fs_update(subject, key, value) {

        if (key === "@type") return await operation_fs_update_type(subject, value);
        if (is_semantic_id(key) && is_semantic_id(value)) return await operation_fs_update_predicate(subject, key, value);

        assert(is_semantic_id(subject),
            `filesystem_adapter - operation_update - invalid {SemanticID} subject <${subject}>`);
        assert(is_nonempty_key(key),
            `filesystem_adapter - operation_update - {String|SemanticID} key <${key}> is empty`);
        assert(is_primitive_value(value),
            `filesystem_adapter - operation_update - invalid {PrimitiveValue|SemanticID} value <${value}>`);

        // if (!(await operation_fs_exist(subject)))
        //     return false;

        // await request_filesystem("HSET", subject, key, JSON.stringify(value));
        // return true;

    } // operation_fs_update

    /**
     * TODO describe operation DELETE_predicate
     * @async
     * @param {SemanticID} subject 
     * @param {SemanticID} predicate 
     * @param {SemanticID} object 
     * @returns {Boolean} 
     */
    async function operation_fs_delete_predicate(subject, predicate, object) {

        assert(is_semantic_id(subject),
            `filesystem_adapter - operation_delete_predicate - invalid {SemanticID} subject <${subject}>`);
        assert(is_semantic_id(predicate),
            `filesystem_adapter - operation_delete_predicate - invalid {SemanticID} predicate <${predicate}>`);
        assert(is_semantic_id(object),
            `filesystem_adapter - operation_delete_predicate - invalid {SemanticID} object <${object}>`);

        // /** @type {Array<SemanticID>|null} */
        // const prevObjects = await operation_fs_list(subject, predicate);
        // if (!prevObjects) return false;
        // const objIndex = prevObjects.indexOf(object);
        // if (objIndex < 0) return false;

        // /** @type {Array<SemanticID>} */
        // const nextObjects = prevObjects; nextObjects.splice(objIndex, 1);
        // if (nextObjects.length > 0) await request_filesystem("HSET", subject, predicate, JSON.stringify(nextObjects));
        // else await request_filesystem("HDEL", subject, predicate);
        // return true;

    } // operation_fs_delete_predicate

    /**
     * TODO describe operation DELETE
     * @async
     * @param {SemanticID} subject 
     * @param {SemanticID} [predicate] 
     * @param {SemanticID} [object] 
     * @returns {Boolean}
     */
    async function operation_fs_delete(subject, predicate, object) {

        if (predicate || object) return await operation_fs_delete_predicate(subject, predicate, object);

        assert(is_semantic_id(subject),
            `filesystem_adapter - operation_delete - invalid {SemanticID} subject <${subject}>`);

        // const deleteRecord = await request_filesystem("DEL", subject);
        // return !!deleteRecord;

    } // operation_fs_delete

    /**
     * TODO describe operation LIST
     * @async
     * @param {SemanticID} subject 
     * @param {SemanticID} predicate 
     * @returns {Array<SemanticID>|null}
     */
    async function operation_fs_list(subject, predicate) {

        assert(is_semantic_id(subject),
            `filesystem_adapter - operation_list - invalid {SemanticID} subject <${subject}>`);
        assert(is_semantic_id(predicate),
            `filesystem_adapter - operation_list - invalid {SemanticID} predicate <${predicate}>`);

        // const listRecord = await request_filesystem("HGET", subject, predicate);
        // return listRecord ? JSON.parse(listRecord) : null;

    } // operation_fs_list

    /**
     * Creates a promise that times out after a given number of seconds.
     * If the original promise finishes before that, the error or result
     * will be resolved or rejected accordingly and the timeout will be canceled.
     * @param {Promise} origPromise 
     * @param {Number} timeoutDelay 
     * @param {String} [errMsg="This promise timed out after waiting ${timeoutDelay}s for the original promise."] 
     * @returns {Promise}
     */
    function create_timeout_promise(origPromise, timeoutDelay, errMsg) {
        assert(origPromise instanceof Promise,
            "The promise must be a Promise.");
        assert(typeof timeoutDelay === "number" && timeoutDelay > 0,
            "The timeout must be a number greater than 0.");

        let timeoutErr = new Error(typeof errMsg === "string" ? errMsg :
            `This promise timed out after waiting ${timeoutDelay}s for the original promise.`);
        Object.defineProperty(timeoutErr, "name", { value: "TimeoutError" });
        Error.captureStackTrace(timeoutErr, create_timeout_promise);

        return new Promise((resolve, reject) => {
            let pending = true;

            let timeoutID = setTimeout(() => {
                if (pending) {
                    pending = false;
                    clearTimeout(timeoutID);
                    reject(timeoutErr);
                }
            }, 1e3 * timeoutDelay);

            origPromise.then((result) => {
                if (pending) {
                    pending = false;
                    clearTimeout(timeoutID);
                    resolve(result);
                }
            }).catch((err) => {
                if (pending) {
                    pending = false;
                    clearTimeout(timeoutID);
                    reject(err);
                }
            });
        });
    } // create_timeout_promise

    /** @type {FilesystemAdapter} */
    const filesystem_adapter = Object.freeze({

        "CREATE": (subject, timeout) => !timeout ? operation_fs_create(subject)
            : create_timeout_promise(operation_fs_create(subject), timeout),

        "READ": (subject, key, timeout) => !timeout ? operation_fs_read(subject, key)
            : create_timeout_promise(operation_fs_read(subject, key), timeout),

        "UPDATE": (subject, key, value, timeout) => !timeout ? operation_fs_update(subject, key, value)
            : create_timeout_promise(operation_fs_update(subject, key, value), timeout),

        "DELETE": (subject, predicate, object, timeout) => !timeout ? operation_fs_delete(subject, predicate, object)
            : create_timeout_promise(operation_fs_delete(subject, predicate, object), timeout),

        "LIST": (subject, predicate, timeout) => !timeout ? operation_fs_list(subject, predicate)
            : create_timeout_promise(operation_fs_list(subject, predicate), timeout),

    }); // filesystem_adapter

    return filesystem_adapter;

}; // module.exports