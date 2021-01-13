const
    regex_semantic_id = /^https?:\/\/\S+$|^\w+:\S+$/,
    regex_nonempty_key = /\S/,
    array_primitive_types = Object.freeze(["boolean", "number", "string"]),
    /** {@link https://nodejs.org/api/errors.html#errors_common_system_errors Common System Errors} */
    Fs_errCodes = {
        access_denied: "EACCES", // An attempt was made to access a file in a way forbidden by its file access permissions.
        operation_denied: "EPERM", // An attempt was made to perform an operation that requires elevated privileges.
        not_found: "ENOENT", // Commonly raised by fs operations to indicate that a component of the specified pathname does not exist. No entity (file or directory) could be found by the given path.
        do_exist: "EEXIST", // An existing file was the target of an operation that required that the target not exist.
        timed_out: "ETIMEDOUT" // A connect or send request failed because the connected party did not properly respond after a period of time.
    },
    /** {@link https://nodejs.org/api/fs.html#fs_file_system_flags File System Flags} */
    Fs_sysFlags = {
        read: "r", // Open file for reading. An exception occurs if the file does not exist.
        // write: "r+", // Open file for reading and writing. An exception occurs if the file does not exist.
        // REM The problem with r+ was that if the file gets shorter, the rest of the file still shows up on overwrite
        write: "w",
        create: "wx", // Open file for writing. The file is created (if it does not exist) or truncated (if it exists).
        delete: "r+"
    };

/**
 * @param {*} value 
 * @param {String} errMsg
 * @param {Class<Error>} [errType=Error] 
 * @throws {Error<errType>} if the value is falsy
 */
function assert(value, errMsg, errType = Error) {
    if (!value) {
        const err = new errType(`filesystem_adapter : ${errMsg}`);
        Error.captureStackTrace(err, assert);
        throw err;
    }
} // assert

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

    // TODO :: config
    // Aufnahme eines persistance-prefix. " "" for space...
    //

    assert(typeof config === "object" && config !== null,
        "The config for a persistence adapter must be a nonnull object.");
    assert(typeof config["fs"] === "object" && config !== null && typeof config["fs"].readFile === "function",
        "The config.fs must contain the filesystem module.");
    assert(typeof config["path"] === "object" && config !== null && typeof config["path"].join === "function",
        "The config.path must contain the path module.");
    assert(is_nonempty_key(config["root"]),
        "The config.root must contain the path to a persistent folder.");

    /** @alias module:fs */
    const Fs = config["fs"];

    /** @alias module:path */
    const Path = config["path"];

    /** @type {String} */
    const root_folder = config["root"];

    /**
     * @param {String} name 
     * @returns {String}  
     */
    function get_file_path(name) {
        return Path.join(
            root_folder,
            Buffer.from(name).toString("base64") + ".json"
        );
    } // get_file_path

    /**
     * Uses the filesystem to make a method call and returns
     * the result as promise instead of using callbacks.
     * @async
     * @param {String} method 
     * @param {...*} args 
     * @returns {*}
     */
    function request_fs(method, ...args) {
        return new Promise((resolve, reject) =>
            Fs[method](...args, (err, result) =>
                err ? reject(err) : resolve(result)
            )
        );
    } // request_fs

    async function subject_to_json(subject) {
        try {
            const buffer = await request_fs(
                "readFile",
                get_file_path(subject),
                { flag: Fs_sysFlags.read }
            );
            return JSON.parse(buffer.toString());
        } catch (err) {
            if (err.code === Fs_errCodes.not_found) return null;
            else throw err;
        }
    } // subject_to_json

    /**
     * TODO describe operation EXIST
     * @async
     * @param {SemanticID} subject 
     * @returns {Boolean}
     */
    async function operation_fs_exist(subject) {

        assert(is_semantic_id(subject),
            `operation_exist : invalid {SemanticID} subject <${subject}>`);

        try {
            /** @type {{isFile: Function}} */
            const statsRecord = await request_fs(
                "stat",
                get_file_path(subject)
            );
            return statsRecord.isFile();
        } catch (err) {
            if (err.code === Fs_errCodes.not_found) return false;
            else throw err;
        }

    } // operation_fs_exist

    /**
     * TODO describe operation CREATE
     * @async
     * @param {SemanticID} subject 
     * @returns {Boolean}
     */
    async function operation_fs_create(subject) {

        assert(is_semantic_id(subject),
            `operation_create : invalid {SemanticID} subject <${subject}>`);

        try {
            await request_fs(
                "writeFile",
                get_file_path(subject),
                JSON.stringify({
                    "@id": subject,
                    "@type": ["rdfs:Resource"]
                }),
                { flag: Fs_sysFlags.create }
            );
            return true;
        } catch (err) {
            if (err.code === Fs_errCodes.do_exist) return false;
            else throw err;
        }

    } // operation_fs_create

    /**
     * TODO describe operation READ_subject
     * @async
     * @param {SemanticID} subject 
     * @returns {Object|null}
     */
    async function operation_fs_read_subject(subject) {

        assert(is_semantic_id(subject),
            `operation_read_subject : invalid {SemanticID} subject <${subject}>`);

        try {
            /** @type {Buffer} */
            const readRecord = await request_fs(
                "readFile",
                get_file_path(subject),
                { flag: Fs_sysFlags.read }
            );
            return JSON.parse(readRecord.toString()) || null;
        } catch (err) {
            if (err.code === Fs_errCodes.not_found) return null;
            else throw err;
        }

    } // operation_fs_read_subject

    /**
     * TODO describe operation READ_type
     * @async
     * @param {SemanticID} subject 
     * @returns {Array<SemanticID>}
     */
    async function operation_fs_read_type(subject) {

        assert(is_semantic_id(subject),
            `operation_read_type : invalid {SemanticID} subject <${subject}>`);

        const readResult = await operation_fs_read_subject(subject);
        return readResult ? readResult["@type"] : null;

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
            `operation_read : invalid {SemanticID} subject <${subject}>`);

        const isArray = Array.isArray(key);
        /** @type {Array<String>} */
        const keyArr = isArray ? key : [key];

        assert(keyArr.every(is_nonempty_key),
            `operation_read : {String|Array<String>} ${isArray ? "some " : ""}key <${key}> is empty`);

        const readResult = await operation_fs_read_subject(subject);
        if (!readResult) return null;

        /** @type {Map<String, PrimitiveValue>} */
        const valueMap = new Map(Object.entries(readResult));
        /** @type {Array<PrimitiveValue>} */
        const valueArr = keyArr.map(val => valueMap.has(val) ? valueMap.get(val) : null);

        return isArray ? valueArr : valueArr[0];

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
            `operation_update_predicate : invalid {SemanticID} subject <${subject}>`);
        assert(is_semantic_id(predicate),
            `operation_update_predicate : invalid {SemanticID} predicate <${predicate}>`);
        assert(is_semantic_id(object),
            `operation_update_predicate : invalid {SemanticID} object <${object}>`);

        try {
            const file_path = get_file_path(subject);
            /** @type {Buffer} */
            const readRecord = await request_fs(
                "readFile",
                file_path,
                { flag: Fs_sysFlags.read }
            );
            /** @type {Object} */
            const json = JSON.parse(readRecord.toString()) || null;
            if (!json) return false;
            if (!Array.isArray(json[predicate]))
                json[predicate] = [];
            if (!json[predicate].includes(object)) {
                json[predicate].push(object);
                await request_fs(
                    "writeFile",
                    file_path,
                    JSON.stringify(json),
                    { flag: Fs_sysFlags.write }
                );
            }
            return true;
        } catch (err) {
            if (err.code === Fs_errCodes.not_found) return false;
            else throw err;
        }

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
            `operation_update_type : invalid {SemanticID} subject <${subject}>`);

        /** @type {Array<SemanticID>} */
        const typeArr = Array.isArray(type) ? type : [type];

        assert(typeArr.every(is_semantic_id),
            `operation_update_type : invalid {SemanticID|Array<SemanticID>} type <${type}>`);
        if (!typeArr.includes("rdfs:Resource"))
            typeArr.push("rdfs:Resource");

        try {
            const file_path = get_file_path(subject);
            /** @type {Buffer} */
            const readRecord = await request_fs(
                "readFile",
                file_path,
                { flag: Fs_sysFlags.read }
            );
            /** @type {Object} */
            const json = JSON.parse(readRecord.toString()) || null;
            if (!json) return false;
            json["@type"] = typeArr;
            await request_fs(
                "writeFile",
                file_path,
                JSON.stringify(json),
                { flag: Fs_sysFlags.write }
            );
            return true;
        } catch (err) {
            if (err.code === Fs_errCodes.not_found) return false;
            else throw err;
        }

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
            `operation_update : invalid {SemanticID} subject <${subject}>`);
        assert(is_nonempty_key(key),
            `operation_update : {String|SemanticID} key <${key}> is empty`);
        assert(is_primitive_value(value),
            `operation_update : invalid {PrimitiveValue|SemanticID} value <${value}>`);

        try {
            const file_path = get_file_path(subject);
            /** @type {Buffer} */
            const readRecord = await request_fs(
                "readFile",
                file_path,
                { flag: Fs_sysFlags.read }
            );
            /** @type {Object} */
            const json = JSON.parse(readRecord.toString()) || null;
            if (!json) return false;
            json[key] = value;
            await request_fs(
                "writeFile",
                file_path,
                JSON.stringify(json),
                { flag: Fs_sysFlags.write }
            );
            return true;
        } catch (err) {
            if (err.code === Fs_errCodes.not_found) return false;
            else throw err;
        }

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
            `operation_delete_predicate : invalid {SemanticID} subject <${subject}>`);
        assert(is_semantic_id(predicate),
            `operation_delete_predicate : invalid {SemanticID} predicate <${predicate}>`);
        assert(is_semantic_id(object),
            `operation_delete_predicate : invalid {SemanticID} object <${object}>`);

        try {
            const file_path = get_file_path(subject);
            /** @type {Buffer} */
            const readRecord = await request_fs(
                "readFile",
                file_path,
                { flag: Fs_sysFlags.read }
            );
            /** @type {Object} */
            const json = JSON.parse(readRecord.toString()) || null;
            if (!json) return false;
            if (!Array.isArray(json[predicate])) return true;
            const index = json[predicate].indexOf(object);
            if (index >= 0) {
                if (json[predicate].length > 1) json[predicate].splice(index, 1);
                else delete json[predicate];
                await request_fs(
                    "writeFile",
                    file_path,
                    JSON.stringify(json),
                    { flag: Fs_sysFlags.write }
                );
            }

            return true;
        } catch (err) {
            if (err.code === Fs_errCodes.not_found) return false;
            else throw err;
        }

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
            `operation_delete : invalid {SemanticID} subject <${subject}>`);

        try {
            await request_fs(
                "unlink",
                get_file_path(subject)
            );

            return true;
        } catch (err) {
            if (err.code === Fs_errCodes.not_found) return false;
            else throw err;
        }

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
            `operation_list : invalid {SemanticID} subject <${subject}>`);
        assert(is_semantic_id(predicate),
            `operation_list : invalid {SemanticID} predicate <${predicate}>`);

        const readResult = await operation_fs_read_subject(subject);
        return (readResult && Array.isArray(readResult[predicate]))
            ? readResult[predicate]
            : null;

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