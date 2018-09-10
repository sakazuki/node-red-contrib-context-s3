/**
 * 
 * 
 * 
 * 
 * s3Bukcet/appname/contexts
 *  ├── global
 *  │     └── global_context.json
 *  ├── <id of Flow 1>
 *  │     ├── flow_context.json
 *  │     ├── <id of Node a>.json
 *  │     └── <id of Node b>.json
 *  └── <id of Flow 2>
 *         ├── flow_context.json
 *         ├── <id of Node x>.json
 *         └── <id of Node y>.json
 *  
 **/

const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const path = require("path");
const safeJSONStringify = require('json-stringify-safe');
const util = require('/mnt/c/cygwin64/home/saito/work/node-red-0.19/node_modules/node-red/red/runtime/util');

function getStoragePath(baseDir, scope){
    if (scope.indexOf(":") === -1) {
        if(scope === "global"){
            return path.join(baseDir, "global", scope);
        }else{
            return path.join(baseDir, scope, "flow");
        }
    }else{
        const ids = scope.split(":");
        return path.join(baseDir, ids[1], ids[0]);
    }
}

function getBasePath(config) {
    const base = config.base || "context";
    return path.join(config.awsS3Appname, base) ;
}

function loadFile(bucket, key){
    const params = {
        Bucket: bucket,
        Key: key
    };
    return new Promise((resolve, reject) => {
        s3.getObject(params, (err, doc) => {
            if (err) {
                if (err.code === 'NoSuchKey') {
                    resolve("");
                }else{
                    console.error('loadFile', err);
                    resolve(undefined);
                }
            }else{
                resolve(doc.Body.toString());
            }
        })
    })
}

function listFiles(bucket, prefix) {
     const params = {
        Bucket: bucket,
        Prefix: prefix
    };
    return s3.listObjects(params, (err, data) => {
        if (err) {
            console.error('listFiles', err);
            return [];
        }else{
            return data.Contents.filter( v => /\.json$/.test(v.Key) )
        }
    });
}

function stringify(value) {
    var hasCircular;
    var result = safeJSONStringify(value,null,4,function(k,v){hasCircular = true})
    return { json: result, circular: hasCircular };
}

function S3context(config) {
    this.config = config;
    this.s3BucketName = config.awsS3Bucket;
    this.appname = config.awsS3Appname;
    this.storageBaseDir = getBasePath(config);
    AWS.config.region = config.awsRegion || process.env.AWS_REGION;
    this.pendingWrites = {};
    this.knownCircularRefs = {};
}

S3context.prototype.open = function () {
    const params = {
        Bucket: this.s3BucketName,
        Key: path.join(this.appname, 'flow.json')
    }
    return s3.getObject(params, (err, doc) => {
        if (err){
            console.error('open', err);
            return Promise.reject(err);
        }else{
            return Promise.resolve();
        }
    })
    // return Promise.resolve();
}

S3context.prototype.close = function () {
    return Promise.resolve();
}

S3context.prototype.get = function (scope, key, callback) {
    if(typeof callback !== "function"){
        throw new Error("Callback must be a function");
    }
    const storagePath = getStoragePath(this.storageBaseDir, scope);
    loadFile(this.s3BucketName, storagePath + ".json").then(function(data){
        var value;
        if(data){
            data = JSON.parse(data);
            if (!Array.isArray(key)) {
                try {
                    value = util.getObjectProperty(data,key);
                } catch(err) {
                    if (err.code === "INVALID_EXPR") {
                        throw err;
                    }
                    value = undefined;
                }
                callback(null, value);
            } else {
                var results = [undefined];
                for (var i=0;i<key.length;i++) {
                    try {
                        value = util.getObjectProperty(data,key[i]);
                    } catch(err) {
                        if (err.code === "INVALID_EXPR") {
                            throw err;
                        }
                        value = undefined;
                    }
                    results.push(value)
                }
                callback.apply(null,results);
            }
        }else {
            callback(null, undefined);
        }
    })
}

S3context.prototype.set = function (scope, key, value, callback) {
    var self = this;
    if(typeof callback !== "function"){
        throw new Error("Callback must be a function");
    }
    const storagePath = getStoragePath(this.storageBaseDir, scope);
    loadFile(this.s3BucketName, storagePath + ".json").then(function(data){
        var obj = data ? JSON.parse(data) : {}
        if (!Array.isArray(key)) {
            key = [key];
            value = [value];
        } else if (!Array.isArray(value)) {
            // key is an array, but value is not - wrap it as an array
            value = [value];
        }
        for (var i=0;i<key.length;i++) {
            var v = null;
            if (i<value.length) {
                v = value[i];
            }
            util.setObjectProperty(obj,key[i],v);
        }
        var stringifiedContext = stringify(obj);
        if (stringifiedContext.circular && !self.knownCircularRefs[scope]) {
            log.warn(log._("error-circular",{scope:scope}));
            self.knownCircularRefs[scope] = true;
        } else {
            delete self.knownCircularRefs[scope];
        }
        const params = {
            Bucket: self.s3BucketName,
            Key: storagePath + ".json",
            Body: JSON.stringify(stringifiedContext.json)
        }
        s3.upload(params, (err, doc) => {
            if (err) {
                throw new Error(err);
            }else{
                return;
            }
        })
    }).then(function(){
        if(typeof callback === "function"){
            callback(null);
        }
    }).catch(function(err){
        if(typeof callback === "function"){
            callback(err);
        }
    });
}

S3context.prototype.keys = function (scope, callback) {
    if(typeof callback !== "function"){
        throw new Error("Callback must be a function");
    }
    var storagePath = getStoragePath(this.storageBaseDir, scope);
    loadFile(this.s3BucketName, storagePath + ".json").then(function(data){
        if(data){
            callback(null, Object.keys(JSON.parse(data)));
        }else{
            callback(null, []);
        }
    }).catch(function(err){
        callback(err);
    });
}

S3context.prototype.delete = function (scope) {
    var that = this;
    delete this.pendingWrites[scope];
    return Promise.resolve().then(function() {
        const key = getStoragePath(this.storageBaseDir, scope);
        const params = {
            Bucket: this.s3BucketName,
            Key: key + ".json"
        }
        return s3.deleteObject(params, (err, data) => {
            if (err) {
                console.log(err);
                return Promise.reject(err);
            }
            return Promise.resolve(data);
        });
    });
}

S3context.prototype.clean = function (_activeNodes) {
    var activeNodes = {};
    _activeNodes.forEach(function(node) { activeNodes[node] = true });
    var self = this;
    this.knownCircularRefs = {};
    return listFiles(self.s3BucketName, self.storageBaseDir).then(function(files) {
        var promises = [];
        files.forEach(function(file) {
            var parts = file.split(path.sep);
            var removePromise;
            if (parts[0] === 'global') {
                // never clean global
                return;
            } else if (!activeNodes[parts[0]]) {
                // Flow removed - remove the whole dir
                params = {
                    Bucket: this.s3BucketName,
                    Key: parts[0]
                }
                removePromise = s3.deleteObject(params).promise();
            } else if (parts[1] !== 'flow.json' && !activeNodes[parts[1].substring(0,parts[1].length-5)]) {
                // Node removed - remove the context file
                params = {
                    Bucket: this.s3BucketName,
                    Key: file
                }
                removePromise = s3.deleteObject(params).promise();
            }
            if (removePromise) {
                promises.push(removePromise);
            }
        });
        return Promise.all(promises);
    })
}

module.exports = function(config){
    return new S3context(config);
};
