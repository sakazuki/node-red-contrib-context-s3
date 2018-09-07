
const AWS = require('aws-sdk')

var MemoryStore = require("./memory");

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


function S3context(config) {
    this.config = config;
    this.s3BucketName = config.awsS3Bucket;
    this.appname = config.awsS3Appname;
    AWS.config.region = config.awsRegion || process.env.AWS_REGION;
    if (config.hasOwnProperty('cache')?config.cache:true) {
        this.cache = MemoryStore({});
    }
}

S3context.prototype.open = () => {
    const self = this;
    return fs.ensureDir
}

S3context.prototype.close = () => {
    return Promise.resolve();
}

S3context.prototype.get = (scope, key, callback) => {
    if(typeof callback !== "function"){
        throw new Error("Callback must be a function");
    }
    const storagePath = getStoragePath(this.s3BucketName, scope);
    const params = {
        Bucket: this.awsS3Bucket,
        Key: soragePath
    }
    s3.getObject(params, (err, doc) => {
        if (err) {

        }else{

        }
    })
}

S3context.prototype.set = (scope, key, value, callback) => {
    if(typeof callback !== "function"){
        throw new Error("Callback must be a function");
    }
    const storagePath = getStoragePath(this.s3BucketName, scope);
    const params = {
        Bucket: this.awsS3Bucket,
        Key: soragePath,
        Body: JSON.stringify(value)
    }
    s3.upload(params, (err, doc) => {
        if (err) {

        }else{

        }
    })
}

S3context.prototype.keys = (scope, callback) => {}

S3context.prototype.delete = (scope) => {}

S3context.prototype.clean = (_activeNodes) => {}

module.exports = function(config){
    return new S3context(config);
};
