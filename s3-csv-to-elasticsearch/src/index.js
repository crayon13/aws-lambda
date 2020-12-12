const _AWS = require('aws-sdk');
const _s3 = new _AWS.S3();
const _stream = require('stream');
const _LineStream = require('byline').LineStream;
const _crypto = require('crypto');
const _https = require('https');

const _configFileName = 'config.json';

const _usable = {
    s3: true,
    elasticsearch: false
}

const _elasticsearch = {
    devel: {
        endpoint: 'my-search-endpoint.amazonaws.com',
        indexSettings: {
            settings: {
                "number_of_shards": 1,
                "number_of_replicas": 0            
            } 
        }   
    }, 
    prod: {
        endpoint: 'my-search-endpoint.amazonaws.com',
        indexSettings: {
            settings: {
                "number_of_shards": 1,
                "number_of_replicas": 0            
            } 
        }      
    },
    region: 'ap-northeast-2',
    service: 'es',
    doctype: '_doc'
};


let _totalRecordsCount = 0;
let _addedRecordsCount = 0;

const _bulkQueue = {
    queue: [], 
    maxQueuSize: 1000,
    isFull: () => {
        return _bulkQueue.maxQueuSize <= _bulkQueue.queue.length;
    }, 
    isEmpty: () => {
        return _bulkQueue.queue.length === 0;
    },
    push: (jsonObject) => {
        _bulkQueue.queue.push(JSON.stringify(jsonObject));
    },
    clear: () => {
        _bulkQueue.queue = [];
    },
    size: () => {
        return _bulkQueue.queue.length;
    },
    makeRequestBody: () => {
        return _bulkQueue.queue.join('\n') + '\n';
    }
};

const _configValues = {
    fromS3Key: ['root', 'index', 'profile', 'fileName'],
    fromFileName: ['dataTime', 'action']
};  

const _config = {
    s3Bucket: '',
    s3Key: '', 
    indexMappings: {},
    fileFieldDelemeter: '',
    indexFieldNames: [], 
    root: '',
    path: '',
    index: '',
    realIndex: '',
    profile: '',
    fileName: '',
    dataTime: '', 
    action: ''
};

const _fn = {
    config: {
        setRealIndex: () => {
            let suffix = '';
            if (_config.action === 'create') {
                suffix = `-${_config.dataTime}`;
            }
            
            _config.realIndex = `${_config.index}${suffix}`;
        },
        setValue: (key, value) => {
            if (typeof _config[key] === 'undefined') {
                const errorMessge = `[fn.config.setValue] ${key} is undefined`;
                throw new Error(errorMessge);
            }

            _config[key] = value;
        },
        setIndexFields: (headerLine) => {
            const fields = headerLine.split(_config.fileFieldDelemeter);
            _fn.validator.fileHeader(fields);
            _config.indexFieldNames = fields;

            console.log('config : ', JSON.stringify(_config));
        },
        init: async (event) => {
            console.log('[config init]: start');

            _totalRecordsCount = 0;
            _addedRecordsCount = 0;

            _config.s3Bucket = event.Records[0].s3.bucket.name;
            _config.s3Key = event.Records[0].s3.object.key;
        
            // key : {root-path}/{index}/{devel|prod}/{YYYYMMDDhh24miss}.{create|update}.csv
            const paths = _config.s3Key.split('/');
            for (let seq = 0; seq < _configValues.fromS3Key.length; seq++) {
                _config[_configValues.fromS3Key[seq]] = paths[seq];
            };
        
            _config.path = paths.slice(0, 3).join('/') + '/';
            
            const indexInfo = _config.fileName.split('.');
            for (let seq = 0; seq < _configValues.fromFileName.length; seq++) {
                _fn.config.setValue(_configValues.fromFileName[seq], indexInfo[seq]);
            };
        
            _fn.config.setRealIndex();
        
            await _fn.file.readS3ObjectStringAndSetConfig(['indexMappings', 'fileFieldDelemeter'], _configFileName);

            console.log('[config init]: end', JSON.stringify(_config));
        }
    },
    validator: {
        fileHeader: (fields) => {
            const indexMappings = _config.indexMappings;
        
            fields.forEach(field => {
                if (!indexMappings.mappings.properties[field]) {
                    const errorMessage = `[validDataHeader] mapping field not found!, index : ${_config.index}, field: ${field}`;
                    throw new Error(errorMessage);
                }
            });
        }, 
        fileRecord: (fields, record) => {
            if (_config.indexFieldNames.length != fields.length) {
                const errorMessage = `[makeBulkJsonAndAddQueue] fieldCount Not Equals, 
                    header : ${_config.indexFieldNames.length }, 
                    document :${fields.length}, 
                    - ${record}`;

                throw new Error(errorMessage);
            }
        }
        
    },
    file: {
        readS3ObjectStringAndSetConfig: async (configKeys, fileName) => {
            console.log('[readS3ObjectStringAndSetConfig] : start');
            if (!_usable.s3) {
                return;
            }

            const params = {
                Bucket: _config.s3Bucket, 
                Key: _config.path + fileName
            };
            
            const s3Object = await _s3.getObject(params).promise();
            const configByFile = JSON.parse(s3Object.Body.toString('utf-8'));
            configKeys.forEach(key => _fn.config.setValue(key, configByFile[key]));

            console.log('[readS3ObjectStringAndSetConfig] : end');
        }
    }, 
    indexing: {
        makeBulkJsonAndAddQueue: (record) => {
            const fields = record.split(_config.fileFieldDelemeter);
        
            _fn.validator.fileRecord(fields, record);
        
            const id = record[0];
        
            const header = {index: {_index: _config.index, _type: 'doc', _id: id}};
            const body = {};

            for (let fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
                body[_config.indexFieldNames[fieldIndex]] = fields[fieldIndex];
            }

            _bulkQueue.push(header);
            _bulkQueue.push(body);
        },
        bulk: (record) => {
            console.log('[bulk] _totalRecordsCount :', _totalRecordsCount, ', _addedRecordsCount', _addedRecordsCount,'recored:', record);
            _fn.indexing.makeBulkJsonAndAddQueue(record);
    
            if (_bulkQueue.isFull()) {
                // bulkQueue 를 처리하고 bulkQueue 초기화
                _fn.elasticsearch.postForBulk();
            }
        },
        excuteIndexing: (context) => {
            if (!_usable.s3) {
                return;
            }
            const params = {
                Bucket: _config.s3Bucket, 
                Key: _config.s3Key
            };

            console.log('[readFileAndBulkIndex] start : ', JSON.stringify(params));

            _fn.elasticsearch.createIndex();
        
            const lineStream = new _LineStream();
            // A stream of log records, from parsing each log line
            const recordStream = new _stream.Transform({objectMode: true})
            recordStream._transform = function(line, encoding, done) {
                this.push(line.toString());
                done();
            }
               
            const s3Stream = _s3.getObject(params).createReadStream();
        
            // Flow: S3 file stream -> Log Line stream -> Log Record stream -> ES
            s3Stream
            .pipe(lineStream)
            .pipe(recordStream)
            .on('data', (record) => {
                if ( _totalRecordsCount === 0 ) {
                    _fn.config.setIndexFields(record);
                    console.log(JSON.stringify(_config));
                } else {
                    _fn.indexing.bulk(record)
                }

                _totalRecordsCount++;
            })
            .on('error', () => {
                console.error(
                    'Error getting object', JSON.stringify(params), 
                    'Make sure they exist and your bucket is in the same region as this function.');
                context.fail();
            })    
            .on('end', () => {
                _fn.elasticsearch.postForBulk();
                _fn.elasticsearch.rebindAlias();

                console.log('[readFileAndBulkIndex] end');
            });
        }
    },
    elasticsearch: {
        postForBulk: () => {
            if (_bulkQueue.isEmpty()) {
                return;
            }

            const requestParams = _fn.elasticsearch.buildRequest('POST', '/_bulk', _bulkQueue.makeRequestBody());
            _fn.elasticsearch.request(requestParams, _fn.elasticsearch.bulkCallback);
            _bulkQueue.clear();
        },
        createIndex: () => {
            if (_config.action !== 'create') {
                return;
            }

            const indexName = _config.realIndex;
            console.log('[createIndex] : start, indexName : ', indexName, ', mappingFile : ', _configFileName);
        
            // do create index;
            const indexScheme = {};
            indexScheme.settings = _elasticsearch[_config.profile].indexSettings.settings;
            indexScheme.mappings = _config.indexMappings.mappings;

            const requestParams = _fn.elasticsearch.buildRequest('PUT', _config.realIndex, JSON.stringify(indexScheme));
            _fn.elasticsearch.request(requestParams, _fn.elasticsearch.bulkCallback);

        },
        rebindAlias: () => {
            if (_config.action !== 'create') {
                return;
            }

            const aliasName = _config.index;
            const indexName = _config.realIndex;
        
            console.log('[rebindAlias] start, indexName : ', indexName, ', read aliasName : ', aliasName);

            // do rebind alias; 
            const command = {

            };

            const requestParams = _fn.elasticsearch.buildRequest('POST', '_alias', JSON.stringify(command));
            _fn.elasticsearch.request(requestParams, _fn.elasticsearch.bulkCallback);

            console.log('[rebindAlias] end');
        },
        buildRequest: (method, path, requestBody) => {
            const datetime = (new Date()).toISOString().replace(/[:\-]|\.\d{3}/g, '');
            const date = datetime.substr(0, 8);
            const kDate = _fn.crypto.hmac('AWS4' + process.env.AWS_SECRET_ACCESS_KEY, date);
            const kRegion = _fn.crypto.hmac(kDate, _elasticsearch.region);
            const kService = _fn.crypto.hmac(kRegion, _elasticsearch.service);
            const kSigning = _fn.crypto.hmac(kService, 'aws4_request');
            const endpoint = _elasticsearch[_config.profile].endpoint
            
            const request = {
                host: endpoint,
                method: method,
                path: path,
                body: requestBody,
                headers: { 
                    'Content-Type': 'application/json',
                    'Host': endpoint,
                    'Content-Length': Buffer.byteLength(requestBody),
                    'X-Amz-Security-Token': process.env.AWS_SESSION_TOKEN,
                    'X-Amz-Date': datetime
                }
            };
        
            const canonicalHeaders = Object.keys(request.headers)
                .sort((a, b) => { return a.toLowerCase() < b.toLowerCase() ? -1 : 1; })
                .map((k) => { return k.toLowerCase() + ':' + request.headers[k]; })
                .join('\n');
        
            const signedHeaders = Object.keys(request.headers)
                .map((k) => { return k.toLowerCase(); })
                .sort()
                .join(';');
        
            const canonicalString = [
                request.method,
                request.path, '',
                canonicalHeaders, '',
                signedHeaders,
                _fn.crypto.hash(request.body, 'hex'),
            ].join('\n');
        
            const credentialString = [ date, _elasticsearch.region, _elasticsearch.service, 'aws4_request' ].join('/');
        
            const stringToSign = [
                'AWS4-HMAC-SHA256',
                datetime,
                credentialString,
                _fn.crypto.hash(canonicalString, 'hex')
            ] .join('\n');
        
            request.headers.Authorization = [
                'AWS4-HMAC-SHA256 Credential=' + process.env.AWS_ACCESS_KEY_ID + '/' + credentialString,
                'SignedHeaders=' + signedHeaders,
                'Signature=' + _fn.crypto.hmac(kSigning, stringToSign, 'hex')
            ].join(', ');
        
            return request;            
        }, 
        request: (requestParams, callback, isBulkIndexing) => {
            if (!_usable.elasticsearch) {
                console.log(JSON.stringify(requestParams));
                return;
            }

            const request = _https.request(requestParams, (response) => {
                const responseBody = [];
                response.on('data', (chunk) => {
                    responseBody.push(chunk);
                });
        
                response.on('end', () => {
                    
        
                    if (response.statusCode !== 200 || info.errors === true) {
                        // prevents logging of failed entries, but allows logging 
                        // of other errors such as access restrictions
                        delete info.items;
                        error = {
                            statusCode: response.statusCode,
                            responseBody: info
                        };
                    }
        
                    callback(
                        {
                            statusCode: response.statusCode,
                            body: responseBod.join(''),
                            error: false
                        }
                    );
                });
            }).on('error', (e) => {
                callback(
                    {
                        statusCode: 0,
                        body: '',
                        error: e
                    }
                );
            });
            request.end(requestParams.body);    
        },
        bulkCallback: (response) => {
            if (error) {
                console.error(JSON.stringify(response));
            }
            
            throw new Error('bulk fail');
        }
    },
    crypto: {
        hmac: (key, str, encoding) => {
            return _crypto.createHmac('sha256', key).update(str, 'utf8').digest(encoding);
        }, 
        hash: (str, encoding) => {
            return _crypto.createHash('sha256').update(str, 'utf8').digest(encoding);
        }        
    }
}

exports.handler = (event, context) => {
    console.log('Received event: ', JSON.stringify(event, null, 2));

    _fn.config.init(event)
    .then(
        () => {
            _fn.indexing.excuteIndexing(context);
        }
    );
};
