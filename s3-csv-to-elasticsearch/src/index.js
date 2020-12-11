const _AWS = require('aws-sdk');
const _s3 = new _AWS.S3();
const _path = require('path');
const _stream = require('stream');
const _LineStream = require('byline').LineStream;
const _parse = require('clf-parser');  // Apache Common Log Format
const _crypto = require('crypto');
const _https = require('https');

let _context;

const _configFileName = 'config.json';

const _usable = {
    s3: true,
    elasticsearch: false
}

const _elasticsearch = {
    devel: {
        endpoint: 'my-search-endpoint.amazonaws.com'
    }
    , prod: {
        endpoint: 'my-search-endpoint.amazonaws.com'
    },
    region: 'ap-northeast-2',
    service: 'es',
    doctype: '_doc'
};


let _totalDocumentsCount = 0;
let _addedDocumentCount = 0;

const _bulkQueue = {
    queue: [], 
    maxQueuSize: 1000,
    isFull: () => {
        return _bulkQueue.maxQueuSize <= _bulkQueue.queue.length;
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
    indexSettings: {},
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
        }, 
        init: (event) => {
            _config.s3Bucket = event.Records[0].s3.bucket.name;
            _config.s3Key = event.Records[0].s3.object.key;
        
            // key : {root-path}/{index}/{devel|prod}/{YYYYMMDDhh24miss}.{create|update}.csv
            const paths = _config.s3Key.split('/');
            for (let seq = 0; seq < _configValues.fromS3Key.length; seq++) {
                _config[_configValues.fromS3Key[seq]] = paths[seq];
            };
        
            _config.path = paths.slice(0, 3).join('/') + '/';
            
            _fn.file.readS3ObjectStringAnsSetConfig(['indexMappings', 'fileFieldDelemeter'], _configFileName);

        
            const indexInfo = _config.fileName.split('.');
            for (let seq = 0; seq < _configValues.fromFileName.length; seq++) {
                _fn.config.setValue(_configValues.fromFileName[seq], indexInfo[seq]);
            };

            _fn.config.setRealIndex();
        
            console.log('config : ', JSON.stringify(_config));
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
        document: (fields, document) => {
            if (_config.indexFieldNames.length != fields.length) {
                const errorMessage = `[makeBulkJsonAndAddQueue] fieldCount Not Equals, 
                    header : ${_config.indexFieldNames.length }, 
                    document :${fields.length}, 
                    - ${document}`;

                throw new Error(errorMessage);
            }
        }
        
    },
    file: {
        readS3ObjectStringAnsSetConfig: (keys, fileName) => {
            console.log('[readS3ObjectStringAnsSetConfig] : ', keys.join(','), _config.path + fileName);

            if (!_usable.s3) {
                return;
            }

            console.log('[readS3ObjectStringAnsSetConfig] : file read');

            _s3.getObject(
                {Bucket: _config.s3Bucket, Key: _config.path + fileName},
                (error, data) => {
                    if (!error) {
                        try {
                            const configByFile = JSON.parse(data.Body.toString('utf-8'));

                            console.log('_s3.getObject', data.Body.toString('utf-8'));
                            
                            keys.forEach(key => _fn.config.setValue(key, configByFile[key]));
                        } catch (exception) {
                            throw new Error(error);
                        }
                    } else {
                        throw new Error(`s3 file read fail - bucket: ${_config.s3Bucket}, key : ${_config.path + fileName}`);
                    }
                } 
            );
        }
    }, 
    indexing: {
        makeBulkJsonAndAddQueue: (document) => {
            const fields = document.split(_config.fileFieldDelemeter);
        
            _fn.validator.document(fields, document);
        
            const id = document[0];
        
            const header = {index: {_index: _config.index, _type: 'doc', _id: id}};
            const body = {};

            for (let fieldIndex = 0; fieldIndex < fields.lengthl; fieldIndex++) {
                body[_config.indexFieldNames[fieldIndex]] = fields[fieldIndex];
            }

            _bulkQueue.push(header);
            _bulkQueue.push(body);
        },
        bulk: (document) => {
            console.log('bulkIndex, document : ', document);
            _addedDocumentCount++;
        
            if (_addedDocumentCount === 1) {
                _fn.config.setIndexFields(document);
            } else {
                // document를 만들고 bulkQueue.push
                _fn.indexing.makeBulkJsonAndAddQueue(document);
        
                if (_bulkQueue.isFull() || _addedDocumentCount === _totalDocumentsCount) {
                    // bulkQueue 를 처리하고 bulkQueue 초기화
                    _fn.elasticsearch.postForBulk();
        
                    _bulkQueue.clear();
                }
            }  
        
            if (_addedDocumentCount === _totalDocumentsCount) {
                // Mark lambda success.  If not done so, it will be retried.
                console.log('All ', _addedDocumentCount, ' log records added to ES.');
            }        
        }
    },
    elasticsearch: {
        postForBulk: () => {
            const requestParams = _fn.elasticsearch.buildRequest('POST', '/_bulk', _bulkQueue.makeRequestBody());
            _fn.elasticsearch.request(requestParams, _fn.elasticsearch.callback);
        },
        createIndex: () => {
            const indexName = _config.realIndex;
            console.log('[createIndex] indexName : ', indexName, ', mappingFile : ', _configFileName);
        
            // do create index;
            const indexScheme = {};
            indexScheme.settings = _config.indexSettings;
            indexScheme.mappings = _config.indexMappings;

            const requestParams = _fn.elasticsearch.buildRequest('PUT', _config.realIndex, JSON.stringify(indexScheme));
            _fn.elasticsearch.request(requestParams, _fn.elasticsearch.callback);

        },
        rebindAlias: () => {
            const aliasName = _config.index;
            const indexName = _config.realIndex;
        
            console.log('[rebindAlias] indexName : ', indexName, ', read aliasName : ', aliasName);

            // do rebind alias; 
            const command = {

            };

            const requestParams = _fn.elasticsearch.buildRequest('POST', '_alias', JSON.stringify(command));
            _fn.elasticsearch.request(requestParams, _fn.elasticsearch.callback);
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
                let responseBody = '';
                response.on('data', (chunk) => {
                    responseBody += chunk;
                });
        
                response.on('end', () => {
                    let info = {};
                    try {
                        info = JSON.parse(responseBody);
                    } catch(error) {}
                    
                    let failedItems;
                    let success;
                    let error;
                    
                    if (response.statusCode >= 200 && response.statusCode < 299) {
                        if (isBulkIndexing) { 
                            failedItems = info.items.filter((x) => {
                                return x.index.status >= 300;
                            });
            
                            success = { 
                                'attemptedItems': info.items.length,
                                'successfulItems': info.items.length - failedItems.length,
                                'failedItems': failedItems.length
                            };
                        } else {
                            success = {
                                'response': info
                            }; 
                        }
                    }
        
                    if (response.statusCode !== 200 || info.errors === true) {
                        // prevents logging of failed entries, but allows logging 
                        // of other errors such as access restrictions
                        delete info.items;
                        error = {
                            statusCode: response.statusCode,
                            responseBody: info
                        };
                    }
        
                    callback(error, success, response.statusCode, failedItems);
                });
            }).on('error', (e) => {
                callback(e);
            });
            request.end(requestParams.body);    
        },
        callback: (error, success, statusCode, failedItems) => {
            console.log('Response: ' + JSON.stringify({ 
                "statusCode": statusCode 
            }));

            if (error) {
                _fn.elasticsearch.logFailure(error, failedItems);
                // _context.fail(JSON.stringify(error));
            } else {
                console.log('Success: ' + JSON.stringify(success));
            }
        },
        logFailure: (error, failedItems) => {
            console.log('Error: ' + JSON.stringify(error, null, 2));
    
            if (failedItems && failedItems.length > 0) {
                console.log("Failed Items: " +
                    JSON.stringify(failedItems, null, 2));
            }

            throw new Error(error);
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
    _context = context;

    try {
        _fn.config.init(event);

        if (_config.action === 'create') {
            _fn.elasticsearch.createIndex();
            readFileAndBulkIndex();
            _fn.elasticsearch.rebindAlias();
        } else if (_config.action === 'update') {
            readFileAndBulkIndex();
        } else {
            throw new Error('action is not create | update')
        }
    } catch(error) {
        console.error(error);
        _context.fail(JSON.stringify(error));
    }

    console.log('Success indexing : ', _config.realIndex, ', action : ', _config.action);
    _context.succeed('Success');
};

function readFileAndBulkIndex() {
    if (!_usable.s3) {
        return;
    }

    const lineStream = new _LineStream();
    // A stream of log records, from parsing each log line
    const documentStream = new _stream.Transform({objectMode: true});
    documentStream._transform = (line, encoding, done) => {
        const documentRecord = _parse(line.toString());
        const serializedRecord = JSON.stringify(documentRecord);
        this.push(serializedRecord);
        _totalDocumentsCount ++;
        done();
    }    

    const s3Stream = _s3.getObject({Bucket: _config.s3Bucket, Key: _config.s3Key}).createReadStream();

    s3Stream
      .pipe(lineStream)
      .pipe(documentStream)
      .on('data', (document) => {
        if (!_fn.indexing.bulk(document)) {
            return;
        }
      });
}
