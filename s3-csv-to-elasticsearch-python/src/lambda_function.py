import boto3
import json
import requests
from requests_aws4auth import AWS4Auth

_configFileName = 'config.json'

_usable = {
    's3': False,
    'elasticsearch': False
}

_elasticsearch = {
    'devel': {
        'endpoint': 'my-search-endpoint.amazonaws.com',
        'indexSettings': {
            'settings': {
                'number_of_shards': 1,
                'number_of_replicas': 0            
            } 
        }   
    }, 
    'prod': {
        'endpoint': 'my-search-endpoint.amazonaws.com',
        'indexSettings': {
            'settings': {
                'number_of_shards': 1,
                'number_of_replicas': 0            
            } 
        }      
    },
    'region': 'ap-northeast-2',
    'service': 'es',
    'doctype': '_doc',
    'headers': {'Content-Type': 'application/x-ndjson; charset=utf-8'}
}

_bulkQueue = {
    'queue': [],
    'maxQueuSize': 1000
}

_configValues = {
    'fromS3Key': ['root', 'alias', 'profile', 'fileName'],
    'fromFileName': ['dataTime', 'action']
}

_config = {
    's3Bucket': '',
    's3Key': '', 
    'indexMappings': {},
    'fileFieldDelemeter': '',
    'indexFieldNames': [], 
    'root': '',
    'path': '',
    'alias': '',
    'realIndex': '',
    'profile': '',
    'fileName': '',
    'dataTime': '', 
    'action': ''    
}

_s3Client = boto3.client('s3')
_credentials = boto3.Session().get_credentials()
_awsauth = AWS4Auth(_credentials.access_key
, _credentials.secret_key, _elasticsearch['region'], _elasticsearch['service']
, session_token=_credentials.token)

def lambda_handler(event, context):
    initConfig(event)
    createIndex()
    bulk()
    rebindAlias()

###########################################################################
def log(messge = ''):
    print('[INFO]', messge)


def setValue(dictionary = [], key = '', value = ''):
    if key in dictionary:
        dictionary[key] = value
    else:
        dictionary[key]
        

def setConfigFromFile(configKeys = []):
    filePath = _config['path'] + _configFileName
    log(_config['s3Bucket'] + ':' + filePath)

    configByFile = {}
    if _usable['s3']:
        # S3에서 파일을 읽어오는 것으로 
        s3Object = _s3Client.get_object(Bucket=_config['s3Bucket'], Key=filePath)
        jsonString = s3Object["Body"].read().decode('utf-8')
        log(jsonString)
        configByFile = json.loads(jsonString)
    else:
        configByFile = _fileConfig

    for key in configKeys:
        setValue(_config, key, configByFile[key])


def initConfig(event = {}):
    setValue(_config, 's3Bucket', event['Records'][0]['s3']['bucket']['name'])
    setValue(_config, 's3Key', event['Records'][0]['s3']['object']['key'])

    # set config From Path
    paths = _config['s3Key'].split('/')
    keySeq = 0
    for key in _configValues['fromS3Key']:
        setValue(_config, key, paths[keySeq])
        keySeq = keySeq + 1

    setValue(_config, 'path', '/'.join(paths[:3]) + '/')
    
    # set config From fileName
    indexInfo = _config['fileName'].split('.')
    keySeq = 0
    for key in _configValues['fromFileName']:
        setValue(_config, key, indexInfo[keySeq])
        keySeq = keySeq + 1

    # set real index name
    indexSuffx = ''
    if (_config['action'] == 'create'):
        indexSuffx = '-' + _config['dataTime']

    setValue(_config, 'realIndex', _config['alias'] + indexSuffx)

    # set Config From config.json
    setConfigFromFile(['indexMappings', 'fileFieldDelemeter'])

    log(_config)


def createIndex():
    alias = _config['alias']
    indexName = _config['realIndex']
    log(f'[createIndex] : start, indexName : {indexName}')

    indexScheme = {}
    indexScheme['settings'] = _elasticsearch[_config['profile']]['indexSettings']['settings']
    indexScheme['mappings'] = _config['indexMappings']['mappings']

    if (_usable['elasticsearch'] == False or _config['action'] != 'create' or alias == indexName):
        log(json.dumps(indexScheme))
        return

    response = requests.put(indexName, auth=_awsauth, data=json.dumps(indexScheme), headers=_elasticsearch['headers'])
    response.raise_for_status()
    return response.text


def headerValidate(fileds = []):
    properties = _config['indexMappings']['mappings']['properties']

    for field in fileds:
        properties[field]


def fieldsValidate(fileds = []):
    fieldsLength = len(fileds)
    headersLength = len(_config['indexFieldNames'])

    if (fieldsLength != headersLength):
        raise Exception(f'[fieldsValidate] fieldCount Not Equals Headers, headersLength : {headersLength}, fieldsLength : {fieldsLength}')


def isFullBulkQueue():
    return _bulkQueue['maxQueuSize'] <= len(_bulkQueue['queue'])


def isEmptyBulkQueue():
    return len(_bulkQueue['queue']) == 0


def addBulkQueue(dictionary):
    _bulkQueue['queue'].append(json.dumps(dictionary))


def makeBulkJsonAndAddQueue(fileds = []):
    fieldsValidate(fileds)

    id = fileds[0]
    header = {'index': {'_index': _config['realIndex'], '_id': id}}
    body = {}

    for fieldSeq in range(0, len(fileds) - 1):
        fieldName = _config['indexFieldNames'][fieldSeq]
        body[fieldName] = fileds[fieldSeq]

    addBulkQueue(header)
    addBulkQueue(body)


def makeRequestBodyByBulkQueueAndClear():
    data = '\n'.join(_bulkQueue['queue']) + '\n'
    _bulkQueue['queue'].clear()
    return data


def postForBulk():
    if isEmptyBulkQueue():
        return 'bulk queue is empty'

    requestBody = makeRequestBodyByBulkQueueAndClear()
    log(f'[postForBulk] {requestBody}')

    if (_usable['elasticsearch'] == False):
        return 'elasticsearch usable : False'

    response = requests.post('_bulk', auth=_awsauth, data=requestBody, headers=_elasticsearch['headers'])
    response.raise_for_status()    
    return response.text


def bulk():
    indexName = _config['realIndex']
    log(f'[bulk] : start, indexName : {indexName}')

    if (_usable['s3'] == False):
        return

    s3Object = _s3Client.get_object(Bucket=_config['s3Bucket'], Key=_config['s3Key'])
    recordLine = s3Object["Body"].read()

    recordCount = 0
    for line in recordLine.splitlines():
        recordCount = recordCount + 1
        record = line.decode('utf-8')
        log(record)

        fields = record.split(_config['fileFieldDelemeter'])

        if (recordCount == 1):
            log('헤더를 만들어요.')
            headerValidate(fields)
            setValue(_config, 'indexFieldNames', fields)
        else: 
            log('큐에 담고 색인을 해요')
            makeBulkJsonAndAddQueue(fields)

            if isFullBulkQueue():
                postForBulk()

    postForBulk()


def getAliasBindedIndex():
    if (_usable['elasticsearch'] == False):
        return ''

    alias = _config['alias']

    response = requests.get(f'_cat/aliases/{alias}?format=json', auth=_awsauth, headers=_elasticsearch['headers'])
    response.raise_for_status()   

    bindedIndices = json.loads(response.text)

    if (len(bindedIndices) > 1):
        raise Exception(f'{alias} is multi indices binded')
    elif (len(bindedIndices) == 0):
        return ''
    
    return bindedIndices[0]['index']



def rebindAlias():
    alias = _config['alias']
    indexName = _config['realIndex']
    bindedIndexName = getAliasBindedIndex()

    log(f'[rebindAlias] : start, alias : {alias}, bindedIndexName : {bindedIndexName}, indexName : {indexName}, ')

    requestBody = {
        'actions': []
    }

    if (bindedIndexName != ''):
        remove = {
            'remove': {
                'alias': alias,
                'index': bindedIndexName
            }
        }
        requestBody['actions'].append(remove)

    add = {
        'add': {
            'alias': alias,
            'index': indexName
        }
    }
    requestBody['actions'].append(add)

    if (_usable['elasticsearch'] == False or _config['action'] != 'create' or alias == indexName):
        log(json.dumps(requestBody))
        return ''    

    response = requests.post('_aliases', auth=_awsauth, data=requestBody, headers=_elasticsearch['headers'])
    response.raise_for_status()    
    return response.text


_event = {
  "Records": [
    {
      "eventVersion": "2.0",
      "eventSource": "aws:s3",
      "awsRegion": "ap-northeast-2",
      "eventTime": "1970-01-01T00:00:00.000Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "EXAMPLE"
      },
      "requestParameters": {
        "sourceIPAddress": "127.0.0.1"
      },
      "responseElements": {
        "x-amz-request-id": "EXAMPLE123456789",
        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "testConfigRule",
        "bucket": {
          "name": "musinsa-search",
          "ownerIdentity": {
            "principalId": "EXAMPLE"
          },
          "arn": "arn:aws:s3:::musinsa-search"
        },
        "object": {
          "key": "service-cluster-data/crayon13/devel/20201201000000.create.csv",
          "size": 1024,
          "eTag": "0123456789abcdef0123456789abcdef",
          "sequencer": "0A1B2C3D4E5F678901"
        }
      }
    }
  ]
}

_fileConfig = {
    "fileFieldDelemeter": ",", 
    "indexMappings" :{
        "mappings": {
            "properties": {
                "no": {
                    "type": "keyword"
                },
                "nickname": {
                    "type": "keyword"
                }
            }
        }
    }
}

# lambda_handler(_event, '')