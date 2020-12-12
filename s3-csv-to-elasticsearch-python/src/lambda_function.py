import boto3
import json
import requests
from requests_aws4auth import AWS4Auth

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

_s3Client = boto3.client('s3')
_credentials = boto3.Session().get_credentials()
_awsauth = AWS4Auth(_credentials.access_key
, _credentials.secret_key, _elasticsearch['region'], _elasticsearch['service']
, session_token=_credentials.token)

_configFileName = 'config.json'

_usable = {
    's3': False,
    'elasticsearch': False
}

_bulkQueue = {
    'queue': [],
    'maxQueuSize': 1000
}

_configValues = {
    'fromS3Key': ['root', 'index', 'profile', 'fileName'],
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
    'index': '',
    'realIndex': '',
    'profile': '',
    'fileName': '',
    'dataTime': '', 
    'action': ''    
}

def log(messge = ''):
    print('[INFO]', messge)

def setValue(dictionary = [], key = '', value = ''):
    if key in dictionary:
        dictionary[key] = value
    else:
        return dictionary[key]
        
def setConfigFromFile(configKeys = []):
    filePath = _config['path'] + _configFileName
    log(_config['s3Bucket'] + ':' + filePath)

    configByFile = {}
    if (_usable['s3']):
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
    if _config['action'] == 'create':
        indexSuffx = '-' + _config['dataTime']

    setValue(_config, 'realIndex', _config['index'] + indexSuffx)

    # set Config From config.json
    setConfigFromFile(['indexMappings', 'fileFieldDelemeter'])

    log(_config)


def createIndex():
    if (_config['action'] != 'create'):
        return
    
    indexName = _config['realIndex']
    log(f'[createIndex] : start, indexName : {indexName}')

    indexScheme = {}
    indexScheme['settings'] = _elasticsearch[_config['profile']]['indexSettings']['settings']
    indexScheme['mappings'] = _config['indexMappings']['mappings']

    if (_usable['elasticsearch'] == False):
        log(json.dumps(indexScheme))
        return

    response = requests.put(indexName, auth=_awsauth, data=json.dumps(indexScheme), headers=_elasticsearch['headers'])
    response.raise_for_status()

def bulk():
    indexName = _config['realIndex']
    log(f'[bulk] : start, indexName : {indexName}')

    if (_usable['s3'] == False):
        return

    s3Object = _s3Client.get_object(Bucket=_config['s3Bucket'], Key=_config['s3Key'])
    recordLine = s3Object["Body"].read()

    for line in recordLine.splitlines():
        record = line.decode('utf-8')
        log(record)


    


    return


def rebindAlias():
    indexName = _config['realIndex']
    log(f'[rebindAlias] : start, indexName : {indexName}')
    
    return


def lambda_handler(event, context):
    initConfig(event)
    createIndex()
    bulk()
    rebindAlias()

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

lambda_handler(_event, '')