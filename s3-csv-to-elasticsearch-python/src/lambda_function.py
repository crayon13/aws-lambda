import boto3
import json
import requests
from requests_aws4auth import AWS4Auth
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

###########################################################################
# variable
###########################################################################
_configFileName = 'config.json'

_usable = {
    's3': True,
    'elasticsearch': True,
    'slack': True
}

_elasticsearch = {
    'devel': {
        'endpoint': 'https://my-search-endpoint.devel.amazonaws.com/',
        'Authorization': '{base64(id:password)}',
        'indexSettings': {
            'settings': {
                'number_of_shards': 1,
                'number_of_replicas': 0,
                'refresh_interval': '3s',
                'index.max_ngram_diff': 30
            }
        }
    },
    'prod': {
        'endpoint': 'https://my-search-endpoint.product.amazonaws.com/',
        'Authorization': '{base64(id:password)}',
        'indexSettings': {
            'settings': {
                'number_of_shards': 3,
                'number_of_replicas': 1,
                'refresh_interval': '3s',
                'index.max_ngram_diff': 30
            }
        }
    },
    'region': 'ap-northeast-2',
    'service': 'es',
    'doctype': '_doc',
    'headers': {
        'Content-Type': 'application/x-ndjson; charset=utf-8',
        'Authorization': ''
    }
}

_bulkQueue = {
    'queue': [],
    'maxQueueSize': 1000,
    'addedDocumentCount': 0,
    'totalDocumentCount': 0
}

_configValues = {
    'fromS3Key': ['root', 'profile', 'alias', 'fileName'],
    'fromFileName': ['dataTime', 'action']
}

_config = {
    's3Bucket': '',
    's3Key': '',
    'slack': {},
    'indexMappings': {},
    'indexAnalysis': {},
    'fileFieldDelimiter': '',
    'fieldArrayDelimiter': '',
    'indexFieldNames': [],
    'root': '',
    'path': '',
    'alias': '',
    'realIndex': '',
    'profile': '',
    'fileName': '',
    'dataTime': '',
    'action': '',
    'notDeleteIndicies': []
}

_backupPath = '/backup-index-data-files'

_s3Client = boto3.client('s3')
_credentials = boto3.Session().get_credentials()
_awsauth = AWS4Auth(_credentials.access_key
                    , _credentials.secret_key, _elasticsearch['region'], _elasticsearch['service']
                    , session_token=_credentials.token)

###########################################################################
# Handler
###########################################################################
def lambda_handler(event, context):
    try:
        initConfig(event)
        setElasticsearchRequestHeader()
        createIndex()
        bulk()
        rebindAlias()
        deleteOldIndcies()
        moveS3Object()
    except HTTPError as e:
        log("HTTPError : " + str(e))
        sendMessage('error', 'HTTPError 에러가 발생되었습니다.\n' + str(e))
    except Exception as e:
        log("Exception : "+ str(e))
        sendMessage('error', '에러가 발생되었습니다.\n' + str(e))
    else:
        sendMessage('finish', '색인이 종료되었습니다.')

###########################################################################
# function - common
###########################################################################
def sendMessage(step, message):
    postToSlack('{slack hook url}', '{slack channel}', message);

    action = _config['action']

    if 'webhookUrl' not in _config['slack']:
        return ''
    if _config['slack']['webhookUrl'] == '':
        return ''
    if step == 'error':
        postToSlack(_config['slack']['webhookUrl'], _config['slack']['channel'], message);
        return ''
    if 'receive' in _config['slack'] and _config['slack']['receive'][action][step] == 'disable':
        return ''

    postToSlack(_config['slack']['webhookUrl'], _config['slack']['channel'], message);

def postToSlack(webhookUrl, channel, message):

    if (_usable['slack'] == False):
        return 'slack usable : False'

    slack_message = {
        'channel': channel,
        'username': 'elsticsearch service indexer',
        'text': '색인진행알람',
        'blocks': [
            {
                'type': 'section',
                'text': {
                    'type': 'mrkdwn',
                    'text': '*색인대상*:\n' + _config['s3Key']
                }
            },{
                'type': 'section',
                'text': {
                    'type': 'mrkdwn',
                    'text': '*상태*:\n' + _config['action']
                }

            },{
                'type': 'section',
                'text': {
                    'type': 'mrkdwn',
                    'text': '*메시지*:\n' + message
                }
            }]
    }

    req = Request(webhookUrl, json.dumps(slack_message).encode('utf-8'))
    try:
        response = urlopen(req)
        response.read()
        log("Message posted to " + slack_message['channel'])
    except HTTPError as e:
        log("Request failed: " + str(e.code) + " "+ e.reason)
    except URLError as e:
        log("Server connection failed:" + e.reason)

def log(messge = ''):
    print('[INFO]', messge)

def setValue(dictionary = [], key = '', value = ''):
    if key in dictionary:
        dictionary[key] = value
    else:
        dictionary[key]

def isNotCreateIndex():
    return (_config['action'] != 'create')

###########################################################################
# function - config
###########################################################################
def setConfigFromFile(configKeys = []):
    filePath = _config['path'] + _configFileName
    log('[setConfigFromFile] ' + _config['s3Bucket'] + ':' + filePath)

    configByFile = {}
    if _usable['s3']:
        # S3에서 파일을 읽어오는 것으로
        s3Object = _s3Client.get_object(Bucket=_config['s3Bucket'], Key=filePath)
        jsonString = s3Object["Body"].read().decode('utf-8')
        configByFile = json.loads(jsonString)

    for key in configKeys:
        if key not in configByFile:
            continue;
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

    fileNameValidate(indexInfo)

    keySeq = 0
    for key in _configValues['fromFileName']:
        setValue(_config, key, indexInfo[keySeq])
        keySeq = keySeq + 1

    # set real index name
    indexSuffx = ''
    if (_config['action'] == 'create'):
        indexSuffx = '-' + _config['dataTime']

    setValue(_config, 'realIndex', _config['alias'] + indexSuffx)

    configPropertyValidate()

    # set Config From config.json
    setConfigFromFile(['slack','fileFieldDelimiter','indexMappings', 'indexAnalysis', 'fieldArrayDelimiter'])

    log('[_config] ' + json.dumps(_config))

    sendMessage('start', '색인이 시작되었습니다.')

def setElasticsearchRequestHeader():
    profile = _config['profile']
    authorizationValue = 'Basic ' + _elasticsearch[profile]['Authorization']
    setValue(_elasticsearch['headers'], 'Authorization', authorizationValue)
    log('[elasticsearchRequestHeader] ' + json.dumps(_elasticsearch['headers']))


def makeElasticsearchUrl(path = ''):
    profile = _config['profile']
    endPoint = _elasticsearch[profile]['endpoint']
    return f'{endPoint}{path}'


###########################################################################
# function - csv file & bulk Queue
###########################################################################
def fileNameValidate(indexInfo = []):
    fileNameFormatLength = len(indexInfo)
    fileName = _config['fileName']

    if (fileNameFormatLength < 3):
        raise Exception(f'[fileNameValidate] Please check s3 data filename : {fileName}. e.g., 20210222113500.create.csv')

def configPropertyValidate():
    action = _config['action']

    if (action != 'create' and action !='update'):
        raise Exception(f'[configPropertyValidate] Please check action value : {action}')


def headerValidate(fields = []):
    properties = _config['indexMappings']['mappings']['properties']

    for field in fields:
        properties[field]


def fieldsValidate(fields = []):
    fieldsLength = len(fields)
    headersLength = len(_config['indexFieldNames'])

    if (fieldsLength != headersLength):
        raise Exception(f'[fieldsValidate] fieldCount Not Equals Headers, headersLength : {headersLength}, fieldsLength : {fieldsLength}')


def isFullBulkQueue():
    return _bulkQueue['maxQueueSize'] <= _bulkQueue['addedDocumentCount']


def isEmptyBulkQueue():
    return len(_bulkQueue['queue']) == 0


def addBulkQueue(dictionary):
    _bulkQueue['queue'].append(json.dumps(dictionary))


def clearBulkQueue():
    _bulkQueue['addedDocumentCount'] = 0
    _bulkQueue['queue'].clear()

def increaseAddedDocumentCount():
    setValue(_bulkQueue, 'addedDocumentCount', _bulkQueue['addedDocumentCount'] + 1)
    increaseTotalDocumentCount()

def increaseTotalDocumentCount():
    setValue(_bulkQueue, 'totalDocumentCount', _bulkQueue['totalDocumentCount'] + 1)

def makeBulkJsonAndAddQueue(fields = []):
    fieldsValidate(fields)

    id = fields[0]
    header = {'index': {'_index': _config['realIndex'], '_id': id}}
    body = {}

    for fieldSeq in range(len(fields)):
        fieldName = _config['indexFieldNames'][fieldSeq]
        if _config['fieldArrayDelimiter'] != '' and _config['fieldArrayDelimiter'] in fields[fieldSeq]:
            body[fieldName] = fields[fieldSeq].split(_config['fieldArrayDelimiter'])
        else:
            body[fieldName] = fields[fieldSeq]

    addBulkQueue(header)
    addBulkQueue(body)
    increaseAddedDocumentCount()


def makeRequestBodyByBulkQueueAndClear():
    totalDocumentCount = _bulkQueue['totalDocumentCount']
    log(f'[totalDocumentCount] {totalDocumentCount}')

    data = '\n'.join(_bulkQueue['queue']) + '\n'
    clearBulkQueue()
    return data

###########################################################################
# function - Elasticsearch
###########################################################################
def createIndex():
    alias = _config['alias']
    indexName = _config['realIndex']

    indexScheme = {}
    indexScheme['mappings'] = _config['indexMappings']['mappings']
    indexScheme['settings'] = _elasticsearch[_config['profile']]['indexSettings']['settings']

    if 'analysis' in _config['indexAnalysis']:
        indexScheme['settings']['analysis'] = _config['indexAnalysis']['analysis']

    if (isNotCreateIndex() or alias == indexName):
        log(json.dumps(indexScheme))
        return

    log(f'[createIndex] : start, indexName : {indexName}')

    url = makeElasticsearchUrl(indexName)
    response = requests.put(url, data=json.dumps(indexScheme), headers=_elasticsearch['headers'])
    log(f'[createIndex] {url}, ' + response.text)
    response.raise_for_status()
    return response.text

def postForBulk():
    if isEmptyBulkQueue():
        return 'bulk queue is empty'

    requestBody = makeRequestBodyByBulkQueueAndClear()

    if (_usable['elasticsearch'] == False):
        return 'elasticsearch usable : False'

    response = requests.post(makeElasticsearchUrl('_bulk'), data=requestBody, headers=_elasticsearch['headers'])
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

        fields = record.split(_config['fileFieldDelimiter'])

        if (recordCount == 1):
            headerValidate(fields)
            setValue(_config, 'indexFieldNames', fields)
        else:
            makeBulkJsonAndAddQueue(fields)

            if isFullBulkQueue():
                postForBulk()

    postForBulk()
    sendMessage('count', '데이터 ' + str(recordCount-1) + '건이 등록되었습니다.')

def getAliasBindedIndex():
    if (_usable['elasticsearch'] == False):
        return ''

    alias = _config['alias']

    url = makeElasticsearchUrl(f'_cat/aliases/{alias}?format=json')
    response = requests.get(url, headers=_elasticsearch['headers'])
    log(f'[getAliasBindedIndex] {url}, ' + response.text)
    response.raise_for_status()

    bindedIndices = json.loads(response.text)

    if (len(bindedIndices) > 1):
        raise Exception(f'{alias} is multi indices binded')
    elif (len(bindedIndices) == 0):
        return ''

    return bindedIndices[0]['index']

def setNotDeleteIndicies(indices = []):
    setValue(_config, 'notDeleteIndicies', indices)

def rebindAlias():
    alias = _config['alias']
    indexName = _config['realIndex']
    bindedIndexName = getAliasBindedIndex()

    if (isNotCreateIndex() or alias == indexName):
        return ''

    setNotDeleteIndicies([indexName, bindedIndexName])

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

    log('rebindAlias ' + json.dumps(requestBody))

    url = makeElasticsearchUrl('_aliases')
    response = requests.post(url, data=json.dumps(requestBody), headers=_elasticsearch['headers'])
    log(f'[rebindAlias] {url}, ' + response.text)
    response.raise_for_status()
    return response.text

def deleteOldIndcies():
    alias = _config['alias']
    notDeleteIndicies = _config['notDeleteIndicies']
    notDeleteIndiciesCount = 0

    for index in notDeleteIndicies:
        if (index != ''):
            notDeleteIndiciesCount = notDeleteIndiciesCount + 1

    if (isNotCreateIndex() or notDeleteIndiciesCount == 0):
        return

    url = makeElasticsearchUrl(f'_cat/indices/{alias}-20*?format=json')
    response = requests.get(url, headers=_elasticsearch['headers'])
    log(f'[deleteOldIndicies] indicies {url}: ' + response.text)
    response.raise_for_status()

    indicies = json.loads(response.text)

    if (len(indicies) == 0):
        return

    deleteIndicies = [index['index'] for index in indicies if index['index'] not in notDeleteIndicies]

    if (len(deleteIndicies) == 0):
        return

    url = makeElasticsearchUrl(','.join(deleteIndicies))
    response = requests.delete(url, headers=_elasticsearch['headers'])
    log(f'[deleteOldIndicies] delete idicies : {url}, ' + response.text)
    response.raise_for_status()
    return response.text

def moveS3Object():
    if (_usable['s3'] == False):
        return

    key = _config['root'] + _backupPath + '/' + _config['profile'] + '.' + _config['realIndex'] + '.' + _config['action'] + '.csv'

    _s3Client.copy_object(
        Bucket=_config['s3Bucket'],
        Key=key,
        CopySource={'Bucket': _config['s3Bucket'], 'Key': _config['s3Key']},
    )

    _s3Client.delete_object(Bucket=_config['s3Bucket'], Key=_config['s3Key'])

    log(f'[moveS3Object] delete s3Object : ' + _config['s3Key'])

    return '';