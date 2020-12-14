import boto3
import json
import requests
from requests_aws4auth import AWS4Auth

region = 'ap-northeast-2' # For example, us-west-1
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

url = 'https://search-musinsa-es-drmgnroc6jq6ibirp7lrkw65mm.ap-northeast-2.es.amazonaws.com/crayon13'
  
# Lambda execution starts here
def lambda_handler(event, context):

    # Put the user query into the query DSL for more accurate search results.
    # Note that certain fields are boosted (^).
    query = {
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0            
        },
        'mappings': {
            'properties': {
                'no': {
                    'type': 'keyword'
                },
                'nickname': {
                    'type': 'keyword'
                }
            }
        }
    }

    # ES 6.x requires an explicit Content-Type header
    headers = { "Content-Type": "application/json" }

    # Make the signed HTTP request
    r = requests.put(url, auth=awsauth, headers=headers, data=json.dumps(query))

    # Create the response and add some extra content to support CORS
    response = {
        "statusCode": r.status_code,
        "headers": {
            "Access-Control-Allow-Origin": '*'
        },
        "isBase64Encoded": False
    }

    # Add the search results to the response
    response['body'] = r.text
    return response