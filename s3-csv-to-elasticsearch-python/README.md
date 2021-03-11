## s3-csv-to-elasticsearch-python
#### process
1. csv file s3 put
2. s3 lambda file create trigger
3. AWS Elasticsearch 
    1. create index
    2. bulk indexing
    3. rebind alias
    4. old index delete

#### S3 & csv file rule
1. S3 path format
```
s3://{bucket}/{root}/{index or alias}/{profile}
s3://my-elasticsearch/indices/user/devel
```

2. file file format
```
{time:yyyyMMddHH24mmss}.{action}.csv
20201211000000.create.csv
20201212000000.update.csv
```

3. config.json
```
{
  "fileFieldDelimiter": ",",
  "fieldArrayDelimiter": "",
  "slack": {
    "webhookUrl": "",
    "channel": "",
    "create": {
      "start": "enable",
      "finish": "enable",
      "count": "enable",
      "error": "enable"
    },
    "update": {
      "start": "enable",
      "finish": "enable",
      "count": "enable",
      "error": "enable"
    }
  },
  "indexMappings": {
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
```
fileFieldDelimiter : csv file filed delimiter
fieldArrayDelimiter : csv file filed value to array delimiter
slack : slack alarm settings
 * create : full indexing event alarm settings
 * update : increment indexing event alarm settings
indexMappings : elsticsearch index mapping 

