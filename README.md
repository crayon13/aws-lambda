# aws-lambda
## s3-csv-to-elasticsearch-python
#### process
1. csv file s3 put
2. s3 lambda file create trigger
3. AWS Elasticsearch 
    1. create index
    2. bulk indexing
    3. rebind alias
    4. old index delete