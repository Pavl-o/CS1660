import boto3
import csv

s3 = boto3.resource('s3', 
    aws_access_key_id='', 
    aws_secret_access_key='' 
)

bucket = s3.Bucket("big-bucket-business")
bucket.Acl().put(ACL = 'public-read')

dyndb = boto3.resource('dynamodb', 
    region_name='us-west-2', 
    aws_access_key_id='', 
    aws_secret_access_key='' 
)

try: 
    table = dyndb.create_table( 
        TableName='DataTable', 
        KeySchema=[ 
            { 
                'AttributeName': 'PartitionKey', 
                'KeyType': 'HASH' 
            }, 
            { 
                'AttributeName': 'RowKey', 
                'KeyType': 'RANGE' 
            } 
        ], 
        AttributeDefinitions=[ 
            { 
                'AttributeName': 'PartitionKey', 
                'AttributeType': 'S' 
            }, 
            { 
                'AttributeName': 'RowKey', 
                'AttributeType': 'S' 
            }, 
 
        ], 
        ProvisionedThroughput={ 
            'ReadCapacityUnits': 5, 
            'WriteCapacityUnits': 5 
        } 
    ) 
except Exception as e: 
    print (e)  
    table = dyndb.Table("DataTable")

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

with open('experiments.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    itercsvf = iter(csvf)
    next(itercsvf) # have to do this to skip the first row with the column headers
    for item in itercsvf:
        print(item)
        body = open(item[5], 'rb')
        s3.Object('big-bucket-business', item[5]).put(Body=body) 
        md = s3.Object('big-bucket-business', item[5]).Acl().put(ACL='public-read') 
        url = "https://big-bucket-business.s3.us-east-2.amazonaws.com/"+item[5] 
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],  
                 'temp' : item[2],'conductivity' : item[3], 'concentration' : item[4], 'url':url} 
        try: 
            table.put_item(Item=metadata_item) 
        except: 
            print("item may already be there or another failure")

