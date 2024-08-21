import boto3
sqs = boto3.client('sqs', region_name='eu-west-1',
                   aws_access_key_id='AKIA467ST2DY3IRHUUWY',
                   aws_secret_access_key='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',
                   )

queue_url = 'https://sqs.eu-west-1.amazonaws.com/891177652465/Pooper'

# Send message to SQS queue
response = sqs.send_message(
    QueueUrl=queue_url,
    DelaySeconds=10,
    MessageAttributes={
        "Title": {
            "DataType": "String",
            "StringValue": "New Market"
        },
    },
    MessageBody=(
        str({"Topic": "New Market",
             "Body": { }
             }).replace("'", '"')
    )
)