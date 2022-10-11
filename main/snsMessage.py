import boto3

sqs = boto3.client('sqs')
messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=20)


''''
put in dev ops tickets to create 2 SQS topics



'''