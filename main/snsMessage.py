# import boto3
#
# sqs = boto3.client('sqs')
# messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=20)

#
# ''''
# put in dev ops tickets to create 2 SQS topics
#
#
#
# '''
import os

import requests

with open("../fixtures/processFiles/bidrequest_MagniteBidRequestIpValidation") as f:
    # print("running test for {0}".format(fileNames))
    v = ""
    for line in f.readlines():
        v += line.replace('\n', '').replace(' ', '')
    headers = {'Content-type': 'application/json'}
    url = "http://127.0.0.1:8080/magnite/bidder"
    url = "http://bidder.coredev.west2.steelhouse.com/magnite/bidder"

    x = requests.post(url=url, data=v, headers= headers)
    print(x.status_code)
    print(x.text)