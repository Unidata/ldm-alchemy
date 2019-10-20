# Copyright (c) 2018 University Corporation for Atmospheric Research/Unidata.
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT
import boto3

sns = boto3.client('sns')
topic = boto3.Session().resource('sns').Topic('arn:aws:sns:us-east-1:684042711724:NewNEXRADLevel2ObjectFilterable')

sqs = boto3.client('sqs')
queue_url = sqs.create_queue(QueueName='test-queue')['QueueUrl']
queue = boto3.Session().resource('sqs').Queue(queue_url)

allow_policy = '''{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Sid":"MySQSPolicy001",
      "Effect":"Allow",
      "Principal": "*",
      "Action":"SQS:SendMessage",
      "Resource":"%s"
    }
  ]
}''' % (queue.attributes['QueueArn'])

sub = topic.subscribe(Protocol='sqs', Endpoint=queue.attributes['QueueArn'])
sub.set_attributes(AttributeName='FilterPolicy',
                   AttributeValue='{"SiteID": ["KEVX"]}')
queue.set_attributes(Attributes={'Policy': allow_policy})

try:
    while True:
        msgs = queue.receive_messages(WaitTimeSeconds=10)
        if msgs is None:
            break
        for msg in msgs:
            print(msg.body)
            msg.delete()
finally:
    queue.purge()
    sns.unsubscribe(SubscriptionArn=sub.attributes['SubscriptionArn'])
    sqs.delete_queue(QueueUrl=queue_url)

