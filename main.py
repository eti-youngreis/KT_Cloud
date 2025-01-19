import boto3

# sns = boto3.client('sns')

# topic_arn = 'arn:aws:sns:eu-west-1:390403884066:TestTopic'

# topic_arn = sns.create_topic(Name='TestTopic')['TopicArn']

# sns.subscribe(
#     TopicArn=topic_arn,
#     Protocol='email',
#     Endpoint='etiyoungreis@gmail.com'
# )

rds = boto3.client('rds')

# rds.create_event_subscription(
#     SnsTopicArn = topic_arn,
#     SubscriptionName = 'TestSubscription',
#     SourceType = 'db-instance',
#     SourceIds = ['test-db-instance'],
#     Enabled = True
# )
# rds.modify_event_subscription(
#     SubscriptionName = 'TestSubscription',
#     Enabled = True
# )
# rds.create_db_instance(
#     DBInstanceIdentifier = 'test-db-instance',
#     DBInstanceClass = 'db.t3.micro',
#     Engine = 'mysql',
#     MasterUsername = 'admin',
#     MasterUserPassword = 'admin123',
#     AllocatedStorage = 20
# )

# rds.delete_event_subscription(
#     SubscriptionName = 'TestSubscription'
# )

# rds.modify_event_subscription(
#     SubscriptionName='TestSubscription',
#     SourceType='db-instance',
#     Enabled = True,
#     SnsTopicArn = topic_arn
# )

# rds.add_source_identifier_to_subscription(
#     SubscriptionName = 'TestSubscription',
#     SourceIdentifier = 'test-db-instance'
# )
# print([s['CustSubscriptionId'] for s in rds.describe_event_subscriptions()['EventSubscriptionsList']])
# print([s['SourceType'] for s in rds.describe_event_subscriptions()['EventSubscriptionsList']])
# rds.delete_db_instance(
#     DBInstanceIdentifier = 'test-db-instance',
#     SkipFinalSnapshot = True,
# )
print([(i['DBInstanceIdentifier'], i['DBInstanceStatus'])
      for i in rds.describe_db_instances()['DBInstances']])

# rds.stop_db_instance(
#     DBInstanceIdentifier = 'test-db-instance'
# )

# rds.start_db_instance(
#     DBInstanceIdentifier = 'test-db-instance'
# )
# rds.delete_db_instance(
#     DBInstanceIdentifier = 'test-db-instance',
#     SkipFinalSnapshot = True,
# )
