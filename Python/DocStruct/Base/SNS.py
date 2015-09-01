# vim:fileencoding=utf-8:ts=2:sw=2:expandtab
#--------------------------------------------------
# SNS RELATED METHODS
#--------------------------------------------------


def CreateTopic(*, session, topicname):
  """Create SNS topic to which notifications will be published from the pipeline

  :param session: Session to use
  :type session: boto3.Session
  :param topicname: Topic to which the notifications will be published
  :type topicname: string
  :return: Newly created topic
  :rtype: boto3.Resource
  """
  snsconn = session.connect_to("sns")
  Topics = session.get_collection("sns", "TopicCollection")
  topics = Topics(connection=snsconn)
  topic = topics.create(name=topicname)
  # We return the topic so that subscriptions can be created
  return topic


def CreateSQSQueueSubscription(*, session, queuearn, topicarn):
  """Use SQS queue to subscribe to the SNS topic

  :param session: Session to use for AWS access
  :type session: boto3.session.Session
  :param queuearn: ARN of the queue
  :type queuearn: str
  :param topicarn: ARN of the SNS topic to subscribe to
  :type topicarn: str
  :return: A subscription object confirming the subscription
  :rtype: boto3.sns.Subscription
  """
  # Create subscriptions
  snsconn = session.connect_to("sns")
  Subscriptions = session.get_collection("sns", "SubscriptionCollection")
  subscriptions = Subscriptions(connection=snsconn)
  subscription = subscriptions.create(topic_arn=topicarn,
                                      protocol="sqs",
                                      notification_endpoint=queuearn)
  # the endpoint will need to confirm the subscription
  return subscription
