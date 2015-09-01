# vim:fileencoding=utf-8:ts=2:sw=2:expandtab
import json

from uuid import uuid4
from urllib import parse
from boto3.core.exceptions import ServerError


def CreateQueue(session, queuename, message_retention_period=1209600, visibility_timeout=60):
  """Creates a queue with name

  :param session: The session to use for AWS requests
  :type session: boto3.session.Session
  :param queuename: Name of the queue being created
  :type queuename: str
  :param message_retention_period: Number of seconds the message will be retained for (DEFAULT: 14 days - AWS max)
  :type message_retention_period: int
  :param visibility_timeout: Timeout in seconds for which a received message will be invisible to other receivers in the queue
  :type visibility_timeout: int
  :return: The URL of the queue
  :rtype: str
  """
  sqsconn = session.connect_to("sqs")
  Queue = session.get_resource("sqs", "Queue")
  q = Queue(connection=sqsconn)
  try:
    qmeta = q.get(queue_name=queuename)
  except ServerError:
    Queues = session.get_collection("sqs", "QueueCollection")
    queues = Queues(connection=sqsconn)
    q = queues.create(queue_name=queuename, attributes={
      "MessageRetentionPeriod": "%d" % message_retention_period,
      "VisibilityTimeout": "%d" % visibility_timeout,
    })
    qmeta = q.get(queue_name=queuename)
  return qmeta["QueueUrl"]


def PostMessage(session, queueurl, message):
  """Post a message to the given queue

  :param session: Session to use for AWS access
  :type session: boto3.session.Session
  :param queueurl: URL of the queue to which we will post messages
  :type queueurl: str
  :param message: Body of message to post
  :type message: str
  :return: The created message
  :rtype: object
  """
  sqsconn = session.connect_to("sqs")
  Messages = session.get_collection("sqs", "MessageCollection")
  messages = Messages(connection=sqsconn, queue_url=queueurl)
  m = messages.create(message_body=message)
  return m


def GetMessageFromQueue(session, queueurl, delete_after_receive=False):
  """Get message from the queue to process

  :param session: Session to use for AWS access
  :type session: boto3.session.Session
  :param queueurl: URL of the queue from which to receive messages
  :type queueurl: str
  :param delete_after_receive: If True, the message will be deleted immediately after receipt
  :type delete_after_receive: bool
  :return: A tuple consisting of (The message body, The message receipt handle)
  :rtype: tuple
  """
  sqsconn = session.connect_to("sqs")
  Messages = session.get_collection("sqs", "MessageCollection")
  messages = Messages(connection=sqsconn, queue_url=queueurl)
  # Try to get 1 message at a time by waiting at most 20 seconds
  for m in messages.each(wait_time_seconds=20, max_number_of_messages=1):
    if delete_after_receive:
      m.delete(queue_url=queueurl, receipt_handle=m.receipt_handle)
    return m.body, m.receipt_handle
  return None, None


def ConvertURLToArn(url):
  parsed = parse.urlparse(url)
  # regionstr = parsed.netloc.replace(".amazonaws.com", "").replace(".", ":")
  regionstr = "us-east-1"
  specificstr = parsed.path.replace("/", ":")
  return "arn:aws:sqs:" + regionstr + specificstr


def AddPermissionForSNSTopic(session, topicarn, qurl):
  sqsconn = session.connect_to("sqs")
  Queue = session.get_resource("sqs", "Queue")
  q = Queue(connection=sqsconn, queue_url=qurl)
  qarn = ConvertURLToArn(qurl)
  policy = json.dumps({
    "Version": "2014-09-24",
    "Id": uuid4().hex,
    "Statement": [{
      "Sid": "AllowSNSToSendMessageToSQS",
      "Effect": "Allow",
      "Principal": {"AWS": "*"},
      "Action": "SQS:SendMessage",
      "Resource": qarn,
      "Condition": {
        "StringEquals": {
          "aws:SourceArn": topicarn,
        }
      }
    }]
  })
  #return q.set_attributes(attributes={"Policy": policy})
  return q.set_attributes(attributes={"Policy": parse.quote_plus(policy, safe="*").replace("us-east-1", "us%2Deast%2D1")})
