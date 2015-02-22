# vim:encoding=utf-8:ts=2:sw=2:expandtab
import os
from uuid import uuid4


def CreateKey(*, session, name):
  """Creates a key pair to be used when SSHing into EC2 instances

  :param session: Session to use for AWS communication
  :type session: boto3.session.Session
  :param name: Name of the key pair
  :type name: str
  :return: The new key pair
  :rtype: dict
  """
  ec2conn = session.connect_to("ec2")
  return ec2conn.create_key_pair(key_name=name)


def StartInstance(*, session, imageid, keyname="", instancetype="t2.micro", userdata=""):
  """Starts an instance of specified imageid

  NOTE: when this function returns, the instance is still NOT fully started. We may need to
  wait a few minutes before we can access the instance via public IP.

  :param session: Session to use for AWS access
  :type session: boto3.session.Session
  :param imageid: ID of the image to use for spawning instances
  :type imageid: str
  :param keyname: Name of the AWS access key that will be used to SSH into the instances
  :type keyname: str
  :param instancetype: Type of instance to start
  :type instancetype: str
  :param userdata: User data to send to the instance
  :type userdata: str
  :return: ID of instance that was started
  :rtype: str
  """
  ec2conn = session.connect_to("ec2")
  ret = ec2conn.run_instances(image_id=imageid, min_count=1, max_count=1, key_name=keyname, instance_type=instancetype, user_data=userdata)
  return ret["Instances"][0]


def TagInstances(*, session, instance_ids, tags):
  """Tag instances

  :param session: Session to use for AWS access
  :type session: boto3.session.Session
  :param instance_ids: List of instance IDs to tag
  :type instance_ids: list
  :param tags: The tags to create on the given resources
  :type tags: list
  :return: return value of create_tags method
  :rtype: any
  """
  ec2conn = session.connect_to("ec2")
  return ec2conn.create_tags(resources=instance_ids, tags=tags)


def StopInstance(*, session, instanceid):
  """Stops an instance identified by instance id.

  :param session: Session to use for AWS access
  :type session: boto3.session.Session
  :param instanceid: ID of instance to stop
  :type instanceid: str
  :return: True if all was successful (NOTE: for now this function always returns True)
  :rtype: bool
  """
  ec2conn = session.connect_to("ec2")
  ret = ec2.stop_instances(instance_ids=[instanceid,])
  return True


def ListInstances(*, session, environmentid, instanceid=None):
  ec2conn = session.connect_to("ec2")
  ret = ec2conn.describe_instances() or {}
  if not ret:
    return []

  # Define a filter function
  def filter_instance(inst):
    if not len(inst['Instances']):
      return False
    if not len(inst['Instances'][0].get('Tags', [])):
      return True
    for tag in inst['Instances'][0]['Tags']:
      if tag['Key'] == 'EnvironmentID' and tag['Value'] == environmentid:
        return True
    return False

  # Return
  return filter(filter_instance, ret.get('Reservations', [{'Instances': []}]))


def TerminateInstance(*, session, instanceid):
  """Terminates an instance identified by instance id.

  :param session: Session to use for AWS access
  :type session: boto3.session.Session
  :param instanceid: ID of instance to terminate
  :type instanceid: str
  :return: True if all was successful (NOTE: for now this function always returns True)
  :rtype: bool
  """
  ec2conn = session.connect_to("ec2")
  return ec2conn.terminate_instances(instance_ids=[instanceid,])
