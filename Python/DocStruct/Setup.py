# vim:encoding=utf-8:ts=2:sw=2:expandtab
# -*- coding: utf-8 -*-
import json

from collections import namedtuple
from base64 import b64encode
from .Base import GetSession, S3, ElasticTranscoder, SNS, SQS, EC2, IAM, CloudFront
from .Config import ApplicationConfig, EnvironmentConfig


def LaunchInstances(*, AMI, EnvironmentConfig, NumInstances=1):
  """Launches <NumInstances> number of instances

  :param AMI: The AMI to use for launching instances
  :type AMI: str
  :param EnvironmentConfig: Configuration for the environment these instances are to be launced in
  :type EnvironmentConfig: DocStruct.Config.EnvironmentConfig
  :param NumInstances: Number of instances to start
  :type NumInstances: int
  :return: The IDs of the instances started
  :rtype: list
  """
  envid = EnvironmentConfig.EnvironmentID
  # Build the PEM file if it does not exist yet
  keyname = EnvironmentConfig.EC2_KeyName
  if not keyname:
    key = EC2.CreateKey(session=EnvironmentConfig.Session, name=envid)
    EnvironmentConfig.EC2_KeyName = key['KeyName']
    EnvironmentConfig.EC2_KeyMaterial = key['KeyMaterial']
    EnvironmentConfig.Save()

  # Prepare data to be passed to instances
  userdata = b64encode(json.dumps({
    "EnvironmentID": envid,
    "AccessKey": EnvironmentConfig.User_AccessKey,
    "SecretKey": EnvironmentConfig.User_SecretKey,
    }).encode('utf-8')).decode('utf-8')

  # Launch
  ret = [EC2.StartInstance(session=EnvironmentConfig.Session, imageid=AMI, keyname=keyname, userdata=userdata) for i in range(NumInstances)]
  instance_ids = [i['InstanceId'] for i in ret]
  EC2.TagInstances(session=EnvironmentConfig.Session, instance_ids=instance_ids, tags=[
    {'Key': 'EnvironmentID', 'Value': envid},
    {'Key': 'Name', 'Value': envid},
    ])
  return ret


def SetupEnvironment(*, CredsFilePath, EnvironmentID, WithDistribution=False):
  """Sets up the environment per the new specs.

  NOTE: this environment is for global usage.
  """
  # Get a session to use for AWS API access
  session = GetSession(CredsFilePath=CredsFilePath)
  # Get bucket class
  inputbucket = S3.GetOrCreateBuckets(session, EnvironmentID)
  S3.SetBucketCorsPolicy(inputbucket)
  # upload the crossdomain.xml and clientaccesspolicy.xml files into the bucket
  S3.SetupBucketForFlashAndSilverlight(session, inputbucket.bucket)
  # Create a CloudFront distribution for serving files from inputbucket
  if WithDistribution:
    distresp = CloudFront.CreateDistributionForBucket(Session=session, BucketName=inputbucket.bucket)
  # Create SQS queue for environment
  qurl = SQS.CreateQueue(session, EnvironmentID)
  # NOTE: since we only return the qurl, we need a way to convert the URL to an ARN
  # TODO: at some point we need to look at getting the ARN directly from the API
  qarn = SQS.ConvertURLToArn(qurl)
  # Create SNS topic so that the pipeline can publish notifications
  topic = SNS.CreateTopic(session=session, topicname=EnvironmentID)
  # Create a pipeline for transcoding videos
  policyname = "Transcoder-Policy-{0}".format(EnvironmentID)
  transcodername = "Transcoder-{0}".format(EnvironmentID)
  role = IAM.SetupRoleWithPolicy(
    session,
    transcodername,
    policyname,
    IAM.GetPolicyStmtForTranscoders(EnvironmentID, topic.topic_arn, qarn)
    )
  roledict = role.get(role_name=role.role_name)
  role_arn = roledict["Role"]["Arn"]
  # Create a pipeline to handle input from <inputbucket> and leave output in <inputbucket>
  pipeline = ElasticTranscoder.CreatePipeline(
    session=session,
    pipelinename="{0}-Transcoding".format(EnvironmentID),
    role_arn=role_arn,
    inputbucketname=inputbucket.bucket,
    outputbucketname=inputbucket.bucket,
    topic_arn=topic.topic_arn
    )
  pipelinedict = pipeline.get()
  pipeline_arn = pipelinedict["Pipeline"]["Arn"]
  # While we're at it, lets get the web preset and save it as in our config
  web_presetarn = ElasticTranscoder.GetPresetWithName(session=session, presetname="System preset: Web")
  # Create a preset to convert files to webm format
  webm_presetarn = ElasticTranscoder.GetPresetWithName(session=session, presetname="User preset: Webm")
  if not webm_presetarn:
    webm_presetarn = ElasticTranscoder.CreatePreset(session=session, presetdata=ElasticTranscoder.WEBM_PRESET_DATA)
  # Get Audio Presets
  mp3_presetarn = ElasticTranscoder.GetPresetWithName(session=session, presetname="System preset: Audio MP3 - 320k")
  # We can subscribe to the SNS topic using the SQS queue so that elastic transcoder
  # notifications are handled by the same jobs processing server
  SNS.CreateSQSQueueSubscription(session=session, queuearn=qarn, topicarn=topic.topic_arn)
  # # We also need to add a permission for the queue so that SNS is able to send messages to this queue
  # SQS.AddPermissionForSNSTopic(session, topic.topic_arn, qurl)
  # Create a user that EC2 will use
  user, credentials = IAM.GetOrCreateUser(
    session,
    EnvironmentID,
    "User-Policy-{0}".format(EnvironmentID),
    IAM.GetPolicyStmtForUser(inputbucket.bucket, inputbucket.bucket),
    )
  usermeta = user.get()
  # Save the environment config so that when we start instances, we can pass the config to it as well
  config = EnvironmentConfig(CredsFilePath=session, EnvironmentID=EnvironmentID)
  # Set the user credentials
  config.User_Arn = usermeta["User"]["Arn"]
  config.User_Username = credentials.user_name
  config.User_AccessKey = credentials.access_key_id
  config.User_SecretKey = credentials.secret_access_key
  # Set the elastic transcoder config
  config.ElasticTranscoder_RoleArn = role_arn
  config.ElasticTranscoder_PipelineArn = pipeline_arn
  config.ElasticTranscoder_TopicArn = topic.topic_arn
  config.ElasticTranscoder_WebPresetArn = web_presetarn
  config.ElasticTranscoder_WebmPresetArn = webm_presetarn
  config.ElasticTranscoder_MP3PresetArn = mp3_presetarn
  # Set the S3 config
  config.S3_InputBucket = inputbucket.bucket
  config.S3_OutputBucket = inputbucket.bucket
  # Set SQS config
  config.SQS_QueueUrl = qurl
  # Now we can save the config file
  return config.Save()


def SetupApplication(*, CredsFilePath, EnvironmentID, ApplicationID, GlobalConfig=None):
  """Sets up an application within an environment."""
  # Get a session to use for AWS API access
  session = GetSession(CredsFilePath=CredsFilePath)
  # If global config was not passed in, we can fetch it.
  if not GlobalConfig:
    GlobalConfig = EnvironmentConfig(CredsFilePath=session, EnvironmentID=EnvironmentID)
    if not GlobalConfig.S3_InputBucket:
      raise Exception("Global environment with name {0} is not available".format(EnvironmentID))
  # Now we can create a user for this project with just the right permissions
  # NOTE: http://blogs.aws.amazon.com/security/post/Tx1P2T3LFXXCNB5/Writing-IAM-policies-Grant-access-to-user-specific-folders-in-an-Amazon-S3-bucke
  user, credentials = IAM.GetOrCreateUser(
    session,
    "{0}-{1}".format(EnvironmentID, ApplicationID),
    "User-Policy-{0}-{1}".format(EnvironmentID, ApplicationID),
    IAM.GetPolicyStmtForAppUser(GlobalConfig.S3_InputBucket, ApplicationID, SQS.ConvertURLToArn(GlobalConfig.SQS_QueueUrl)),
    )
  usermeta = user.get()
  # Save the application config
  config = ApplicationConfig(CredsFilePath=session, EnvironmentID=EnvironmentID, ApplicationID=ApplicationID)
  # Set the user credentials
  config.User_Arn = usermeta["User"]["Arn"]
  config.User_Username = credentials.user_name
  config.User_AccessKey = credentials.access_key_id
  config.User_SecretKey = credentials.secret_access_key
  # Now we can save the config file
  return config.Save()


def KillEnvironment(*, CredsFilePath, EnvironmentID, GlobalConfig=None):
  # TODO
  pass


def KillApplication(*, CredsFilePath, EnvironmentID, ApplicationID, GlobalConfig=None):
  # TODO
  # Delete the app specific user
  # Move the folder with <app_id> prefix into <app_id>_uuid
  pass
