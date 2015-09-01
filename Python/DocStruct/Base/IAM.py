# vim:fileencoding=utf-8:ts=2:sw=2:expandtab
import json

from boto3.core.exceptions import ServerError

#--------------------------------------------------
# IAM RELATED METHODS
#--------------------------------------------------


def GetPolicyStmtForTranscoders(commonbucketprefix, topic_arn, queue_arn):
  # http://docs.aws.amazon.com/elastictranscoder/latest/developerguide/security.html
  return json.dumps({
    "Statement": [{
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:Put*",
        "s3:Get*",
        "s3:*MultipartUpload*",
      ],
      "Resource": ["arn:aws:s3:::{0}*".format(commonbucketprefix)],
    }, {
      "Effect": "Allow",
      "Action": ["sns:Publish"],
      "Resource": [topic_arn],
    }, {
      "Effect": "Allow",
      "Action": ["sqs:*"],
      "Resource": [queue_arn],
    }, {
      "Effect": "Deny",
      "Action": [
        "s3:*Policy*",
        "sns:*Permission*",
        "sns:*Delete*",
        "s3:*Delete*",
        "sns:*Remove*",
      ],
      "Resource": ["*"],
    }]
    })


def GetPolicyStmtForTranscoderRole():
  return json.dumps({
    "Statement": [{
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": ["elastictranscoder.amazonaws.com"]
      },
    }]
    })


def GetPolicyStmtForUser(inputbucketname, outputbucketname):
  return json.dumps({
    "Statement": [{
      "Effect": "Allow",
      "Action": [
        "s3:DeleteObject",
        "s3:PutObject",
        "s3:PostObject",
        "s3:PutObjectAcl",
      ],
      "Resource": ["arn:aws:s3:::{0}/*".format(inputbucketname)]
    }, {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket",
      ],
      "Resource": ["arn:aws:s3:::{0}/*".format(outputbucketname)]
    }, {
      "Effect": "Allow",
      "Action": ["elastictranscoder:*"],
      "Resource": ["*"],
    }, {
      "Effect": "Allow",
      "Action": ["sqs:*"],
      "Resource": ["*"],
    }]
    })


def GetPolicyStmtForAppUser(inputbucketname, keyprefix, queuearn):
  return json.dumps({
    "Statement": [{
      "Effect": "Allow",
      "Action": [
        "s3:DeleteObject",
        "s3:PutObject",
        "s3:GetObject",
        "s3:PostObject",
        "s3:PutObjectAcl",
      ],
      "Resource": ["arn:aws:s3:::{0}/{1}/*".format(inputbucketname, keyprefix)],
    }, {
      "Effect": "Allow",
      "Action": [
        "sqs:*"
      ],
      "Resource": [queuearn],
    }]
  })


def SetupRoleWithPolicy(session, role_name, policy_name, policy_document):
  """Setup ACL for the bucket.

  Default should be:
  - Full access to root
  - ReadOnly access to app developers
  - Object level write access to app developers

  :param session: The session to use for AWS connections
  :type session: boto3.Session
  :param role_name: Name of the group to which we add the new policy
  :type role_name: str | unicode
  :param policy_name: Name to give this new policy
  :type policy_name: str | unicode
  :param policy_document: The role policy
  :type policy_document: str | unicode
  :return: The role
  :rtype: boto3.Resource
  """
  iamconn = session.connect_to('iam')
  Role = session.get_resource('iam', 'Role')
  role = Role(connection=iamconn, role_name=role_name)
  try:
    role.list_role_policies(role_name=role_name)
  except ServerError:
    # If role does not exist, we create it and add the policy
    Roles = session.get_collection("iam", "RoleCollection")
    roles = Roles(connection=iamconn)
    assume_role_policy_document = GetPolicyStmtForTranscoderRole()
    role = roles.create(role_name=role_name, assume_role_policy_document=assume_role_policy_document)
    # Now we put the role policy in place
    role.put_policy(
      role_name=role_name,
      policy_name=policy_name,
      policy_document=policy_document
      )
  # Returns the role setup to handle transcoder jobs
  return role


def GetOrCreateUser(session, username, policy_name, policy_document, withcredentials=True):
  """Gets the user identified by the passed in username

  :param session: Session to use for connections
  :type session: boto3.session.Session
  :param username: Username
  :type username: str
  :param policy_name: Name of the policy to use for access control for this user
  :type policy_name: str
  :param policy_document: The JSON document that describes user policy
  :type policy_document: str
  :param withcredentials: If True, credentials for this user will be updated
  :type withcredentials: bool
  :return: The newly created user or an existing user
  :rtype: boto3.core.resources.User
  """
  iamconn = session.connect_to("iam")
  User = session.get_resource("iam", "User")
  user = User(connection=iamconn, user_name=username)
  try:
    user.list_policies()
  except ServerError:
    Users = session.get_collection("iam", "UserCollection")
    users = Users(connection=iamconn)
    user = users.create(user_name=username)
  # Setup user
  user.put_policy(policy_name=policy_name, policy_document=policy_document)
  AddUserToGroup(session, user.user_name, group_name="Users")
  # Recreate accesskey for this user if required
  if withcredentials:
    # Prepare the access key collection class
    AccessKeys = session.get_collection("iam", "AccessKeyCollection")
    AccessKey = session.get_resource("iam", "AccessKey")
    accesskeys = AccessKeys(connection=iamconn)
    # We should delete old access keys since there is a quota for access keys (Normally 2)
    for accesskey in accesskeys.each(user_name=user.user_name):
      accesskeyobj = AccessKey(connection=iamconn)
      accesskeyobj.delete(user_name=user.user_name, access_key_id=accesskey.access_key_id)
    # Now we create access keys for this user
    accesskey = accesskeys.create(user_name=user.user_name)
    return user, accesskey
  return user, None


def SetupUserGroupPolicy(session, group_name, policy_name, policy_document):
  iamconn = session.connect_to('iam')
  Group = session.get_resource('iam', 'Group')
  # Get the group we want to update
  # NOTE: we are assuming that the group is already created. It need not have any permissions yet
  group = Group(connection=iamconn, group_name=group_name)
  # Now we put the role policy in place
  group.put_policy(
    group_name=group_name,
    policy_name=policy_name,
    policy_document=policy_document
    )
  # Returns the group that should be used in the app to do authenticated reads and writes
  return group


def AddUserToGroup(session, user_name, group_name="Users", must_exist=False):
  """Add the provided user to the group specified by group_name

  :param session: Boto3 session to use for connections
  :type session: boto3.session.Session
  :param user_name: UserName to add to group
  :type user_name: str
  :param group_name: GroupName to add user to
  :type group_name: str
  :param must_exist: Specify True, if we can just assume that the group exists
  :type must_exist: bool
  :return: The group the user was added to
  :rtype: boto3.core.resource.Group
  """
  iamconn = session.connect_to('iam')
  Group = session.get_resource('iam', 'Group')
  # Get the group we want to update
  # NOTE: we are assuming that the group is already created.
  group = Group(connection=iamconn, group_name=group_name)
  # Now we add the user to this group
  try:
    group.add_user(user_name=user_name)
  except ServerError:
    if must_exist:
      raise
    return None
  # Return the group
  return group
