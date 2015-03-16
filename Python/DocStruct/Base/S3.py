# vim:encoding=utf-8:ts=2:sw=2:expandtab
import json
import base64
import collections
import hmac
import time

from hashlib import sha256, sha1
from urllib import parse
from datetime import datetime, timedelta
from boto3.core.exceptions import ServerError
from botocore.auth import SigV4Auth


#--------------------------------------------------
# S3 RELATED METHODS
#--------------------------------------------------


AWS_TIME_FORMAT = '%Y%m%dT%H%M%SZ'
AWS_EXPIRES_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.000Z'


CROSSDOMAIN_XML = """<?xml version="1.0"?>
<!DOCTYPE cross-domain-policy SYSTEM
"http://www.macromedia.com/xml/dtds/cross-domain-policy.dtd">
<cross-domain-policy>
  <allow-access-from domain="*" secure="false" />
</cross-domain-policy>"""


CLIENTACCESSPOLICY_XML = """<?xml version="1.0" encoding="utf-8" ?>
<access-policy>
  <cross-domain-access>
    <policy>
      <allow-from http-request-headers="*">
        <domain uri="*"/>
      </allow-from>
      <grant-to>
        <resource path="/" include-subpaths="true"/>
      </grant-to>
    </policy>
  </cross-domain-access>
</access-policy>"""


XML_MAP = {
  "crossdomain.xml": CROSSDOMAIN_XML.encode("utf-8"),
  "clientaccesspolicy.xml": CLIENTACCESSPOLICY_XML.encode("utf-8"),
}


def GetBucketAndKeyFromArn(Arn):
  pth = Arn.split(":")[-1]
  splits = pth.split("/")
  bucket = splits[0]
  key = "/".join(splits[1:])
  return bucket, key


def GetBucketUrl(bucketname):
  return 'https://{0}.s3.amazonaws.com:443'.format(bucketname)


def GetOrCreateBuckets(session, *bucketnames):
  """Will create 2 buckets with the provided bucket names

  :param session: The boto3 session to use
  :type session: boto3.Session
  :param bucketnames: List of bucket names to create
  :type bucketnames: str ...
  :return: Tuple (<input bucket>, <output bucket>)
  :rtype: list
  """
  if len(bucketnames) == 0:
    return None
  s3conn = session.connect_to("s3")
  BucketCollection = session.get_collection("s3", "BucketCollection")
  Bucket = session.get_resource("s3", "Bucket")
  bc = BucketCollection(connection=s3conn)
  buckets = []
  for bucketname in bucketnames:
    bucket = Bucket(connection=s3conn, bucket=bucketname)
    try:
      bucket.get_acl()
    except ServerError:
      bucket = bc.create(bucket=bucketname)
      bucket._data["bucket"] = bucketname
      buckets.append(bucket)
  return buckets if len(buckets) > 1 else buckets[0]


def GetObjectPolicy(bucketname, expiretime, params):
  d = dict(params)
  # success_action_condition depends on what is available in params
  if d.get('success_action_redirect'):
    success_action_condition = {'success_action_redirect': d['success_action_redirect']}
  else:
    success_action_condition = {'success_action_status': d['success_action_status']}
  # Now we can prepare the policy
  policy = json.dumps(collections.OrderedDict([
    ("expiration", expiretime.strftime(AWS_EXPIRES_TIME_FORMAT)),
    ("conditions", [
      {"bucket": bucketname},
      {"key": d["key"]},
      {"acl": d["acl"]},
      success_action_condition,
      ["starts-with", "$Content-Type", ""],
      ["starts-with", "$Content-Disposition", ""],
      ["starts-with", "$x-amz-meta-filename", ""],
      ["starts-with", "$name", ""],
    ]),
  ]))
  if params.get("x-amz-algorithm"):
    policy["conditions"].extend([
      {"x-amz-credential": d["x-amz-credential"]},
      {"x-amz-algorithm": "AWS4-HMAC-SHA256"},
      {"x-amz-date": d["x-amz-date"]},
    ])
  return base64.b64encode(policy.strip().replace("\n", "").encode("utf-8"))


def SetBucketCorsPolicy(bucket, allowed_origins=('*',)):
  """Setup CORS rules for the bucket.

  :param bucket: The bucket on which we need to setup ACL
  :type bucket: boto3.S3Bucket
  :param allowed_origins: The allowed origins for the bucket CORS configurations
  :type allowed_origins: list | tuple
  :return: None
  :rtype: None
  """
  bucket.put_cors(cors_configuration={
    'CORSRules': [{
      'AllowedOrigins': allowed_origins,
      'AllowedMethods': ['GET', 'POST', 'HEAD', 'PUT'],
      'AllowedHeaders': ['*'],
    }]
  })
  # Returns the configured bucket
  return bucket


def SetupBucketForFlashAndSilverlight(session, bucketname):
  # Connect to S3 and see if the bucket already contains said files
  s3conn = session.connect_to("s3")
  Object = session.get_resource("s3", "S3Object")
  Objects = session.get_collection("s3", "S3ObjectCollection")
  objects = Objects(connection=s3conn, bucket=bucketname)
  # Loop over the two files
  for keyname in ("crossdomain.xml", "clientaccesspolicy.xml"):
    object = Object(connection=s3conn, bucket=bucketname, key=keyname)
    try:
      object.get_acl()
    except ServerError:
      # Get the file bodies
      objects.create(
        key=keyname,
        acl="public-read",
        content_type="text/xml; charset=utf-8;",
        body=XML_MAP[keyname]
      )
    print("Uploaded {0} to {1}".format(keyname, bucketname))


def GetSignedUrl(session, bucketname, key, expires=3600):
  """Returns a signed URL that can be used to access resource identified by <key>

  :param session: Session to use for connections
  :type session: boto3.session.Session
  :param bucketname: Bucketname in which this object resides
  :type bucketname: str
  :param key: Key idenitifying the resource within bucket
  :type key: str
  :param expires: The time (in seconds) this URL will be valid for. Default 1hour
  :type expires: int
  :return: The signed URL
  :rtype: str
  """
  baseurl = "{0}/{1}".format(GetBucketUrl(bucketname), key)
  creds = session.core_session.get_credentials()
  expireson = int(time.time()) + expires
  # Prepare string to sign
  strtosign = "GET\n\n\n{0}\n/{1}/{2}".format(
    expireson,
    bucketname,
    key
  )
  HMAC = hmac.new(
    creds.secret_key.strip().encode('utf-8'),
    strtosign.encode('utf-8'),
    digestmod=sha1
  )
  # Now we are ready for the signature
  signature = base64.b64encode(HMAC.digest())
  # Prepare the query string
  qstr = parse.urlencode([
    ("AWSAccessKeyId", creds.access_key.strip()),
    ("Expires", expireson),
    ("Signature", signature),
  ])
  # Return the URL
  return "{0}?{1}".format(baseurl, qstr)


def GetBaseFormParameters(bucketname, keyprefix, keyuuid, redirectto, contenttype, extra_params):
  # Decide what we need to do on successful upload
  success_tuple = ("success_action_status", "201")
  if redirectto:
    success_tuple = ("success_action_redirect", redirectto)
  # Decide contenttype tuple
  contenttype_tuple = ("Content-Type", contenttype)
  # Setup extra_params
  if not isinstance(extra_params, dict):
    extra_params = {}
  # Now we are ready to start
  ret = collections.OrderedDict()
  ret.update(extra_params)
  ret.update([
    ('key', '%s%s/input.dat' % (keyprefix, keyuuid)),
    success_tuple,
    contenttype_tuple,
    ('x-amz-meta-filename', "${filename}"),
  ])
  return ret


def GetFormParameters(session, bucketname, keyuuid, algo="HMAC-SHA256", redirectto="", contenttype="application/octet-stream", expiration=None, keyprefix=""):
  # Get credentials to use
  creds = session.core_session.get_credentials()
  # Setup expiration if required
  if expiration is None:
    now = datetime.utcnow()
    expiration = now + timedelta(seconds=1800)
  # Get the base policy elements
  if algo.startswith("HMAC"):
    extra = {"AWSAccessKeyId": creds.access_key.strip(), "acl": "authenticated-read"}
  else:
    extra = {}
  extra['Content-Disposition'] = 'attachment; filename="${filename}"'
  ret = GetBaseFormParameters(bucketname, keyprefix, keyuuid, redirectto, contenttype, extra)
  # Based on the algorithm requested, we will decide on policy
  if algo == "HMAC-SHA256":
    auth = SigV4Auth(creds, "s3", "us-east-1")
    # HACK: replace the timestamp so that we have control over the timestamp
    auth.timestamp = now.strftime(AWS_TIME_FORMAT)
    ret.update([
      ('x-amz-credential', auth.scope('')),
      ('x-amz-algorithm', 'AWS4-HMAC-SHA256'),
      ('x-amz-date', auth.timestamp),
    ])
    # Prepare the policy statement
    policy = GetObjectPolicy(bucketname, expiration, ret)
    signature = auth.signature(policy.decode("utf-8"))
    # Now add the policy statment and signature to the return dict
    ret["Policy"] = policy.decode("utf-8").replace("\n", "")
    ret["Signature"] = signature
  elif algo == "HMAC-SHA1":
    ret["bucket"] = bucketname
    ret.move_to_end("bucket", last=False)
    policy = GetObjectPolicy(bucketname, expiration, ret)
    signature = base64.b64encode(hmac.new(creds.secret_key.strip().encode("utf-8"), policy, digestmod=sha1).digest())
    ret["Policy"] = policy.decode("utf-8").replace("\n", "")
    ret["Signature"] = signature.decode("utf-8").replace("\n", "")
  return {
    "action": GetBucketUrl(bucketname),
    "policy": ret,
  }


def GetObject(*, session, bucket, key):
  """Get the object data as saved in S3

  :param session: The session to use for AWS access
  :type session: boto3.session.Session
  :param bucket: The bucket in which the object resides
  :type bucket: str
  :param key: The file to extract from S3
  :type key: str
  :return: The data saved at the given filename
  :rtype: bytes
  """
  s3conn = session.connect_to("s3")
  S3Object = session.get_resource("s3", "S3Object")
  o = S3Object(connection=s3conn, bucket=bucket, key=key)
  ret = None
  try:
    resp = o.get()
  except ServerError:
    pass
  else:
    if resp["ContentType"] == "application/xml":
      return None
    ret = resp["Body"].data
  return ret


def GetJSON(*, session, bucket, key):
  """Get the JSON saved in S3

  :param session: The session to use for AWS access
  :type session: boto3.session.Session
  :param bucket: The bucket in which the object resides
  :type bucket: str
  :param key: The file to extract from S3
  :type key: str
  :return: The JSON saved at the given filename
  :rtype: dict
  """
  obj = GetObject(session=session, bucket=bucket, key=key)
  if not obj:
    return None
  return json.loads(obj.decode('utf-8'))


def PutObject(*, session, bucket, key, content, type_="application/octet-stream"):
  """Saves data to S3 under specified filename and bucketname

  :param session: The session to use for AWS connection
  :type session: boto3.session.Session
  :param bucket: Name of bucket
  :type bucket: str
  :param key: Name of file
  :type key: str
  :param content: Data to save
  :type content: bytes | str
  :param type_: Content type of the data to put
  :type type_: str
  :return: The new S3 object
  :rtype: boto3.core.resource.S3Object
  """
  s3conn = session.connect_to("s3")
  # Make sure, we have the bucket to add object to
  try:
    b = GetOrCreateBuckets(session, bucket)
  except Exception as e:
    # There is a chance that the user trying to PutObject does not have permissions
    # to Create/List Buckets. In such cases and error is thrown. We can still try to
    # save and assume the bucket already exists.
    pass
  # Now we can create the object
  S3Objects = session.get_collection("s3", "S3ObjectCollection")
  s3objects = S3Objects(connection=s3conn, bucket=bucket, key=key)
  if isinstance(content, str):
    bindata = content.encode("utf-8")
  else:
    bindata = content
  # Now we create the object
  return s3objects.create(key=key, acl="private", content_type=type_, body=bindata)


def PutJSON(*, session, bucket, key, content):
  """Saves JSON to S3

  :param session: The session to use for AWS connection
  :type session: boto3.session.Session
  :param bucket: Name of bucket
  :type bucket: str
  :param key: Name of file
  :type key: str
  :param content: Data to save
  :type content: bytes | str
  :return: The new S3 object
  :rtype: boto3.core.resource.S3Object
  """
  return PutObject(
    session=session,
    bucket=bucket,
    key=key,
    content=json.dumps(content),
    type_="application/json"
    )


def ListKeysInBucket(*, session, bucketname, prefix=None):
  """List the keys available in a bucket

  :param session: The boto3 session to use
  :type session: boto3.Session
  :param bucketname: Bucket name within which to find keys
  :type bucketname: str
  :param prefix: If provided, the list will be restricted to keys starting with prefix
  :type prefix: str
  :return: List of keys available in the bucket
  :rtype: list
  """
  s3conn = session.connect_to("s3")
  S3Objects = session.get_collection("s3", "S3ObjectCollection")
  s3objects = S3Objects(connection=s3conn, bucket=bucketname)
  return s3objects.each(Prefix=prefix)


def DeleteObjects(*, session, bucketname, keys):
  """Deletes the object specified by key

  :param session: The session to use for AWS connection
  :type session: boto3.session.Session
  :param bucketname: Name of bucket
  :type bucketname: str
  :param keys: Keys to delete from bucket
  :type keys: iterable
  """
  s3conn = session.connect_to("s3")
  S3Objects = session.get_collection("s3", "S3ObjectCollection")
  s3objects = S3Objects(connection=s3conn, bucket=bucketname)
  return s3objects.delete()
