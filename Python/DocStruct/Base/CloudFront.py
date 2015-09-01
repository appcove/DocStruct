# vim:fileencoding=utf-8:ts=2:sw=2:expandtab

#--------------------------------------------------
# CLOUDFRONT RELATED METHODS
#--------------------------------------------------

def CreateDistributionForBucket(*, Session, BucketName):
  """Create a CloudFront distribution to serve files from the bucket identified by <bucketname>

  SEE: http://docs.aws.amazon.com/AmazonCloudFront/latest/APIReference/DistributionConfigDatatype.html for config info
  NOTE: boto3 does not support cloudfront yet. So we need to operate at the lower botocore level :(

  :param session: Session to use for AWS communication
  :type session: boto3.session.Session
  :param bucketname: Name of bucket to use as origin for this distribution
  :type bucketname: str
  :return: Reseponse from the CreateBucket operation
  :rtype: dict
  """
  cfservice = session.get_core_service("cloudfront")
  endpoint = cfservice.get_endpoint()
  operation = cfservice.get_operation("CreateDistribution")
  targetorigin_id = '{0}_origin'.format(bucketname)
  respstatus, resp = operation.call(endpoint, distribution_config={
    'Enabled': True,
    'CallerReference': 'arn:aws:s3:::{0}'.format(bucketname),
    'Comment': 'A distribution to serve video files from {0}'.format(bucketname),
    'PriceClass': 'PriceClass_All',
    'DefaultRootObject': '',
    # CNAME config
    'Aliases': {
      'Quantity': 0,
      'Items': [],
      },
    # Default cache behavior
    'DefaultCacheBehavior': {
      'ForwardedValues': {
        'QueryString': False,
        'Cookies': {'Forward': 'none'}
        },
      # TODO: we need to add the project user here
      'TrustedSigners': {'Quantity': 0, 'Enabled': False},
      'MinTTL': 86400,
      'TargetOriginId': targetorigin_id,
      'AllowedMethods': {'Quantity': 2, 'Items': ['GET', 'HEAD']},
      'ViewerProtocolPolicy': 'allow-all'
      },
    # Other cache behaviors
    'CacheBehaviors': {'Quantity': 0},
    # Logging will be disabled
    'Logging': {
      'Bucket': '',
      'Prefix': '',
      'IncludeCookies': False,
      'Enabled': False,
      },
    # This is the main part of the config
    'Origins': {
      'Quantity': 1,
      'Items': [{
        'Id': targetorigin_id,
        'DomainName': '{0}.s3.amazonaws.com'.format(bucketname),
        'S3OriginConfig': {'OriginAccessIdentity': ''}
        }],
      }
    })
  # If there is an error, we print out the error and return None
  if 'Errors' in resp and len(resp["Errors"]):
    print("ERROR creating distribution")
    print(resp["Errors"][0])
    return None
  return resp
