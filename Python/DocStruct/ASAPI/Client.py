# vim:encoding=utf-8:ts=2:sw=2:expandtab

import json
import mimetypes
from datetime import datetime, timezone, timedelta

from AppStruct.Security import RandomHex

from ..Base import GetSession, S3, SQS
from . import AWS


class AWSConfig(object):

  def __init__(self, ConfigDict):
    # Run all assertions
    # Assert that required keys exist
    assert "keyprefix" in ConfigDict and isinstance(ConfigDict["keyprefix"], str)
    assert "input_bucket" in ConfigDict and isinstance(ConfigDict["input_bucket"], str)
    assert "output_bucket" in ConfigDict and isinstance(ConfigDict["output_bucket"], str)
    # Check sqs
    assert "sqs" in ConfigDict and isinstance(ConfigDict["sqs"], dict)
    assert "queueurl" in ConfigDict["sqs"] and isinstance(ConfigDict["sqs"]["queueurl"], str)
    # Check user
    assert "user" in ConfigDict and isinstance(ConfigDict["user"], dict)
    assert "access_key_id" in ConfigDict["user"] and isinstance(ConfigDict["user"]["access_key_id"], str)
    assert "secret_key" in ConfigDict["user"] and isinstance(ConfigDict["user"]["secret_key"], str)

    # Now prepare attributes
    self.AccessKey = ConfigDict["user"]["access_key_id"]
    self.SecretKey = ConfigDict["user"]["secret_key"]
    self.QueueUrl = ConfigDict["sqs"]["queueurl"]
    self.KeyPrefix = ConfigDict["keyprefix"]
    self.InputBucket = ConfigDict["input_bucket"]
    self.OutputBucket = ConfigDict["output_bucket"]


class Client(object):

  SchemaVersion = '1.0.0'

  def __init__(self, *, Config, Schema):
    self.Schema = Schema

    # TODO: this should be converted to it's own object that validates the incoming dict
    self.Config = AWSConfig(Config)

    # One of the things to do here, because this should only run once per thread init,
    # is to check the Version of the Schema in the current DB connection and verify
    # that our code and the DB schema are in sync.
    SchemaVersion = App.DB.Value('''
      SELECT
        "Version"
      FROM
        "AWS"."Release"
      '''
      )
    if SchemaVersion != self.SchemaVersion:
      raise TypeError("DocStruct schema version does not match DocStruct code version. Please make sure database is upgraded before running App.")

  @property
  def Session(self):
    try:
      session = self._session
    except AttributeError:
      session = self._session = GetSession(
        AccessKey=self.Config.AccessKey,
        SecretKey=self.Config.SecretKey
        )
    return session

  def GetBucketAndKeyFromArn(self, Arn):
    return S3.GetBucketAndKeyFromArn(Arn)

  def GetInputBucketUrl(self):
    return S3.GetBucketUrl(self.Config.InputBucket)

  def GetOutputBucketUrl(self):
    return S3.GetBucketUrl(self.Config.OutputBucket)

  ###############################################################################
  def S3_PrepareUpload(self, *, FileInfo, RemoteAddr, CreateUser, OwnerHint="nobody", expiresin=3600):
    filesize = FileInfo["FileSize"]
    filename = FileInfo["FileName"]
    filetype = FileInfo["FileType"]
    mimetype = mimetypes.guess_type(filename)[0] or "application/octet-stream"
    esid = (datetime.now().strftime('%Y%m%d%H%M%S') + RandomHex())[0:64]
    expiresat = datetime.utcnow() + timedelta(seconds=expiresin)
    key = "{0}/{1}".format(self.Config.KeyPrefix, esid)
    # Get the policy dict
    d = S3.GetFormParameters(
      self.Session,
      self.Config.InputBucket,
      key,
      algo="HMAC-SHA1",
      contenttype=mimetype,
      expiration=expiresat
      )
    # Before we return, we need to create a file record
    tzpolicyex = expiresat.replace(tzinfo=timezone(timedelta(seconds=0)))
    s3file = AWS.S3_File.Create(
      S3_File_ESID=esid,
      CreateAddr=RemoteAddr,
      CreateUser=CreateUser,
      OwnerHint=OwnerHint,
      Input_FileName=filename,
      Input_Size=filesize,
      Input_ContentType=mimetype,
      Input_Type=filetype,
      Input_Expiration=tzpolicyex
      )
    d["S3_File_MNID"] = s3file.S3_File_MNID
    d["S3_File_ESID"] = s3file.S3_File_ESID
    return d

  ###############################################################################
  def S3_UploadStarted(self, FileInfo):
    # Get the file record by S3_File_ESID
    s3file = AWS.S3_File.FindByESID(S3_File_ESID=FileInfo['S3_File_ESID'], DB=App.DB)
    # Now we can update the File record
    s3file.MarkAsStarted()
    return s3file

  ###############################################################################
  def S3_UploadComplete(self, FileInfo):
    bucketname = FileInfo['Bucket']
    key = FileInfo['Key']
    objectarn = "arn:aws:s3:::{0}/{1}".format(bucketname, key)

    # Get the File object from DB
    s3file = AWS.S3_File.FindByESID(S3_File_ESID=FileInfo['S3_File_ESID'], DB=App.DB)
    s3file.Input_Arn = objectarn

    # Prepare job parameters
    jobparams = s3file.PrepareJobParameters(self)
    if not jobparams:
      return {"Message": ""}

    jobspec = jobparams.ToJSON()
    # Post message to SQS
    message = SQS.PostMessage(self.Session, self.Config.QueueUrl, jobspec)

    # Now we can mark the file as finished uploading.
    # NOTE: we pass in an empty string for the JobArn so that the pending queries still work
    s3file.MarkAsEnded(JobArn="", ObjectArn=objectarn, JobSpecification=jobspec)

    # That's it. We're ready to return the message id
    return {"Message": message.message_id}

  ###############################################################################
  def S3_VideoStatus(self, S3_File_MNID):
    s3file = AWS.S3_File(S3_File_MNID)

    RVAL = aadict()
    RVAL.S3_File_MNID = s3file.S3_File_MNID

    if s3file.IsTranscoded:
      RVAL.Ready = True
      RVAL.Status = 'Video Processing Complete'
    elif s3file.Input_Error:
      RVAL.Ready = False
      RVAL.Status = 'Error Encountered During Video Processing'
    elif s3file.Input_EndTime is None:
      RVAL.Ready = False
      RVAL.Status = 'File Is Not Uploaded Yet or Error Encountered During Upload'
    else:
      RVAL.Ready = False
      RVAL.Status = 'Video Processing...'

    RVAL.FileName = s3file.Input_FileName

    return RVAL

  ###############################################################################
  def S3_TranscodeStatusCheck(self, S3_File_ESID):
    s3file = AWS.S3_File.FindByESID(S3_File_ESID=S3_File_ESID, DB=App.DB)

    key = s3file.Input_Arn.split(':')[-1].replace("{0}/".format(self.Config.OutputBucket), "").replace('input.dat', 'output.json')

    # check if the output file is available yet
    jdict = S3.GetJSON(
      session=self.Session,
      bucket=self.Config.OutputBucket,
      key=key
      )

    if not jdict:
      #TODO: let's check and see if too long has passed, we will update this with an error message
      return None

    # Get the job state
    state = jdict['state']

    # On complete we will create and save the output versions
    if state == "COMPLETED":
      s3file.AddVersions(DB=App.DB, AWSResponse=jdict, OutputBucket=self.Config.OutputBucket)

    elif state == 'ERROR':
      s3file.Input_Error = json.dumps(jdict)
      s3file.IsTranscoded = False
      s3file.Save()

    return s3file

  ###############################################################################
  def S3_ServeVideoVersionMap(self, S3_File_MNID, expiresin=10800):
    """
    Create the URIs for serving different video versions of this file

    Returns

    OrderedDict {
      VideoVersion: aadict(src = ..., type=...)
      ...
      }
    """

    s3file = AWS.S3_File(S3_File_MNID)

    # If there was an error, we can just return None
    if s3file.Input_Error:
      return None

    # File may have been updated in the TranscoderStatusCheck call
    if not s3file.IsTranscoded:
      return None

    OutputMap = OrderedDict()

    for version in s3file.GetVideoVersionList():
      bucket, key = self.Config.GetBucketAndKeyFromArn(version["Arn"])

      OutputMap[version.VideoVersion] = aadict(
        src = S3.GetSignedUrl(self.Session, bucket, key, expiresin),
        type = version.HTML_Type,
        )

      # App.Log(version)

    return OutputMap or None

  ###############################################################################
  def S3_File_Poll(self):
    numfiles = 0
    numerrors = 0
    numtranscoded = 0
    for f in AWS.S3_File.ListPending(DB=App.DB):
      try:
        print("checking status for S3_File_MNID={0}, S3_File_ESID={1}".format(f.S3_File_MNID, f.S3_File_ESID))
        self.S3_TranscodeStatusCheck(f.S3_File_ESID)
      except Exception as e:
        numerrors += 1
        print("ERROR: (checking status for <{0}>)".format(f.S3_File_MNID))
        traceback.print_exc()
        print()
        f.Input_Error = str(e)
        f.Save()
      else:
        numtranscoded += 1
      finally:
        numfiles+=1
    return numfiles, numerrors, numtranscoded

  ###############################################################################
  def Get_Transcoded_S3_File_From_ACRM_File(self, File_MNID):
    '''
    This function either returns an S3_File object or None

    To find the S3_File object, it looks up the Hash from the ACRM.File table and then
    pads it with "0", and uses that as an S3_File_ESID.
    '''
    DB = App.DB
    try:
      FileInfo = DB.Row('''
        SELECT
          "File_MNID",
          "FileName",
          "ContentType",
          "Size",
          "Hash"
        FROM
          "ACRM"."File"
        WHERE True
          AND "File_MNID" = $File_MNID
        ''',
        File_MNID = File_MNID
        )
    except DB.NotOneFound:
      raise ValueError("File_MNID {0} not found.".format(File_MNID))

    # Create the ESID using YYYYMMDDHHIISS<hash>0000000000
    # Instead of actual values for YYYYMMDDHHIISS, just use zeros.
    S3_File_ESID = '00000000000000' + FileInfo.Hash + '0000000000'

    # If it already exists in S3_File, then exit
    try:
      s3file = AWS.S3_File.FindByESID(S3_File_ESID=S3_File_ESID, DB=App.DB)
    except DB.NotOneFound:
      return None

    # is it transcoded?
    if not s3file.IsTranscoded:
      return None

    return s3file

  ###############################################################################
  def S3_UploadFromACRM(self, File_MNID, *, GetFS, Input_Type='Video', Overwrite=False):
    DB = App.DB
    FS = GetFS()
    try:
      FileInfo = DB.Row('''
        SELECT
          "File_MNID",
          "FileName",
          "ContentType",
          "Size",
          "Hash"
        FROM
          "ACRM"."File"
        WHERE True
          AND "File_MNID" = $File_MNID
        ''',
        File_MNID = File_MNID
        )
    except DB.NotOneFound:
      raise ValueError("File_MNID {0} not found.".format(File_MNID))

    # Create the ESID using YYYYMMDDHHIISS<hash>0000000000
    # Instead of actual values for YYYYMMDDHHIISS, just use zeros.
    S3_File_ESID = '00000000000000' + FileInfo.Hash + '0000000000'

    # If it already exists in S3_File, then exit
    try:
      f = AWS.S3_File.FindByESID(S3_File_ESID=S3_File_ESID, DB=DB)
      if Overwrite:
        f.Delete()
      else:
        return f
    # Otherwise, let's continue
    except DB.NotOneFound:
      pass

    # Ensure the hash exists in FileStruct
    if FileInfo.Hash not in FS:
      raise ValueError("FileHash '{0}' not found in FileStruct!".format(FileInfo.Hash))

    # Here we need to upload the file to S3 and trigger a transcoding job
    # First create some variables we will need to create the S3_File record
    expiresat = datetime.utcnow().replace(tzinfo=timezone(timedelta(seconds=0)))
    key = "{0}/{1}/input.dat".format(self.Config.KeyPrefix, S3_File_ESID)

    # Now we have all the info to create the S3_File record
    s3file = AWS.S3_File.Create(
      S3_File_ESID = S3_File_ESID,
      CreateAddr = "127.0.0.1",
      CreateUser = -1,
      OwnerHint = "nobody",
      Input_FileName = FileInfo.FileName,
      Input_Size = FileInfo.Size,
      Input_ContentType = FileInfo.ContentType,
      Input_Type = Input_Type,
      Input_Expiration = expiresat
      )

    # We are about to start upload
    S3_UploadStarted({'S3_File_ESID': S3_File_ESID})

    # Upload the file to S3
    with open(FS[FileInfo.Hash].Path, "rb") as fp:
      S3.PutObject(session=self.session, bucket=self.Config.InputBucket, key=key, content=fp, type_=FileInfo.ContentType)

    # Since file has uploaded we can mark it as ended.
    # NOTE: this also posts the SQS message to transcode file
    S3_UploadComplete({
      "Key": key,
      "Bucket": self.Config.InputBucket,
      "S3_File_ESID": S3_File_ESID,
      })

    return s3file

  ###############################################################################
  def S3_File_RetriggerJob(self, S3_File_MNID):
    DB = App.DB
    # Try to get the file info
    try:
      f = AWS.S3_File(S3_File_MNID)
    except DB.NotOneFound:
      print("Could not find S3_File<{0}>".format(S3_File_MNID))
      return

    # Save for later
    Bucket, Key = self.Config.GetBucketAndKeyFromArn(f.Input_Arn)

    # Delete all related info
    with DB.Transaction():
      if f.Input_Type == 'Document':
        AWS.S3_File_Document.DeleteAllRecords(S3_File_MNID)
      elif f.Input_Type == 'Video':
        AWS.S3_File_Video.DeleteAllRecords(S3_File_MNID)
      elif f.Input_Type.endswith('Image'):
        AWS.S3_File_Image.DeleteAllRecords(S3_File_MNID)

      # Delete all file info
      f.IsTranscoded = False
      f.JobSpecification = "{}"
      f.Input_Arn = ""
      f.Input_JobArn = ""
      f.Input_Error = ""
      f.Save()

    # Now we retrigger
    S3_UploadComplete({
      "Key": Key,
      "Bucket": self.Config.InputBucket,
      "S3_File_ESID": f.S3_File_ESID,
      })
