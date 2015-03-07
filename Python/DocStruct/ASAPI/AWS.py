# vim:ts=2:sw=2:expandtab
# -*- coding: utf-8 -*-
import json
import mimetypes
import traceback
import os.path

from datetime import datetime, timedelta, timezone

from AppStruct.Base.V1 import *
from AppStruct.Util import aadict, SQL
from AppStruct.Security import RandomHex

from Project.Base import MakeSIUD
import Pusher.ClientLib

from ..JobSpecification import TranscodeVideoJob, ConvertToPDFJob, ResizeImageJob, NormalizeImageJob


###############################################################################
# TODO: merge into AppStruct
class DatetimeField(DatetimeField):

  FORMAT = '%Y-%m-%d %H:%M:%S.%f%z'

  def Validate(self, record, fv):
    if isinstance(fv.value, str):
      if fv.value == 'NOW()' and (record.InsertMode or record.UpdateMode):
        return True
      # NOTE: %z expects a 5 character timezone indicator
      #       BUT pg saves timezone in 3 characters. Hence the '00'
      # TODO: check if there is a way for pg to save datetime with 5 character tz
      try:
        fv.value = datetime.strptime(fv.value + '00', self.FORMAT)
      except ValueError:
        fv.AddError('{0} cannot be parsed using {1}'.format(fv.value, self.FORMAT))
        return False
    return True


###############################################################################
class S3_File(metaclass=MetaRecord):

  SCHEMA = 'AWS'
  TABLE = 'S3_File'

  PrimaryKeyFields = ['S3_File_MNID']
  SELECT,INSERT,UPDATE,DELETE = MakeSIUD(SCHEMA, TABLE, *PrimaryKeyFields)

  #============================================================================
  class S3_File_MNID(IntegerField):
    Flags = +Read

  #============================================================================
  class S3_File_ESID(StringField):
    MinLength = 64
    MaxLength = 64
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class CreateDate(DatetimeField):
    InsertValue = SQL('NOW()')
    Flags = +Read +InsertWrite

  #============================================================================
  class CreateAddr(StringField):
    MinLength = 7  # 0.0.0.0
    MaxLength = 15 # 000.000.000.000
    RegexMatch = (r'\d{1,3}(\.\d{1,3}){3}', 'A valid IPv4 address is required')
    Flags = +Read +InsertWrite

  #============================================================================
  class CreateUser(IntegerField):
    Flags = +Read +InsertWrite

  #============================================================================
  class OwnerHint(StringField):
    Flags = +Read +InsertWrite

  #============================================================================
  class Input_FileName(StringField):
    """Filename as provided by the client."""
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class Input_ContentType(StringField):
    """Content-Type as guessed by the mimetypes module"""
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class Input_Size(IntegerField):
    """Size of the input file."""
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class Input_Type(StringField):
    """Project specific string identifying type of input."""
    # RegexMatch = (r'^(Audio|Video|Document|ProfileImage|ShockBoxImage)$', 'Type must refer to a valid S3_File_Type instance')
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class Input_Expiration(DatetimeField):
    """Time when the upload policy expires. Uploads must complete by this time."""
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class Input_StartTime(DatetimeField):
    """Time when the upload started"""
    AllowEmpty = True
    AllowSQL = True
    Flags = +Read +Write

  #============================================================================
  class Input_EndTime(DatetimeField):
    """Time when the upload ended"""
    AllowEmpty = True
    AllowSQL = True
    Flags = +Read +Write

  #============================================================================
  class Input_Error(StringField):
    """Holds error message returned by AWS if any"""
    AllowEmpty = True
    Flags = +Read +Write

  #============================================================================
  class Input_Arn(StringField):
    """Amazon Arn identifying this object"""
    AllowEmpty = True
    Flags = +Read +Write

  #============================================================================
  class Input_JobArn(StringField):
    """Amazon Arn indentifying the job that was created for transcoding this file"""
    AllowEmpty = True
    Flags = +Read +Write

  #============================================================================
  class IsTranscoded(BoolField):
    """Will return true if the transcoding was completed"""
    AllowEmpty = False
    AllowSQL = True
    InsertValue = SQL('FALSE')
    Flags = +Read +Write

  #============================================================================
  class JobSpecification(StringField):
    """Maintains specifications for the job that was triggered by this file"""
    AllowEmpty = True
    Flags = +Read +Write

  #============================================================================
  @property
  def Status(self):
    if self.Input_Error:
      return "Error"
    if not self.Input_EndTime:
      return "Uploading"
    if not self.IsTranscoded:
      return "Transcoding"
    return "Finished"

  #============================================================================
  @property
  def VideoMap(self):
    return App.DS.S3_ServeVideoVersionMapForS3File(self)

  #============================================================================
  @property
  def VideoDuration(self):
    try:
      d = S3_File_Video(self.S3_File_MNID)
      return d.VideoDuration
    except App.DB.NotOneFound:
      return None

  #============================================================================
  def GetData(self, FieldSet=None):
    r = super().GetData(FieldSet=FieldSet)
    if (FieldSet is None) or (FieldSet is not None and 'VideoMap' in FieldSet):
      r['VideoMap'] = self.VideoMap
    if (FieldSet is None) or (FieldSet is not None and 'VideoDuration' in FieldSet):
      r['VideoDuration'] = self.VideoDuration
    return r

  #============================================================================
  def GetVideoVersionList(self):
    if not self.IsTranscoded or self.Input_Error:
      return []
    return App.DB.RowList('''
      SELECT
        "S3_File_Video"."VideoDuration" as "Duration",
        "S3_File_Video_Version"."VideoVersion" as "VideoVersion",
        "S3_File_Video_Version"."Width" as "Width",
        "S3_File_Video_Version"."Height" as "Height",
        "S3_File_Video_Version"."Arn" as "Arn",
        "VideoVersion"."HTML_Type"
      FROM
        "AWS"."S3_File"
        INNER JOIN "AWS"."S3_File_Video" USING ("S3_File_MNID")
        INNER JOIN "AWS"."S3_File_Video_Version" USING ("S3_File_MNID")
        INNER JOIN "AWS"."VideoVersion" USING ("VideoVersion")
      WHERE True
        AND "S3_File"."S3_File_MNID" = $S3_File_MNID
      ''',
      S3_File_MNID=self.S3_File_MNID
      )

  #============================================================================
  def MarkAsStarted(self):
    self.Input_StartTime = SQL('NOW()')
    return self.Save()

  #============================================================================
  def MarkAsEnded(self, *, JobArn, ObjectArn, JobSpecification):
    self.Input_Arn = ObjectArn
    self.Input_JobArn = JobArn
    self.Input_EndTime = SQL('NOW()')
    self.JobSpecification = JobSpecification
    return self.Save()

  #============================================================================
  def AddVersions(self, *, AWSResponse, OutputBucket):
    # Handle creating of versions in a transaction
    if self.Input_Type == 'Video':
      S3_File_Video.AddVersions(
        S3_File_MNID=self.S3_File_MNID,
        AWSResponse=AWSResponse,
        OutputBucket=OutputBucket
        )
    elif self.Input_Type.endswith('Image'):
      S3_File_Image.AddVersions(
        S3_File_MNID=self.S3_File_MNID,
        AWSResponse=AWSResponse,
        OutputBucket=OutputBucket
        )
    elif self.Input_Type == 'Document':
      S3_File_Document.AddVersions(
        S3_File_MNID=self.S3_File_MNID,
        AWSResponse=AWSResponse,
        OutputBucket=OutputBucket
        )
    # Now we can mark this file as transcoded
    self.Input_JobArn = ""
    self.IsTranscoded = True
    self.Save()

    if self.Input_Type == 'Video':
      # Send pusher message
      PusherChannel = 'presence-channel_docstruct_' + str(App.DevLevel)
      PusherClient = Pusher.ClientLib.Pusher(**App.Pusher)
      # Send pusher update message
      PusherClient[PusherChannel].trigger('video-file-transcoded',{
        'S3_File_MNID' : self.S3_File_MNID,
        'Input_FileName' : self.Input_FileName,
      })

    # Return
    return self

  #============================================================================
  def PrepareJobParameters(self, AWSClient):

    Bucket, Key = AWSClient.GetBucketAndKeyFromArn(self.Input_Arn)
    OutputKeyPrefix = "/".join(Key.split("/")[:-1])

    jobcls = None

    if self.Input_Type == "Video":
      jobcls = TranscodeVideoJob

    elif self.Input_Type == "ShockBoxImage":
      jobcls = ResizeImageJob

    elif self.Input_Type == "ProfileImage":
      jobcls = NormalizeImageJob

    elif self.Input_Type.endswith('Image'):
      jobcls = ResizeImageJob

    elif self.Input_Type == "Document":
      jobcls = ConvertToPDFJob

    elif self.Input_Type == "Audio":
      # TODO:
      return None

    else:
      raise ValueError('For some reason, the job for S3_File_MNID={0} did not have a valid input type of "{1}".'.format(self.S3_File_MNID, self.Input_Type))

    # Instantiate the job object and return its JSON representation
    return jobcls(InputKey=Key, OutputKeyPrefix=OutputKeyPrefix)

  ###############################################################################
  @classmethod
  def Create(cls, *, S3_File_ESID, CreateAddr, CreateUser, OwnerHint, Input_FileName, Input_Size, Input_ContentType, Input_Type, Input_Expiration):
    f = cls(None)
    f.S3_File_ESID = S3_File_ESID
    f.CreateAddr = CreateAddr
    f.CreateUser = CreateUser
    f.OwnerHint = OwnerHint
    f.Input_FileName = Input_FileName
    f.Input_Size = Input_Size
    f.Input_ContentType = Input_ContentType
    f.Input_Type = Input_Type  # 'Audio' if Input_ContentType.startswith("audio") else 'Video'
    f.Input_Expiration = Input_Expiration
    f.Save()
    return f

  ###############################################################################
  @classmethod
  def FindByESID(cls, *, S3_File_ESID):
    return cls(App.DB.Value('''
      SELECT
        "S3_File_MNID"
      FROM
        "AWS"."S3_File"
      WHERE True
        AND "S3_File_ESID" = $S3_File_ESID
      ''',
      S3_File_ESID=S3_File_ESID
      ))

  ###############################################################################
  @classmethod
  def ListByQuery(cls, Query, **kwargs):
    return [cls(row.S3_File_MNID) for row in App.DB.RowList(Query, **kwargs)]

  ###############################################################################
  @classmethod
  def ListPending(cls, Start=0, PageLength=100):
    return cls.ListByQuery('''
      SELECT
        "S3_File_MNID"
      FROM
        "AWS"."S3_File"
      WHERE True
        AND "Input_JobArn" IS NOT NULL
        AND "Input_Error" IS NULL
        AND "Input_EndTime" > "Input_StartTime"
        AND NOT "IsTranscoded"
      ORDER BY "Input_StartTime"
      OFFSET $Start LIMIT $PageLength
      ''',
      Start=Start,
      PageLength=PageLength
      )

  ###############################################################################
  @classmethod
  def ListUnStarted(cls, Start=0, PageLength=100):
    return cls.ListByQuery('''
      SELECT
        "S3_File_MNID"
      FROM
        "AWS"."S3_File"
      WHERE True
        AND "Input_StartTime" IS NULL
      ORDER BY "CreateDate"
      OFFSET $Start LIMIT $PageLength
      ''',
      Start=Start,
      PageLength=PageLength
      )

  ###############################################################################
  @classmethod
  def ListErroneous(cls, Start=0, PageLength=100):
    return cls.ListByQuery('''
      SELECT
        "S3_File_MNID"
      FROM
        "AWS"."S3_File"
      WHERE True
        AND "Input_Error" IS NOT NULL
      ORDER BY "CreateDate"
      OFFSET $Start LIMIT $PageLength
      ''',
      Start=Start,
      PageLength=PageLength
      )

  ###############################################################################
  @classmethod
  def ListCompleted(cls, Start=0, PageLength=100):
    return cls.ListByQuery('''
      SELECT
        "S3_File_MNID"
      FROM
        "AWS"."S3_File"
      WHERE True
        AND "IsTranscoded"
      ORDER BY "Input_StartTime"
      OFFSET $Start LIMIT $PageLength
      ''',
      Start=Start,
      PageLength=PageLength
      )


###############################################################################
class S3_File_Document(metaclass=MetaRecord):
  SCHEMA = 'AWS'
  TABLE = 'S3_File_Document'

  PrimaryKeyFields = ['S3_File_MNID']
  SELECT,INSERT,UPDATE,DELETE = MakeSIUD(SCHEMA, TABLE, *PrimaryKeyFields)

  #============================================================================
  class S3_File_MNID(IntegerField):
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class NumPages(IntegerField):
    Flags = +Read +Write

  #============================================================================
  @classmethod
  def AddVersions(cls, *, S3_File_MNID, AWSResponse, OutputBucket):
    with App.DB.Transaction():
      # Save the main record
      Input = AWSResponse["Input"]
      try:
        doc = S3_File_Document(S3_File_MNID)
      except App.DB.NotOneFound:
        doc = S3_File_Document(None)
        doc.S3_File_MNID = S3_File_MNID
      # Saving input properties here
      doc.NumPages = Input["NumPages"]
      doc.Save()

      # Save versions
      for i, output in enumerate(AWSResponse["Outputs"]):
        # The first element is always the converted output file
        if i == 0:
          # Create the version records
          try:
            docversion = S3_File_Document_Version(S3_File_MNID, 'PDF')
          except App.DB.NotOneFound:
            docversion = S3_File_Document_Version(None, None)
            docversion.S3_File_MNID = S3_File_MNID
          # Save the modified props
          docversion.DocumentVersion = "PDF"
          docversion.Arn = "arn:aws:s3:::{0}/{1}/{2}".format(
            OutputBucket,
            AWSResponse["OutputKeyPrefix"],
            output["Key"]
            )
          docversion.Save()
        else:
          # Create the page records
          quality = 'Regular' if output["Key"].find('.1200x1200.') > -1 else 'Thumbnail'
          try:
            docpage = S3_File_Document_Page(S3_File_MNID, i, quality)
          except App.DB.NotOneFound:
            docpage = S3_File_Document_Page(None, None, None)
            docpage.S3_File_MNID = S3_File_MNID
            docpage.PageNumber = output["PageNumber"]
            docpage.Quality = quality
          # Save the modified props
          docpage.Width = output["Width"]
          docpage.Height = output["Height"]
          docpage.ImageType = output["Type"]
          docpage.Arn = "arn:aws:s3:::{0}/{1}/{2}".format(
            OutputBucket,
            AWSResponse["OutputKeyPrefix"],
            output["Key"]
            )
          docpage.Save()

  #============================================================================
  @classmethod
  def DeleteAllRecords(cls, S3_File_MNID):
    App.DB.Execute('''
      DELETE FROM
        "AWS"."S3_File_Document_Page"
      WHERE
        "S3_File_MNID" = $S3_File_MNID
      ''',
      S3_File_MNID = S3_File_MNID
      )

    App.DB.Execute('''
      DELETE FROM
        "AWS"."S3_File_Document_Version"
      WHERE
        "S3_File_MNID" = $S3_File_MNID
      ''',
      S3_File_MNID = S3_File_MNID
      )

    App.DB.Execute('''
      DELETE FROM
        "AWS"."S3_File_Document"
      WHERE
        "S3_File_MNID" = $S3_File_MNID
      ''',
      S3_File_MNID = S3_File_MNID
      )


###############################################################################
class S3_File_Document_Version(metaclass=MetaRecord):
  SCHEMA = 'AWS'
  TABLE = 'S3_File_Document_Version'

  PrimaryKeyFields = ['S3_File_MNID', 'DocumentVersion']
  SELECT,INSERT,UPDATE,DELETE = MakeSIUD(SCHEMA, TABLE, *PrimaryKeyFields)

  #============================================================================
  class S3_File_MNID(IntegerField):
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class DocumentVersion(StringField):
    Flags = +Read +Write

  #============================================================================
  class Arn(StringField):
    Flags = +Read +Write


###############################################################################
class S3_File_Document_Page(metaclass=MetaRecord):

  SCHEMA = 'AWS'
  TABLE = 'S3_File_Document_Page'

  PrimaryKeyFields = ['S3_File_MNID', 'PageNumber', 'Quality']
  SELECT,INSERT,UPDATE,DELETE = MakeSIUD(SCHEMA, TABLE, *PrimaryKeyFields)

  #============================================================================
  class S3_File_MNID(IntegerField):
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class PageNumber(IntegerField):
    Flags = +Read +Write

  #============================================================================
  class Quality(StringField):
    Flags = +Read +Write

  #============================================================================
  class Width(IntegerField):
    Flags = +Read +Write

  #============================================================================
  class Height(IntegerField):
    Flags = +Read +Write

  #============================================================================
  class ImageType(StringField):
    Flags = +Read +Write

  #============================================================================
  class Arn(StringField):
    Flags = +Read +Write


###############################################################################
class S3_File_Image(metaclass=MetaRecord):
  SCHEMA = 'AWS'
  TABLE = 'S3_File_Image'

  PrimaryKeyFields = ['S3_File_MNID']
  SELECT,INSERT,UPDATE,DELETE = MakeSIUD(SCHEMA, TABLE, *PrimaryKeyFields)

  #============================================================================
  class S3_File_MNID(IntegerField):
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class Width(IntegerField):
    Flags = +Read +Write

  #============================================================================
  class Height(IntegerField):
    Flags = +Read +Write

  #============================================================================
  class ImageType(StringField):
    Flags = +Read +Write

  #============================================================================
  @classmethod
  def AddVersions(cls, *, S3_File_MNID, AWSResponse, OutputBucket):
    with App.DB.Transaction():
      # Save the main record
      Input = AWSResponse["Input"]
      try:
        img = S3_File_Image(S3_File_MNID)
      except App.DB.NotOneFound:
        img = S3_File_Image(None)
        img.S3_File_MNID = S3_File_MNID
      # Saving input properties here
      img.Width = Input["Width"]
      img.Height = Input["Height"]
      img.ImageType = Input["Type"]
      img.Save()

      # Save versions
      for i, output in enumerate(AWSResponse["Outputs"]):
        Quality = os.path.basename(output['Key']).replace('.jpg', '')
        # Create the version records
        try:
          imgversion = S3_File_Image_Version(S3_File_MNID, Quality, img.Width, img.Height)
        except App.DB.NotOneFound:
          imgversion = S3_File_Image_Version(None, None, None, None)
          imgversion.S3_File_MNID = S3_File_MNID
        # Save the modified props
        imgversion.Width = output["Width"]
        imgversion.Height = output["Height"]
        imgversion.Quality = Quality
        imgversion.ImageType = output["Type"]
        imgversion.Arn = "arn:aws:s3:::{0}/{1}{2}".format(
          OutputBucket,
          AWSResponse["OutputKeyPrefix"],
          output["Key"]
          )
        imgversion.Save()

  #============================================================================
  @classmethod
  def DeleteAllRecords(cls, *, S3_File_MNID):
    App.DB.Execute('''
      DELETE FROM
        "AWS"."S3_File_Image_Version"
      WHERE
        "S3_File_MNID" = $S3_File_MNID
      ''',
      S3_File_MNID = S3_File_MNID
      )

    App.DB.Execute('''
      DELETE FROM
        "AWS"."S3_File_Image"
      WHERE
        "S3_File_MNID" = $S3_File_MNID
      ''',
      S3_File_MNID = S3_File_MNID
      )


###############################################################################
class S3_File_Image_Version(metaclass=MetaRecord):
  SCHEMA = 'AWS'
  TABLE = 'S3_File_Image_Version'

  PrimaryKeyFields = ['S3_File_MNID', 'Quality', 'Width', 'Height']
  SELECT,INSERT,UPDATE,DELETE = MakeSIUD(SCHEMA, TABLE, *PrimaryKeyFields)

  #============================================================================
  class S3_File_MNID(IntegerField):
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class Quality(StringField):
    Flags = +Read +Write

  #============================================================================
  class Width(IntegerField):
    Flags = +Read +Write

  #============================================================================
  class Height(IntegerField):
    Flags = +Read +Write

  #============================================================================
  class ImageType(StringField):
    Flags = +Read +Write

  #============================================================================
  class Arn(StringField):
    Flags = +Read +Write


###############################################################################
class S3_File_Video(metaclass=MetaRecord):
  SCHEMA = 'AWS'
  TABLE = 'S3_File_Video'

  PrimaryKeyFields = ['S3_File_MNID']
  SELECT,INSERT,UPDATE,DELETE = MakeSIUD(SCHEMA, TABLE, *PrimaryKeyFields)

  #============================================================================
  class S3_File_MNID(IntegerField):
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class VideoDuration(IntegerField):
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  @classmethod
  def AddVersions(cls, *, S3_File_MNID, AWSResponse, OutputBucket):
    with App.DB.Transaction():
      for i, output in enumerate(AWSResponse["outputs"]):
        if i == 0:
          # Get or create the main record
          try:
            vid = S3_File_Video(S3_File_MNID)
          except App.DB.NotOneFound:
            vid = S3_File_Video(None)
            vid.S3_File_MNID = S3_File_MNID
          # Update a video version
          vid.VideoDuration = output["duration"]
          vid.Save()
        # Create the video version
        VideoVersion = "Web" if output['key'].endswith('mp4') else "Webm"
        try:
          vidversion = S3_File_Video_Version(S3_File_MNID, VideoVersion)
        except App.DB.NotOneFound:
          vidversion = S3_File_Video_Version(None, None)
          vidversion.S3_File_MNID = S3_File_MNID
          vidversion.VideoVersion = VideoVersion
        # Save the modified props
        vidversion.Width = output["width"]
        vidversion.Height = output["height"]
        vidversion.Arn = "arn:aws:s3:::{0}/{1}{2}".format(
          OutputBucket,
          AWSResponse["outputKeyPrefix"],
          output["key"]
          )
        vidversion.TranscoderDump = json.dumps(output)
        vidversion.Save()

  #============================================================================
  @classmethod
  def DeleteAllRecords(cls, *, S3_File_MNID):
    # Delete records from S3_File_Video_Version
    App.DB.Execute('''
      DELETE FROM
        "AWS"."S3_File_Video_Version"
      WHERE
        "S3_File_MNID" = $S3_File_MNID
      ''',
      S3_File_MNID = S3_File_MNID
      )
    # Delete records from S3_File_Video
    App.DB.Execute('''
      DELETE FROM
        "AWS"."S3_File_Video"
      WHERE
        "S3_File_MNID" = $S3_File_MNID
      ''',
      S3_File_MNID = S3_File_MNID
      )


###############################################################################
class S3_File_Video_Version(metaclass=MetaRecord):
  SCHEMA = 'AWS'
  TABLE = 'S3_File_Video_Version'

  PrimaryKeyFields = ['S3_File_MNID', 'VideoVersion']
  SELECT,INSERT,UPDATE,DELETE = MakeSIUD(SCHEMA, TABLE, *PrimaryKeyFields)

  #============================================================================
  class S3_File_MNID(IntegerField):
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class VideoVersion(StringField):
    MaxLength = 16
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class Width(IntegerField):
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class Height(IntegerField):
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class Arn(StringField):
    MaxLength = 1024
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class TranscoderDump(StringField):
    InsertDefault = ''
    InsertValue = ''
    AllowEmpty = True
    Flags = +Read +Write


###############################################################################
class S3_File_Video_Version(metaclass=MetaRecord):
  SCHEMA = 'AWS'
  TABLE = 'S3_File_Video_Version'

  PrimaryKeyFields = ['S3_File_MNID', 'VideoVersion']
  SELECT,INSERT,UPDATE,DELETE = MakeSIUD(SCHEMA, TABLE, *PrimaryKeyFields)

  #============================================================================
  class S3_File_MNID(IntegerField):
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class VideoVersion(StringField):
    MaxLength = 16
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class Width(IntegerField):
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class Height(IntegerField):
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class Arn(StringField):
    MaxLength = 1024
    Flags = +Read +InsertWrite +InsertRequired

  #============================================================================
  class TranscoderDump(StringField):
    InsertDefault = ''
    InsertValue = ''
    AllowEmpty = True
    Flags = +Read +Write
