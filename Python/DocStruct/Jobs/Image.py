# vim:fileencoding=utf-8:ts=2:sw=2:expandtab
import os.path
import collections
import subprocess
import mimetypes

from hashlib import sha1
from ..Base import GetSession, S3
from . import Job, S3BackedFile


Output = collections.namedtuple('Output', ('Width', 'Height', 'OutputKey'))


class S3BackedImage(S3BackedFile):

  def __init__(self, *, JobName, PreferredOutputs, **kwargs):
    super().__init__(**kwargs)
    self.JobName = JobName
    self.PreferredOutputs = PreferredOutputs
    self._LocalFilePath = None

  def Process(self, *, Output, Command):
    # Process the image by executing the given command
    self.Logger.debug("{0} job for {1} started".format(self.JobName, self.InputKey))
    subprocess.check_output(Command, stderr=subprocess.STDOUT)
    self.Logger.debug("{0} job for {1} completed".format(self.JobName, self.InputKey))
    # Upload new file to S3
    self.Logger.debug("Starting Upload of {0} to S3".format(Output.OutputKey))
    o_fpath = Command[-1]
    o_type = mimetypes.guess_type(o_fpath)[0] or "application/octet-stream"
    o_key = os.path.join(self.OutputKeyPrefix, Output.OutputKey)
    with open(o_fpath, 'rb') as fp:
      S3.PutObject(
        session=self.Config.Session,
        bucket=self.Config.S3_OutputBucket,
        key=o_key,
        content=fp,
        type_=o_type
        )
    self.Logger.debug("Finished Upload of {0} to S3".format(Output.OutputKey))
    # inspect file and save the output so that we can build output.json
    o_fprops = self.InspectImage(o_fpath)
    o_fprops['Key'] = o_key
    # Save the returned properties to create output.json
    self.Output['Outputs'].append(o_fprops)
    # Mark file for deletion
    self.MarkFilePathForCleanup(o_fpath)
    # Return key of the output
    return o_fprops

  @property
  def LocalFilePath(self):
    ret = super().LocalFilePath
    # Inspect the input file and save its properties first
    i_fprops = self.InspectImage(ret)
    i_fprops['Key'] = self.InputKey
    self.Output['Input'] = i_fprops
    # Return original
    return ret

  def Resize(self):
    self.Logger.debug("ResizeImage job for {0} started".format(self.InputKey))
    FilePath = self.LocalFilePath
    # Now we can start main processing
    OutputKeys = []
    # Loop over required outputs to create them
    for o_ in self.PreferredOutputs:
      o = Output(*o_)
      if not o.Width or not o.Height:
        cmd = (
          self.Binaries.Convert,
          FilePath,
          self.GetLocalFilePathFromS3Key(KeyPrefix=self.OutputKeyPrefix, Key=o.OutputKey),
          )
      else:
        cmd = (
          self.Binaries.Convert,
          FilePath,
          '-resize', '{0}x{1}'.format(str(o.Width), str(o.Height)),
          self.GetLocalFilePathFromS3Key(KeyPrefix=self.OutputKeyPrefix, Key=o.OutputKey),
          )
      # Now run the command
      self.Process(Output=o, Command=cmd)

  def Normalize(self):
    self.Logger.debug("NormalizeImage job for {0} started".format(self.InputKey))
    # Download the required file and save it to a temp location
    FilePath = self.LocalFilePath
    OutputKeys = []
    # Loop over required outputs to create them
    for o_ in self.PreferredOutputs:
      o = Output(*o_)
      cmd = (
        self.Binaries.Convert,
        FilePath,
        '-resize', '{0}x{1}^'.format(str(o.Width), str(o.Height)),
        '-gravity', 'Center',
        '-extent', '{0}x{1}'.format(str(o.Width), str(o.Height)),
        self.GetLocalFilePathFromS3Key(KeyPrefix=self.OutputKeyPrefix, Key=o.OutputKey),
        )
      # Now run the command
      self.Process(Output=o, Command=cmd)

  def Run(self):
    if self.JobName == 'ResizeImage':
      return self.Resize()
    if self.JobName == 'NormalizeImage':
      return self.Normalize()
    raise Exception("{0} is not a known job name".format(self.JobName))


@Job
def ResizeImage(*, InputKey, OutputKeyPrefix, PreferredOutputs, Config, Logger):
  # Prepare context in which we'll run
  ctxt = S3BackedImage(
    InputKey=InputKey,
    OutputKeyPrefix=OutputKeyPrefix,
    Config=Config,
    Logger=Logger,
    JobName='ResizeImage',
    PreferredOutputs=PreferredOutputs,
    )
  # Start the processing
  with ctxt as im:
    im.Run()


@Job
def NormalizeImage(*, InputKey, OutputKeyPrefix, PreferredOutputs, Config, Logger):
  # Prepare context in which we'll run
  ctxt = S3BackedImage(
    InputKey=InputKey,
    OutputKeyPrefix=OutputKeyPrefix,
    Config=Config,
    Logger=Logger,
    JobName='NormalizeImage',
    PreferredOutputs=PreferredOutputs,
    )
  # Start the processing
  with ctxt as im:
    im.Run()
