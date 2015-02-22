# vim:encoding=utf-8:ts=2:sw=2:expandtab
import re
import os
import os.path
import glob
import mimetypes
import subprocess

from ..Base import S3
from . import Job, S3BackedFile


BIN_CONVERT = "/usr/bin/convert"
GS_BIN = "/usr/bin/gs"
CONVERTER_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../DocumentConverter.py'))


class S3BackedDocument(S3BackedFile):

  def __init__(self, *, OutputKey, **kw):
    super().__init__(**kw)
    self.OutputKey = OutputKey

  def GenerateImagesFromPDF(self, *, PDFPath):
    # Generate images from pages of PDF and save images to S3
    PageNamePrefix = PDFPath.replace('.pdf', '')
    PageNameFormat = PageNamePrefix + '-%d.png'

    # Call ghostscript to produce the images
    try:
      out = subprocess.check_output((GS_BIN, '-sDEVICE=png256', '-dNOPAUSE', '-r300', '-o', PageNameFormat, PDFPath), stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as exc:
      raise Exception("ERROR: {0}".format(exc.output))

    # search for all files with the relevant prefix and upload them to S3
    images = glob.glob(PageNamePrefix + '-*.png')
    regex = re.compile(PageNamePrefix + '-(\d+)\.png')
    images.sort(key=lambda im: int(regex.sub('\g<1>', im)))
    thumbs = []

    # Loop over the images of pages to create thumbnails from them
    for im in images:

      # Prepare filenames
      regular_thumnail_file = im.replace('.png', '.1200x1200.png')
      small_thumbnail_file = im.replace('.png', '.160x160.png')
      page_num = int(regex.sub('\g<1>', im))

      # Now, upload the 2 thumbnails to S3
      for fname in (regular_thumnail_file, small_thumbnail_file):
        # Get the size specification from the filename.
        # EX: if fname == thumb-1.1200x1200.png, fsize = 1200x1200
        fsize = fname.split('.')[-2]
        # Create regular version
        try:
          cmd = (
            BIN_CONVERT,
            im,
            '-resize', fsize,
            fname,
            )
          subprocess.call(cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as exc:
          raise Exception("ERROR: {0}".format(exc.output))

        # We'll upload using a stream
        with open(fname, 'rb') as fp:
          o_key = os.path.join(self.OutputKeyPrefix, fname.replace(PageNamePrefix, 'thumb'))
          o_mime = mimetypes.guess_type(fname)[0] or "application/octet-stream"
          S3.PutObject(
            session=self.Config.Session,
            bucket=self.Config.S3_OutputBucket,
            key=o_key,
            content=fp,
            type_=o_mime,
            )

          # Log message saying that images has uploaded
          self.Logger.debug("Finished Upload of {0} to S3".format(o_key))

          # Inspect the path so that we get image props
          props = self.InspectImage(fname)
          props['Key'] = o_key
          props['PageNumber'] = page_num
          thumbs.append(props)

          # Mark for cleanup
          self.MarkFilePathForCleanup(fname)

      # Make sure the main file gets deleted after we exit
      self.MarkFilePathForCleanup(im)

    # Return all the keys that have been uploaded to S3
    return thumbs

  def ConvertToPDF(self):
    # Prepare some variables we need for this job
    FilePath = self.LocalFilePath
    OutputFilePath = self.GetLocalFilePathFromS3Key(Key=self.OutputKey, KeyPrefix=self.OutputKeyPrefix)
    self.Logger.debug("Will convert {0} to {1}".format(self.InputKey, self.OutputKey))

    # Use pyuno to speak to the headless openoffice server
    try:
      out = subprocess.check_output(('python2', CONVERTER_PATH, FilePath, OutputFilePath), stderr=subprocess.STDOUT)
      self.Logger.debug("Done with conversion")
    except subprocess.CalledProcessError as exc:
      raise Exception("ERROR: {0}".format(exc.output))

    # After conversion upload the file to S3
    o_key = os.path.join(self.OutputKeyPrefix, self.OutputKey)
    o_mime = mimetypes.guess_type(self.OutputKey)[0] or "application/octet-stream"
    with open(OutputFilePath, 'rb') as fp:
      S3.PutObject(
        session=self.Config.Session,
        bucket=self.Config.S3_OutputBucket,
        key=o_key,
        content=fp,
        type_=o_mime,
        )

    # Save output key
    self.Output['Outputs'].append({'Key': o_key, 'Type': 'PDF'})
    # Log a message
    self.Logger.debug("Finished Upload of {0} to S3".format(self.OutputKey))

    # Generate images from all pages of PDF
    o_thumbs = self.GenerateImagesFromPDF(PDFPath=OutputFilePath)
    self.Output['Outputs'].extend(o_thumbs)
    # Add number of thumbnails to Input
    # NOTE: we create 2 thumbnails per input, hence the division by 2
    self.Output['Input'] = {'NumPages': (len(o_thumbs) / 2)}

    # Mark the new file for deletion
    self.MarkFilePathForCleanup(OutputFilePath)

  def Run(self):
    self.ConvertToPDF()


@Job
def ConvertToPDF(*, InputKey, OutputKeyPrefix, Config, Logger, OutputKey='output.pdf'):
  Logger.debug("ResizeImage job for {0} started".format(InputKey))
  # Prepare context in which we'll run
  ctxt = S3BackedDocument(
    InputKey=InputKey,
    OutputKeyPrefix=OutputKeyPrefix,
    OutputKey=OutputKey,
    Config=Config,
    Logger=Logger,
    )
  # Start the processing
  with ctxt as doc:
    doc.Run()
