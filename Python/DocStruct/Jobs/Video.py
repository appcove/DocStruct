# vim:encoding=utf-8:ts=2:sw=2:expandtab
import re

from os.path import basename
from DocStruct.Base import S3, ElasticTranscoder
from . import Job, RegisterNotificationHandlerForJobId, UnregisterNotificationHandlerForJobId


# def NotificationHandler(msg, *, Config, Logger):
#   # We only have work to do if the job has completed
#   if msg and msg['state'] == 'COMPLETED':
#     Logger.debug("Transcoder Job with ID = {0} has completed".format(msg['jobId']))

#     # Get original object
#     source = S3.GetObject(
#       session=Config.Session,
#       bucket=Config.S3_OutputBucket,
#       key=msg['input']['key'],
#       return_data=False,
#       )

#     if source:
#       source_filename = source["Body"].headers.get("x-amz-meta-filename")
#       if not source_filename:
#         source_filename = msg['input']['key']
#       source_filename = basename(source_filename)

#       # Add Content-Disposition Header to the video & thumbnail file created
#       for output in msg['outputs']:
#         # Try to get the extension for the output file
#         output_extension = re.sub(r'^[^.]*\.', '', output['key'])
#         if not output_extension:
#           output_extension = 'dat'
#         output_extension = '.' + output_extension

#         # Now update the content-disposition header
#         S3.UpdateObject(
#           session=Config.Session,
#           bucket=Config.S3_OutputBucket,
#           key='{0}{1}'.format(msg['outputKeyPrefix'], output['key']),
#           content_disposition='inline; filename="{0}";'.format(re.sub(r'\.[^.]+$', output_extension, source_filename)),
#           )

#         Logger.info("Updated content-disposition header for {0}".format(output['key']))

#     else:
#       Logger.info("Could not get original source object. `Content-Disposition` header could not be added to output.")

#   elif msg['state'] == 'ERROR':
#     Logger.info("ERROR: Transcoder Job with ID = {0} failed.".format(msg['jobId']))
#   else:
#     return None

#   # Write the message to the relevant file in S3
#   ret = S3.PutJSON(
#     session=Config.Session,
#     bucket=Config.S3_OutputBucket,
#     key="{0}output.json".format(msg['outputKeyPrefix']),
#     content=msg
#     )

#   # Unrgister this handler for JobId so that we don't leak memory
#   UnregisterNotificationHandlerForJobId(JobId=msg['jobId'], Logger=Logger)
#   # Return
#   return ret


@Job
def TranscodeVideo(*, InputKey, OutputKeyPrefix, OutputFormats, Config, Logger):
  Logger.debug("TranscodeVideo started for {0}".format(InputKey))
  # Convert formats to output types
  Outputs = []
  for o in OutputFormats:
    if o == 'webm':
      Outputs.append({
        'Key': 'video.webm',
        'PresetId': basename(Config.ElasticTranscoder_WebmPresetArn),
        'ThumbnailPattern': 'thumb_{resolution}_{count}.webm'
        })
    elif o == 'mp4':
      Outputs.append({
        'Key': 'video.mp4',
        'PresetId': basename(Config.ElasticTranscoder_WebPresetArn),
        'ThumbnailPattern': 'thumb_{resolution}_{count}.mp4'
        })
  # Set Pipeline ID
  PipelineId = basename(Config.ElasticTranscoder_PipelineArn)
  # Trigger the transcoding
  ret = ElasticTranscoder.StartTranscoding(
    session=Config.Session,
    pipeline_id=PipelineId,
    input_path=InputKey,
    output_key_prefix=OutputKeyPrefix,
    outputs=Outputs,
    )
  Logger.debug("ElasticTranscoder job created: {0}".format(ret["Job"]["Arn"]))
  # # Register a handler to handle notifications for this job
  # RegisterNotificationHandlerForJobId(JobId=ret['Job']['Id'], Handler=NotificationHandler, Logger=Logger)
  # Now we are ready to return
  return ret
