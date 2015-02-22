# vim:encoding=utf-8:ts=2:sw=2:expandtab
from os.path import basename

#--------------------------------------------------
# ELASTIC TRANSCODER RELATED METHODS
#--------------------------------------------------


WEBM_PRESET_DATA = {
  'container': 'webm',
  'name': 'User preset: Webm',
  'description': 'User preset to convert files to webm',
  'audio': {
    'Codec': 'vorbis',
    'SampleRate': '44100',
    'BitRate': '160',
    'Channels': '2',
    },
  'video': {
    'Codec': 'vp8',
    'CodecOptions': {'Profile': '0'},
    'KeyframesMaxDist': '90',
    'FixedGOP': 'false',
    'BitRate': '2200',
    'FrameRate': '30',
    'MaxWidth': '1280',
    'MaxHeight': '720',
    'SizingPolicy': 'ShrinkToFit',
    'PaddingPolicy': 'NoPad',
    'DisplayAspectRatio': 'auto'
    },
  'thumbnails': {
    'Format': 'png',
    'Interval': '60',
    'MaxWidth': '192',
    'MaxHeight': '108',
    'SizingPolicy': 'ShrinkToFit',
    'PaddingPolicy': 'NoPad'
    }}


def CreatePipeline(*, session, pipelinename, role_arn, inputbucketname, outputbucketname, topic_arn):
  # SEE: https://boto3.readthedocs.org/en/latest/topics/service_names.html#service-names for service names
  etconn = session.connect_to("elastictranscoder")
  Pipelines = session.get_collection("elastictranscoder", "PipelineCollection")
  pipelines = Pipelines(connection=etconn)
  # If pipeline already exists, we don't need to create it.
  # TODO: this is not the cleanest way to achieve this. We need to update this soon.
  for p in pipelines.each():
    if p.name == pipelinename:
      return p
  # If pipeline does not exist, we can create it
  pipeline = pipelines.create(
    name=pipelinename,
    role=role_arn,
    input_bucket=inputbucketname,
    output_bucket=outputbucketname,
    notifications={
      "Error": topic_arn,
      "Warning": topic_arn,
      "Progressing": topic_arn,
      "Completed": topic_arn,
      }
    )
  return pipeline


def StartTranscoding(*, session, pipeline_id, video_path, outputs, output_key_prefix):
  """Start transcoding a video pointed to by keyname

  :param session: The session to user for credentials
  :type session: boto3.session.Session
  :param pipeline_id: Name of the pipeline into which this job will be pushed
  :type pipeline_id: str
  :param video_path: Path to the video file (relative to the input bucket)
  :type video_path: str
  :param outputs: The outputs to which the video file should be transcoded
  :type outputs: list
  :param output_key_prefix: The prefix for output files
  :type output_key_prefix: str
  :return: Info about the newly created job
  :rtype: dict
  """
  etconn = session.connect_to("elastictranscoder")
  Jobs = session.get_collection("elastictranscoder", "JobCollection")
  jobs = Jobs(connection=etconn)
  job = jobs.create(
    pipeline_id=pipeline_id,
    input={"Key": video_path},
    output_key_prefix="{0}/".format(output_key_prefix),
    outputs=outputs)
  # Return info for the job so that we can get status etc...
  return job.get()


def GetTranscodingJobInfo(*, session, jobarn):
  etconn = session.connect_to("elastictranscoder")
  Job = session.get_resource("elastictranscoder", "Job")
  job = Job(connection=etconn, id=basename(jobarn))
  try:
    jobinfo = job.get()
  except ServerError:
    jobinfo = None
  return jobinfo


def GetPresetWithName(*, session, presetname):
  etconn = session.connect_to("elastictranscoder")
  Presets = session.get_collection("elastictranscoder", "PresetCollection")
  presets = Presets(connection=etconn)
  # We need to go over the presets to find if it exists already
  for presetmeta in presets.each():
    if presetmeta.name.strip() == presetname:
      Preset = session.get_resource("elastictranscoder", "Preset")
      preset = Preset(id=presetmeta.id)
      presetinfo = preset.get()
      return presetinfo['Preset']['Arn']
  # If we found nothing, just return an empty string
  return ''


def CreatePreset(*, session, presetdata):
  etconn = session.connect_to("elastictranscoder")
  Presets = session.get_collection("elastictranscoder", "PresetCollection")
  presets = Presets(connection=etconn)
  preset = presets.create(**presetdata)
  return preset.arn
