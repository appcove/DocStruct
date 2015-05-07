# vim:encoding=utf-8:ts=2:sw=2:expandtab
from os.path import basename
from DocStruct.Base import ElasticTranscoder
from . import Job


@Job
def TranscodeAudio(*, InputKey, OutputKeyPrefix, OutputFormats, Config, Logger):
  Logger.debug("TranscodeAudio started for {0}".format(InputKey))
  # Convert formats to output types
  Outputs = []
  for o in OutputFormats:
    PresetConfigProperty = "ElasticTranscoder_{0}PresetArn".format(o.upper())
    Outputs.append({'Key': 'audio.{0}'.format(o), 'PresetId': basename(getattr(Config, PresetConfigProperty))})
  # Set Pipeline ID
  PipelineId = basename(Config.ElasticTranscoder_PipelineArn)
  # Trigger the transcoding
  ret = ElasticTranscoder.StartTranscoding(
    session=Config.Session,
    pipeline_id=PipelineId,
    input_path=InputKey,
    output_key_prefix=OutputKeyPrefix,
    outputs=Outputs
    )
  Logger.debug("ElasticTranscoder job created: {0}".format(ret["Job"]["Arn"]))
  # Now we are ready to return
  return ret
