# vim:encoding=utf-8:ts=2:sw=2:expandtab
from abc import ABCMeta
from .Base import GetSession, S3


class Config(object):
  """Wraps around a config file to provide easy access and read/write capabilities"""

  __metaclass__ = ABCMeta

  _directprops = ('Session',)

  def __init__(self, *, CredsFilePath):
    self.Session = GetSession(CredsFilePath=CredsFilePath) if isinstance(CredsFilePath, str) else CredsFilePath

  @property
  def Data(self):
    try:
      ret = object.__getattribute__(self, "__Data")
    except AttributeError:
      ret = self._get_config_from_s3() or {}
      object.__setattr__(self, "__Data", ret)
    return ret

  def __getattr__(self, name):
    key, d = self._get_data_and_key(name)
    return d.get(key)

  def __setattr__(self, name, value):
    if name in self._directprops:
      object.__setattr__(self, name, value)
    else:
      key, d = self._get_data_and_key(name, set_subdict=True)
      d[key] = value
      object.__setattr__(self, 'haschanges', True)
    return value

  def _get_data_and_key(self, name, set_subdict=False):
    d = self.Data
    key = name
    if name.find("_") > -1:
      parts = name.split("_")
      tmp = d.get(parts[0], {})
      if set_subdict:
        d[parts[0]] = tmp
      d = tmp
      key = "_".join(parts[1:])
    return key, d

  def _build_config_file_key(self):
    raise NotImplementedError()

  def _get_bucket_name(self):
    raise NotImplementedError()

  def _get_config_from_s3(self):
    key = self._build_config_file_key()
    # Get the file from S3
    # Convert data to a dict
    # Set it to the data field
    return S3.GetJSON(session=self.Session, bucket=self._get_bucket_name(), key=key)

  def Save(self):
    haschanges = False
    try:
      haschanges = object.__getattribute__(self, 'haschanges')
    except AttributeError:
      pass
    if haschanges:
      d = self.Data.copy()
      if 'Status' in d:
        del d['Status']
      S3.PutJSON(session=self.Session, bucket=self._get_bucket_name(), key=self._build_config_file_key(), content=d)
    return self


class EnvironmentConfig(Config):
  """Wraps around an environment's config file"""

  _directprops = ('EnvironmentID',) + Config._directprops

  def __init__(self, *, EnvironmentID, **kwargs):
    super().__init__(**kwargs)
    self.EnvironmentID = EnvironmentID

  def _build_config_file_key(self):
    return "DocStruct/environment.json"

  def _get_bucket_name(self):
    return self.EnvironmentID


class ApplicationConfig(EnvironmentConfig):
  """Wraps around an application's config file"""

  _directprops = ('ApplicationID', 'EnvironmentConfig') + EnvironmentConfig._directprops

  def __init__(self, *, ApplicationID, **kwargs):
    super().__init__(**kwargs)
    self.ApplicationID = ApplicationID
    object.__setattr__(self, 'EnvironmentConfig', EnvironmentConfig(CredsFilePath=self.Session, EnvironmentID=self.EnvironmentID))

  def _build_config_file_key(self):
    return "{0}/DocStruct/application.json".format(self.ApplicationID)

  def _get_bucket_name(self):
    return self.EnvironmentID


class ReadOnlyConfig(Config):
  """Implements a read only config"""
  
  def __init__(self, *, Data):
    session = GetSession(AccessKey=Data['User']['AccessKey'], SecretKey=Data['User']['SecretKey'])
    super().__init__(CredsFilePath=session)
    object.__setattr__(self, '__Data', Data)

  def Save(self):
    raise Exception("Cannot save this read-only config")
