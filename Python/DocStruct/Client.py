# vim:encoding=utf-8:ts=2:sw=2:expandtab


from . import AWS as AWSModule


class Client():

  SchemaVersion = 100

  def __init__(self, *, Config, GetDB, Schema):
    self.GetDB = GetDB
    self.Schema = Schema
    
    # TODO: this should be converted to it's own object that validates the incoming dict
    self.Config = Config

    # One of the things to do here, because this should only run once per thread init,
    # is to check the Version of the Schema in the current DB connection and verify
    # that our code and the DB schema are in sync.




  def DoSomething(self):
    AWSModule.DoSomething(self)

