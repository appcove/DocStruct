# vim:encoding=utf-8:ts=2:sw=2:expandtab



'''
A DocStruct_Release table in the schema

Every time we do an upgrade, we'll replace the column in this table with the correct version number.

ALTER TABLE DocStruct_Release RENAME 1.0.1 TO 1.0.2 ... this will fail if you are out of sync.

'''


from .Client import Client











