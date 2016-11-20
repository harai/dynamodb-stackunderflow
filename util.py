import logging
from datetime import datetime
from pprint import PrettyPrinter

from botocore.exceptions import ClientError

log = logging.getLogger(__name__)
pp = PrettyPrinter(indent=3)


def table_name(name):
  return 'harai-stackunderflow-{}'.format(name)


def provisioned_throughput(read=1, write=1):
  return {
      'ReadCapacityUnits': read,
      'WriteCapacityUnits': write,
  }


def attribute_definition(name, type):
  return {
      'AttributeName': name,
      'AttributeType': type,
  }


def hash_key(name):
  return {
      'AttributeName': name,
      'KeyType': 'HASH',
  }


def range_key(name):
  return {
      'AttributeName': name,
      'KeyType': 'RANGE',
  }


def project_all():
  return {'ProjectionType': 'ALL'}


def lsi(IndexName, KeySchema, Projection):
  return {
      'IndexName': IndexName,
      'KeySchema': KeySchema,
      'Projection': Projection,
  }


def gsi(IndexName, KeySchema, Projection, ProvisionedThroughput):
  return {
      'IndexName': IndexName,
      'KeySchema': KeySchema,
      'Projection': Projection,
      'ProvisionedThroughput': ProvisionedThroughput,
  }


def table_exists(db, name):
  try:
    db.Table(name).load()
  except ClientError as e:
    if is_dynamodb_error(e, 'ResourceNotFoundException'):
      return False
    raise e
  return True


def create_table_if_not_exist(db, TableName, *args, **kwargs):
  if not table_exists(db, TableName):
    db.create_table(TableName=TableName, *args, **kwargs)


def delete_table_if_exists(db, name):
  if table_exists(db, name):
    db.Table(name).delete()


def encode_date(date):
  return int(date.timestamp())


def decode_date(timestamp):
  return datetime.fromtimestamp(timestamp)


def is_dynamodb_error(e, name):
  return e.response.get('Error', {}).get('Code') == name
