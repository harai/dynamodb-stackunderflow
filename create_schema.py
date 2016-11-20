import logging
from pprint import PrettyPrinter

import boto3

import util
from model import Sequences

log = logging.getLogger(__name__)
pp = PrettyPrinter(indent=3)


def create_schema(db):
  util.create_table_if_not_exist(
      db,
      TableName=util.table_name('sequences'),
      KeySchema=[util.hash_key('name')],
      AttributeDefinitions=[util.attribute_definition('name', 'S')],
      ProvisionedThroughput=util.provisioned_throughput())

  util.create_table_if_not_exist(
      db,
      TableName=util.table_name('questions'),
      KeySchema=[util.hash_key('id')],
      AttributeDefinitions=[util.attribute_definition('id', 'N')],
      ProvisionedThroughput=util.provisioned_throughput())

  util.create_table_if_not_exist(
      db,
      TableName=util.table_name('answers'),
      KeySchema=[util.hash_key('question_id'), util.range_key('answer_no')],
      AttributeDefinitions=[
          util.attribute_definition('question_id', 'N'),
          util.attribute_definition('answer_no', 'N'),
          util.attribute_definition('upvote', 'N'),
      ],
      LocalSecondaryIndexes=[
          util.lsi(
              IndexName='useful_answers',
              KeySchema=[
                  util.hash_key('question_id'), util.range_key('upvote')
              ],
              Projection=util.project_all())
      ],
      ProvisionedThroughput=util.provisioned_throughput())

  util.create_table_if_not_exist(
      db,
      TableName=util.table_name('upvotes'),
      KeySchema=[util.hash_key('user_id'), util.range_key('question_id')],
      AttributeDefinitions=[
          util.attribute_definition('user_id', 'S'),
          util.attribute_definition('question_id', 'N'),
      ],
      ProvisionedThroughput=util.provisioned_throughput())

  util.create_table_if_not_exist(
      db,
      TableName=util.table_name('users'),
      KeySchema=[util.hash_key('user_id')],
      AttributeDefinitions=[util.attribute_definition('user_id', 'S')],
      ProvisionedThroughput=util.provisioned_throughput())

  util.create_table_if_not_exist(
      db,
      TableName=util.table_name('tags'),
      KeySchema=[util.hash_key('name')],
      AttributeDefinitions=[util.attribute_definition('name', 'S')],
      ProvisionedThroughput=util.provisioned_throughput())

  util.create_table_if_not_exist(
      db,
      TableName=util.table_name('question_tags'),
      KeySchema=[util.hash_key('tag_name')],
      AttributeDefinitions=[
          util.attribute_definition('tag_name', 'S'),
          util.attribute_definition('question_id', 'N')
      ],
      GlobalSecondaryIndexes=[
          util.gsi(
              IndexName='tagged_questions',
              KeySchema=[util.hash_key('question_id')],
              Projection=util.project_all(),
              ProvisionedThroughput=util.provisioned_throughput())
      ],
      ProvisionedThroughput=util.provisioned_throughput())


def wait(db):
  c = db.meta.client
  tables = [
      'sequences',
      'questions',
      'answers',
      'upvotes',
      'users',
      'tags',
      'question_tags',
  ]
  for t in tables:
    c.get_waiter('table_exists').wait(TableName=util.table_name(t))


def populate_init_data(db):
  Sequences.new(db, 'questions')


if __name__ == '__main__':
  dynamodb = boto3.resource('dynamodb')
  create_schema(dynamodb)
  wait(dynamodb)
  populate_init_data(dynamodb)
