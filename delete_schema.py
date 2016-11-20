import logging
from pprint import PrettyPrinter

import boto3

import util

log = logging.getLogger(__name__)
pp = PrettyPrinter(indent=3)


def delete_schema():
  dynamodb = boto3.resource('dynamodb')

  util.delete_table_if_exists(dynamodb, util.table_name('sequences'))
  util.delete_table_if_exists(dynamodb, util.table_name('questions'))
  util.delete_table_if_exists(dynamodb, util.table_name('answers'))
  util.delete_table_if_exists(dynamodb, util.table_name('upvotes'))
  util.delete_table_if_exists(dynamodb, util.table_name('users'))
  util.delete_table_if_exists(dynamodb, util.table_name('tags'))
  util.delete_table_if_exists(dynamodb, util.table_name('question_tags'))


if __name__ == '__main__':
  delete_schema()
