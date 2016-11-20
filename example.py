import logging
from datetime import datetime
from pprint import PrettyPrinter

import boto3

import api
from model import Sequences

log = logging.getLogger(__name__)
pp = PrettyPrinter(indent=3)


def dummy():
  Sequences
  api


def example():
  db = boto3.resource('dynamodb')
  # Sequences.create(db, 'questions')
  # # api.create_user(db, 'akihiro', datetime.now())
  # api.create_question(
  #     db, 'First Question', 'DynamoDB is difficult. Please help!', 'akihiro',
  #     datetime.now())
  # api.add_answer(db, 5, 'ggrks2', 'akihiro', datetime.now())
  api.get_question(db, 5)


if __name__ == '__main__':
  example()
