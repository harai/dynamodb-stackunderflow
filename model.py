import logging
from pprint import PrettyPrinter

from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

import util

log = logging.getLogger(__name__)
pp = PrettyPrinter(indent=3)


class Sequences(object):

  def __init__(self, db, name):
    self._table = db.Table(util.table_name('sequences'))
    self._name = name

  def next(self):
    v = self._table.update_item(
        Key={'name': self._name},
        AttributeUpdates={'counter': {
            'Value': 1,
            'Action': 'ADD',
        }},
        ReturnValues='UPDATED_NEW')
    return v['Attributes']['counter']

  @classmethod
  def create(cls, db, name):
    s = cls(db, name)
    try:
      s._table.put_item(
          Item={
              'name': name,
              'counter': 0,
          },
          ConditionExpression=Attr('name').not_exists())
    except ClientError as e:
      if util.is_dynamodb_error(e, 'ConditionalCheckFailedException'):
        pass
      else:
        raise e
    return s


class Users(object):

  def __init__(self, db, user_id):
    self._table = db.Table(util.table_name('users'))
    self._user_id = user_id

  @property
  def user_id(self):
    return self._user_id

  @classmethod
  def create(cls, db, user_id, created_at):
    u = cls(db, user_id)
    u._table.put_item(
        Item={
            'user_id': user_id,
            'created_at': util.encode_date(created_at),
        })
    return u


class Questions(object):

  def __init__(self, db, id):
    self._table = db.Table(util.table_name('questions'))
    self._id = id

  @property
  def id(self):
    return self._id

  def get(self):
    v = self._table.get_item(Key={'id': self._id})
    return v['Item']

  @classmethod
  def create(cls, db, sequence, title, body, user, created_at):
    id = sequence.next()
    u = cls(db, id)
    u._table.put_item(
        Item={
            'id': id,
            'title': str(title),
            'body': str(body),
            'user_id': user.user_id,
            'created_at': util.encode_date(created_at),
        })
    return u


class Answers(object):

  def __init__(self, db, question_id, answer_no):
    self._table = self.table(db)
    self._question_id = question_id
    self._answer_no = answer_no

  @classmethod
  def table(cls, db):
    return db.Table(util.table_name('answers'))

  @classmethod
  def find_by_question(cls, db, question):
    v = cls.table(db).query(
        KeyConditionExpression=Key('question_id').eq(question.id),
        ScanIndexForward=False)
    return v['Items']

  @classmethod
  def create(cls, db, question, body, user, created_at):
    no = Sequences.create(db, 'questions.{}'.format(question.id)).next()
    u = cls(db, question.id, no)
    u._table.put_item(
        Item={
            'question_id': question.id,
            'answer_no': no,
            'body': str(body),
            'user_id': user.user_id,
            'created_at': util.encode_date(created_at),
        })
    return u
