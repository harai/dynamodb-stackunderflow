import logging
from pprint import PrettyPrinter

from model import Answers, Questions, Sequences, Users

log = logging.getLogger(__name__)
pp = PrettyPrinter(indent=3)


def create_user(db, name, created_at):
  return Users.create(db, name, created_at)


def create_question(db, title, body, user_id, created_at):
  user = Users(db, user_id)
  sequence = Sequences(db, 'questions')
  return Questions.create(db, sequence, title, body, user, created_at)


def add_answer(db, question_id, body, user_id, created_at):
  user = Users(db, user_id)
  question = Questions(db, question_id)
  return Answers.create(db, question, body, user, created_at)


def get_question(db, question_id):
  q = Questions(db, question_id)
  pp.pprint(q.get())
  pp.pprint(Answers.find_by_question(db, q))
