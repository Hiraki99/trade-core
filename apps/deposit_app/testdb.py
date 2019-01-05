import os

import logging
import hmac
import base64
import struct
import hashlib
import time
import uuid

import datetime
from . import Base
# from pyblinktrade.utils import smart_str


import random

from sqlalchemy import ForeignKey
from sqlalchemy import desc, func
from sqlalchemy.sql.expression import and_, or_, exists
from sqlalchemy import Column, Integer, Unicode, String, DateTime, Boolean, Numeric, Text, Date, UniqueConstraint, UnicodeText, Index
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
import json

from copy import deepcopy


def get_datetime_now(timezone=None):
  #this is just a workaround, so we can test datetime.datetime.now()
  from trade import get_now
  return get_now(timezone)
class Deposit(Base):
  __tablename__ = 'deposit'

  id = Column(String(25), primary_key=True)
  deposit_option_name = Column(String(15), nullable=False)
  username = Column(String,     nullable=False)
  broker_username = Column(String,     nullable=False)
  broker_deposit_ctrl_num = Column(Integer,    index=True)
  secret = Column(String(10), index=True)

  type = Column(String(3),  nullable=False, index=True)
  currency = Column(String(3),  nullable=False, index=True)
  value = Column(Integer,    nullable=False, default=0, index=True)
  paid_value = Column(Integer,    nullable=False, default=0, index=True)
  # 0-Pending, 1-Unconfirmed, 2-In-progress, 4-Complete, 8-Cancelled
  status = Column(String(1),  nullable=False, default='0', index=True)
  data = Column(Text,       nullable=False, index=True)
  created = Column(DateTime,   nullable=False,
                   default=get_datetime_now, index=True)
  instructions = Column(Text)

  client_order_id = Column(String(30), index=True)

  percent_fee = Column(Numeric,    nullable=False, default=0)
  fixed_fee = Column(Integer,    nullable=False, default=0)

  reason_id = Column(Integer)
  reason = Column(String)
  email_lang = Column(String,     nullable=False)
