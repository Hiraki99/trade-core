from sqlalchemy import ForeignKey
from sqlalchemy import asc, desc, func
from sqlalchemy.sql.expression import and_, or_, exists
from sqlalchemy import Column, Integer, Unicode, String, DateTime, Boolean, Numeric, Text, Date, UniqueConstraint, UnicodeText, Index, Float
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
import json
import datetime
from . import Base, db_session
import uuid, random
from ..ulib import obj_to_dict


class BaseModel(object):
    __tablename__ = 'BaseModel'
    id = Column(Integer, primary_key=True)
    live = Column(Boolean,  nullable=False, default=True)
    createAt = Column(DateTime,   nullable=False, default=datetime.datetime.now, index=True)

    @staticmethod
    def to_dict(row):
        d = obj_to_dict(row)
        return d

    def __repr__(self):
        return '<%s is: id= %r>' % (self.__tablename__, self.id)


class Account(BaseModel, Base):
    __tablename__ = 'Account'

    id = Column(Integer, primary_key=True)

    user_id = Column(Integer)
    account_no = Column(String(30), unique=True, nullable=False)
    subkey = Column(String(255),  nullable=False, index=True)
    description = Column(String(255),  nullable=False)
    type = Column(String(10),  nullable=False, index=True)
    currency = Column(String(10),  nullable=False, index=True)
    balance = Column(Float,    nullable=False, default=0, index=True)

    live = Column(Boolean,  nullable=False, default=True)
    createAt = Column(DateTime,   nullable=False, default=datetime.datetime.now, index=True)

    def __init__(self, user_id, account_no, type='0001', description='', currency='VND', balance=0.0):
        self.user_id = user_id
        self.account_no = account_no
        self.subkey = uuid.uuid4().hex # will replace by real subkey later
        self.type = type
        self.description = description
        self.currency = currency
        self.balance = balance

    @staticmethod
    def view_account_all():
        with db_session() as session:
            list_query = session.query(Account).filter(
                Account.live == True).order_by(Account.createAt.desc())
            full_accs = [Account.to_dict(row) for row in list_query]
        return full_accs
    
    @staticmethod
    def view_account(account_no):
        with db_session() as session:
            list_query = session.query(Account).filter(Account.account_no == account_no,
                Account.live == True).order_by(Account.createAt.desc())
            full_accs = [Account.to_dict(row) for row in list_query]
        return full_accs
    
    @staticmethod
    def hide_account(parameter_list):
        pass


class Transaction(BaseModel, Base):
    __tablename__ = 'Transaction'

    id = Column(String(64), primary_key=True)
    user_id = Column(Integer)
    account_id = Column(Integer)
    account_no = Column(String(30), nullable=False)
    approver_name = Column(String(255),nullable=False)

    # 'depo'-deposit, 'with'-withdraw
    kind = Column(String(4),  nullable=False, default='depo')

    type = Column(String(10),  nullable=False, index=True)
    currency = Column(String(10),  nullable=False, index=True)
    # will be negative if transfer money out of this account
    value = Column(Float,    nullable=False, default=0, index=True)
    description = Column(String(255),  nullable=False)
    deposit_method = Column(String(30),  nullable=False)

    # 0-Pending, 1-Unconfirmed, 2-In-progress, 4-Complete, 8-Cancelled
    status = Column(String(1),  nullable=False, default='0', index=True)
    live = Column(Boolean,  nullable=False, default=True)
    createAt = Column(DateTime,   nullable=False, default=datetime.datetime.now, index=True)

    def __init__(self, user_id, account_id, account_no, kind='depo', approver_name='', deposit_method='bank_transfer', type='0001', description='', currency='VND', value=0.0, status=0):
        self.id = uuid.uuid1().hex
        self.user_id = user_id
        self.account_no = account_no
        self.kind = kind
        self.approver_name = approver_name
        self.type = type
        self.description = description
        self.currency = currency
        self.value = value
        self.deposit_method = deposit_method
        self.status = status
    
    @staticmethod
    def view_account_transaction(parameter_list):
        pass

    @staticmethod
    def open_account_transactio(parameter_list):
        pass

    @staticmethod
    def cancel_account_transactio(parameter_list):
        pass

    @staticmethod
    def process_account_transactio(parameter_list):
        pass


    


