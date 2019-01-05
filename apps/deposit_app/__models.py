import os

import logging
import hmac
import base64
import struct
import hashlib
import time
import uuid

import datetime
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
# from  ..pyblinktrade.json_encoder import JsonEncoder

Base = declarative_base()

from .trade_application import TradeApplication

from sqlalchemy.ext.declarative import DeclarativeMeta

import onetimepass

# noinspection PyPackageRequirements
class Withdraw(Base):
  __tablename__ = 'withdraws'
  id = Column(Integer,       primary_key=True)
  user_id = Column(Integer,       ForeignKey('users.id'))
  account_id = Column(Integer,       ForeignKey('users.id'))
  broker_id = Column(Integer,       ForeignKey('users.id'))
  broker_username = Column(String,        nullable=False, index=True)
  username = Column(String,        nullable=False, index=True)
  currency = Column(String,        nullable=False, index=True)
  amount = Column(Integer,       nullable=False, index=True)

  method = Column(String,        nullable=False, index=True)
  data = Column(Text,          nullable=False, index=True)

  confirmation_token = Column(String,     index=True, unique=True)
  status = Column(String(1),     nullable=False, default='0', index=True)
  created = Column(DateTime,      nullable=False,
                   default=get_datetime_now, index=True)
  reason_id = Column(Integer)
  reason = Column(String)
  email_lang = Column(String)

  client_order_id = Column(String(30), index=True)

  percent_fee = Column(Numeric,    nullable=False, default=0)
  fixed_fee = Column(Integer,    nullable=False, default=0)
  paid_amount = Column(Integer,    nullable=False, default=0, index=True)

  def as_dict(self):
    import json
    obj = {c.name: getattr(self, c.name) for c in self.__table__.columns}
    obj.update(json.loads(self.data))
    return obj

  def confirm_using_second_factor(self, session):
    if self.status != '0':
      return False

    self.status = '1'
    session.add(self)
    WithdrawTrustedRecipients.add(session, self)
    session.flush()

    return True

  @staticmethod
  def user_confirm(session, confirmation_token=None):
    withdraw_data = session.query(Withdraw).filter_by(
        confirmation_token=confirmation_token).filter_by(status=0).first()
    if not withdraw_data:
      return None

    withdraw_data.status = '1'
    session.add(withdraw_data)
    WithdrawTrustedRecipients.add(session, withdraw_data)
    session.flush()

    return withdraw_data

  def set_in_progress(self, session, percent_fee, fixed_fee, data, broker_fees_account):
    if self.status != '1':
      return False

    new_data = {}
    new_data.update(json.loads(self.data))
    if data:
      new_data.update(data)
      if self.data != json.dumps(new_data):
        self.data = json.dumps(new_data)

    self.percent_fee = percent_fee
    self.fixed_fee = fixed_fee

    self.paid_amount = self.amount
    if percent_fee > 0 or fixed_fee > 0:
      self.paid_amount = (
          self.amount / (100.0 - float(self.percent_fee)) * 100.0) + self.fixed_fee

    current_balance = Balance.get_balance(
        session, self.account_id, self.broker_id, self.currency)
    if self.paid_amount > current_balance:
      self.cancel(session, -1, None, broker_fees_account)  # Insufficient funds
      return True

    # User won't be able to withdraw his funds if he has any unconfirmed bitcoin deposits
    # This will only be a issue in case of a double spend attack.
    current_positions = Position.get_positions_by_account_broker(
        session, self.account_id, self.broker_id)
    for position in current_positions:
      if position.position != 0:
        # User has deposits that are not yet confirmed
        self.cancel(session, -8, None, broker_fees_account)
        return True

    self.status = '2'
    Ledger.transfer(session,
                    self.account_id,        # from_account_id
                    self.username,          # from_account_name
                    self.broker_id,         # from_broker_id
                    self.broker_username,   # from_broker_name
                    self.broker_id,         # to_account_id
                    self.broker_username,   # to_account_name
                    self.broker_id,         # to_broker_id
                    self.broker_username,   # to_broker_name
                    self.currency,          # currency
                    self.amount,            # amount
                    str(self.id),           # reference
                    'W'                     # descriptions
                    )

    total_fees = self.paid_amount - self.amount

    if total_fees:
      Ledger.transfer(session,
                      self.account_id,        # from_account_id
                      self.username,          # from_account_name
                      self.broker_id,         # from_broker_id
                      self.broker_username,   # from_broker_name
                      broker_fees_account[0],  # to_account_id
                      broker_fees_account[1],  # to_account_name
                      self.broker_id,         # to_broker_id
                      self.broker_username,   # to_broker_name
                      self.currency,          # currency
                      total_fees,             # amount
                      str(self.id),           # reference
                      'WF'                    # descriptions
                      )

      for x in xrange(2, len(broker_fees_account)):
        fwd_fee_account = broker_fees_account[x]
        fwd_fee = total_fees * fwd_fee_account[2]
        Ledger.transfer(session,
                        broker_fees_account[0],  # from_account_id
                        broker_fees_account[1],  # from_account_name
                        self.broker_id,         # from_broker_id
                        self.broker_username,   # from_broker_name
                        fwd_fee_account[0],     # to_account_id
                        fwd_fee_account[1],     # to_account_name
                        self.broker_id,         # to_broker_id
                        self.broker_username,   # to_broker_name
                        self.currency,          # currency
                        fwd_fee,                # amount
                        str(self.id),           # reference
                        'WF'                    # descriptions
                        )

    session.add(self)
    session.flush()

    formatted_amount = Currency.format_number(
        session, self.currency, self.amount / 1.e8)
    template_name = "withdraw-progress"
    template_parameters = self.as_dict()
    template_parameters['amount'] = formatted_amount

    UserEmail.create(session=session,
                     user_id=self.user_id,
                     broker_id=self.broker_id,
                     subject="WP",
                     template=template_name,
                     language=self.email_lang,
                     params=json.dumps(template_parameters, cls=JsonEncoder))

    return True

  def set_as_complete(self, session, data, broker_fees_account):
    if self.status != '2':
      return False

    new_data = {}
    new_data.update(json.loads(self.data))
    if data:
      new_data.update(data)
      if self.data != json.dumps(new_data):
        self.data = json.dumps(new_data)

    self.status = '4'  # COMPLETE

    session.add(self)
    session.flush()

    formatted_amount = Currency.format_number(
        session, self.currency, self.amount / 1.e8)
    template_name = "withdraw-complete"
    template_parameters = self.as_dict()
    template_parameters['amount'] = formatted_amount

    UserEmail.create(session=session,
                     user_id=self.user_id,
                     broker_id=self.broker_id,
                     subject="WF",
                     template=template_name,
                     language=self.email_lang,
                     params=json.dumps(template_parameters, cls=JsonEncoder))

    return True

  def cancel(self, session, reason_id, reason, broker_fees_account):
    if self.status == '4':
      return False

    if self.status == '2':  # in progress or completed
      #revert the transaction
      Ledger.transfer(session,
                      self.broker_id,         # from_account_id
                      self.broker_username,   # from_account_name
                      self.broker_id,         # from_broker_id
                      self.broker_username,   # from_broker_name
                      self.account_id,        # to_account_id
                      self.username,          # to_account_name
                      self.broker_id,         # to_broker_id
                      self.broker_username,   # to_broker_name
                      self.currency,          # currency
                      self.amount,            # amount
                      str(self.id),           # reference
                      'W'                     # descriptions
                      )

      total_fees = self.paid_amount - self.amount
      if total_fees:
        Ledger.transfer(session,
                        broker_fees_account[0],  # from_account_id
                        broker_fees_account[1],  # from_account_name
                        self.broker_id,         # from_broker_id
                        self.broker_username,   # from_broker_name
                        self.account_id,        # to_account_id
                        self.username,          # to_account_name
                        self.broker_id,         # to_broker_id
                        self.broker_username,   # to_broker_name
                        self.currency,          # currency
                        total_fees,             # amount
                        str(self.id),           # reference
                        'WF'                    # descriptions
                        )

        for x in xrange(2, len(broker_fees_account)):
          fwd_fee_account = broker_fees_account[x]
          fwd_fee = total_fees * fwd_fee_account[2]
          Ledger.transfer(session,
                          fwd_fee_account[0],     # from_account_id
                          fwd_fee_account[1],     # from_account_name
                          self.broker_id,         # from_broker_id
                          self.broker_username,   # from_broker_name
                          broker_fees_account[0],  # to_account_id
                          broker_fees_account[1],  # to_account_name
                          self.broker_id,         # to_broker_id
                          self.broker_username,   # to_broker_name
                          self.currency,          # currency
                          fwd_fee,                # amount
                          str(self.id),           # reference
                          'WF'                    # descriptions
                          )

    self.status = '8'  # CANCELLED
    self.reason_id = reason_id
    self.reason = reason

    session.add(self)
    session.flush()

    formatted_amount = Currency.format_number(
        session, self.currency, self.amount / 1.e8)

    balance = Balance.get_balance(
        session, self.account_id, self.broker_id, self.currency)
    formatted_balance = Currency.format_number(
        session, self.currency, balance / 1.e8)

    template_name = "withdraw-cancelled"
    template_parameters = self.as_dict()
    template_parameters['amount'] = formatted_amount
    template_parameters['reason_id'] = reason_id
    if reason_id == -1:
      template_parameters['reason'] = 'INSUFFICIENT_FUNDS'
    elif reason_id == -2:
      template_parameters['reason'] = 'ACCOUNT_NOT_VERIFIED'
    elif reason_id == -3:
      template_parameters['reason'] = 'SUSPICION_OF_FRAUD'
    elif reason_id == -4:
      template_parameters['reason'] = 'DIFFERENT_ACCOUNT'
    elif reason_id == -5:
      template_parameters['reason'] = 'INVALID_WALLET'
    elif reason_id == -6:
      template_parameters['reason'] = 'INVALID_BANK_ACCOUNT'
    elif reason_id == -7:
      template_parameters['reason'] = 'OVER_LIMIT'
    elif reason_id == -8:
      template_parameters['reason'] = 'HAS_UNCONFIRMED_DEPOSITS'
    else:
      template_parameters['reason'] = reason if reason is not None else ''

    template_parameters['balance'] = formatted_balance

    UserEmail.create(session=session,
                     user_id=self.user_id,
                     broker_id=self.broker_id,
                     subject="WC",
                     template=template_name,
                     language=self.email_lang,
                     params=json.dumps(template_parameters, cls=JsonEncoder))
    return True

  @staticmethod
  def get_withdraw(session, withdraw_id):
    return session.query(Withdraw).filter_by(id=withdraw_id).first()

  @staticmethod
  def get_list(session, broker_id, account_id, status_list, page_size, offset, filter_array):
    query = session.query(Withdraw).filter(Withdraw.status.in_(
        status_list)).filter(Withdraw.broker_id == broker_id)

    if account_id:
      query = query.filter(Withdraw.account_id == account_id)

    for filter in filter_array:
      if filter:
        if filter.isdigit():
          query = query.filter(or_(Withdraw.data.like('%' + filter + '%'),
                                   Withdraw.currency == filter,
                                   Withdraw.amount == int(filter) * 1e8,
                                   ))
        else:
          query = query.filter(or_(Withdraw.data.like('%' + filter + '%'),
                                   Withdraw.currency == filter))

    query = query.order_by(Withdraw.created.desc())

    if page_size:
      query = query.limit(page_size)
    if offset:
      query = query.offset(offset)

    return query

  @staticmethod
  def create(session, user, broker,  currency, amount, method, data, client_order_id, email_lang,
             percent_fee, fixed_fee):
    import uuid
    confirmation_token = uuid.uuid4().hex

    if not user.has_instant_withdrawal:
      new_data = json.loads(data)
      new_data["Instant"] = 'NO'
      data = json.dumps(new_data)

    withdraw_record = Withdraw(user_id=user.id,
                               account_id=user.id,
                               username=user.username,
                               broker_id=user.broker_id,
                               broker_username=user.broker_username,
                               method=method,
                               currency=currency,
                               amount=amount,
                               email_lang=email_lang,
                               confirmation_token=confirmation_token,
                               percent_fee=percent_fee,
                               fixed_fee=fixed_fee,
                               client_order_id=client_order_id,
                               data=data)

    is_crypto_currency = Currency.get_currency(session, currency).is_crypto

    if is_crypto_currency and \
            user.withdraw_email_validation and \
            not WithdrawTrustedRecipients.is_trusted(session, withdraw_record):

      if not user.two_factor_enabled:
        formatted_amount = Currency.format_number(
            session, withdraw_record.currency, withdraw_record.amount / 1.e8)

        template_name = 'withdraw-confirmation'
        template_parameters = withdraw_record.as_dict()
        template_parameters['amount'] = formatted_amount
        template_parameters['created'] = get_datetime_now()

        UserEmail.create(session=session,
                         user_id=user.id,
                         broker_id=user.broker_id,
                         subject="CW",
                         template=template_name,
                         language=email_lang,
                         params=json.dumps(template_parameters, cls=JsonEncoder))
    else:
      if user.withdraw_email_validation:
        WithdrawTrustedRecipients.add(session, withdraw_record)

      withdraw_record.status = '1'

    session.add(withdraw_record)
    session.flush()

    return withdraw_record

  def __repr__(self):
    return u"<Withdraw(id=%r, user_id=%r, account_id=%r, broker_id=%r, username=%r, currency=%r, method=%r, amount=%r, "\
           u"broker_username=%r, data=%r, percent_fee=%r, fixed_fee=%r, "\
           u"confirmation_token=%r, status=%r, created=%r, reason_id=%r, reason=%r, paid_amount=%r, email_lang=%r)>" % (
               self.id, self.user_id, self.account_id, self.broker_id, self.username, self.currency, self.method, self.amount,
               self.broker_username, self.data, self.percent_fee, self.fixed_fee,
               self.confirmation_token, self.status, self.created, self.reason_id, self.reason, self.paid_amount, self.email_lang)




class Deposit(Base):
  __tablename__ = 'deposit'

  id = Column(String(25), primary_key=True)
  user_id = Column(Integer,    ForeignKey('users.id'))
  account_id = Column(Integer,    ForeignKey('users.id'))
  broker_id = Column(Integer,    ForeignKey('users.id'))
  deposit_option_id = Column(Integer,    ForeignKey('deposit_options.id'))
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

  def __repr__(self):
    return u"<Deposit(id=%r, user_id=%r, account_id=%r, broker_id=%r, deposit_option_id=%r, "\
           u"deposit_option_name=%r, username=%r, broker_username=%r,  broker_deposit_ctrl_num=%r,"\
           u"secret=%r, type=%r, currency=%r, value=%r, paid_value=%r, status=%r, "\
           u"data=%r, created=%r, reason_id=%r, reason=%r, email_lang=%r,"\
           u"fixed_fee=%r, percent_fee=%r, client_order_id=%r, instructions=%r)>" % (
               self.id,  self.user_id, self.account_id, self.broker_id, self.deposit_option_id,
               self.deposit_option_name,  self.username, self.broker_username, self.broker_deposit_ctrl_num,
               self.secret, self.type,  self.currency, self.value, self.paid_value, self.status,
               self.data, self.created, self.reason_id, self.reason, self.email_lang,
               self.fixed_fee, self.percent_fee, self.client_order_id, self.instructions)

  def as_dict(self):
    import json
    obj = {c.name: getattr(self, c.name) for c in self.__table__.columns}
    obj.update(json.loads(self.data))
    return obj

  @staticmethod
  def create_crypto_currency_deposit(session, user, currency, input_address, destination, secret, client_order_id, instructions=None, value=None):
    import uuid
    deposit_id = uuid.uuid4().hex

    deposit = Deposit(
        id=deposit_id,
        deposit_option_name='deposit_' + currency.lower(),
        user_id=user.id,
        account_id=user.id,
        username=user.username,
        broker_username=user.broker_username,
        broker_id=user.broker_id,
        email_lang=user.email_lang,
        type='CRY',
        currency=currency,
        secret=secret,
        percent_fee=0.,
        fixed_fee=0,
        data=json.dumps({'InputAddress': input_address,
                         'Destination': destination}),
    )

    if instructions:
      deposit.instructions = json.dumps(instructions)

    if client_order_id:
      deposit.client_order_id = client_order_id

    if value:
      deposit.value = value

    session.add(deposit)

    session.flush()

    return deposit

  @staticmethod
  def get_list(session, broker_id, account_id, status_list, page_size, offset, filter_array=[]):
    query = session.query(Deposit).filter(Deposit.status.in_(
        status_list)).filter(Deposit.broker_id == broker_id)

    if account_id:
      query = query.filter(Deposit.account_id == account_id)

    if filter_array:
      for filter in filter_array:
        if filter.isdigit():
          query = query.filter(or_(Deposit.data.like('%' + filter + '%'),
                                   Deposit.currency == filter,
                                   Deposit.deposit_option_name == filter,
                                   Deposit.value == int(filter) * 1e8,
                                   Deposit.paid_value == int(filter) * 1e8,
                                   Deposit.broker_deposit_ctrl_num == int(
              filter),
          ))
        else:
          query = query.filter(or_(Deposit.data.like('%' + filter + '%'),
                                   Deposit.currency == filter,
                                   Deposit.deposit_option_name == filter))

    query = query.order_by(Deposit.created.desc())

    if page_size:
      query = query.limit(page_size)
    if offset:
      query = query.offset(offset)

    return query

  @staticmethod
  def get_deposit_list_by_secret(session, secret):
    q = session.query(Deposit).filter_by(secret=secret)
    return q

  @staticmethod
  def get_deposit(session, deposit_id=None, secret=None, broker_id=None, broker_control_number=None):
    q = session.query(Deposit)
    if deposit_id:
      q = q.filter_by(id=deposit_id)
    if secret:
      q = q.filter_by(secret=secret)
    if broker_id:
      q = q.filter_by(broker_id=broker_id)
    if broker_control_number:
      q = q.filter_by(broker_deposit_ctrl_num=broker_control_number)
    return q.first()

  def cancel(self, session, reason_id, reason=None):
    if self.status == '4':
      Ledger.transfer(session,
                      self.account_id,        # from_account_id
                      self.username,          # from_account_name
                      self.broker_id,         # from_broker_id
                      self.broker_username,   # from_broker_name
                      self.broker_id,         # to_account_id
                      self.broker_username,   # to_account_name
                      self.broker_id,         # to_broker_id
                      self.broker_username,   # to_broker_name
                      self.currency,          # currency
                      self.paid_value,        # amount
                      str(self.id),           # reference
                      'D'                     # descriptions
                      )

    self.status = '8'
    self.reason_id = reason_id
    self.reason = reason
    session.add(self)
    session.flush()

  def user_confirm(self, session, data=None):
    if self.status != '0':
      return

    new_data = {}
    new_data.update(json.loads(self.data))
    if data:
      new_data.update(data)
      if self.data != json.dumps(new_data):
        self.data = json.dumps(new_data)

    self.status = '1'

    session.add(self)
    session.flush()

  def set_in_progress(self, session, data=None):
    if self.status == '4' or self.status == '2':
      return

    self.status = '2'
    session.add(self)
    session.flush()

    # Send deposit-progress email only for Non crypto currencies deposits.
    if self.type != 'CRY':
      template_name = "deposit-progress"
      template_parameters = self.as_dict()

      if self.value:
        formatted_value = Currency.format_number(
            session, self.currency, self.value / 1.e8)
        template_parameters['value'] = formatted_value

      if self.paid_value:
        formatted_paid_value = Currency.format_number(
            session, self.currency, self.paid_value / 1.e8)
        template_parameters['paid_value'] = formatted_paid_value

      UserEmail.create(session=session,
                       user_id=self.user_id,
                       broker_id=self.broker_id,
                       subject="DP",
                       template=template_name,
                       language=self.email_lang,
                       params=json.dumps(template_parameters, cls=JsonEncoder))

  def match_deposit_data(self, session, amount, data):
    # let's compute the new data
    new_data = {}
    current_data = json.loads(self.data)
    new_data.update(current_data)
    if data:
      new_data.update(data)

    if self.status != '0' and self.type == 'CRY':
      if self.paid_value != amount:
        return False

      if 'InputTransactionHash' in current_data and  \
         'InputTransactionHash' in new_data and  \
              current_data['InputTransactionHash'] != new_data['InputTransactionHash']:
        return False

    return True

  def process_confirmation(self, session, amount, percent_fee=0., fixed_fee=0, data=None):
    should_update = False
    should_confirm = False
    should_start_a_loan_from_broker_to_the_user = False
    broker = None
    user = None
    broker_crypto_currencies = None
    crypto_currency_param = None

    # let's compute the new data
    new_data = {}
    current_data = json.loads(self.data)
    new_data.update(current_data)
    if data:
      new_data.update(data)
      if self.data != json.dumps(new_data):
        # let's update the deposit record in case the data section has changed.
        should_update = True

    # let's check if the crypto currency confirmation data is valid and if we should confirm
    if self.type == 'CRY':
      if not broker:
        broker = Broker.get_broker(session, self.broker_id)
      if broker_crypto_currencies is None:
        broker_crypto_currencies = json.loads(broker.crypto_currencies)

      # check if the broker has a wallet for this crypto currency
      for crypto_currency_param in broker_crypto_currencies:
        if crypto_currency_param["CurrencyCode"] == self.currency:
          break

      if crypto_currency_param is None:
        # The broker doesn't have a wallet for this crypto currency. This is probably an attack.
        return

      for amount_start, amount_end, confirmations in crypto_currency_param["Confirmations"]:
        if amount_start < amount <= amount_end and data['Confirmations'] >= confirmations:
          should_confirm = True

    else:
      should_confirm = True

    # let's check if the user is reusing the same bitcoin in the same address
    if self.status == '4' and self.type == 'CRY':
      # the deposit has been already confirmed .... nothing to do here. This is probably an attack
      return

    self.paid_value = amount
    loan_amount = self.paid_value
    if self.type == 'CRY' and data:
      if not broker:
        broker = Broker.get_broker(session, self.broker_id)

      # Check if we should give a loan to the user on the 0 confirmation.
      if not data['Confirmations'] and self.status == '0':
        if not user:
          user = User.get_user(
              session, broker_id=self.broker_id, user_id=self.user_id)

        max_loan_amount = 0
        for amount_start, amount_end, confirmations in crypto_currency_param["Confirmations"]:
          if confirmations >= 1:
            max_loan_amount = amount_end
            break
        if loan_amount > max_loan_amount:
          loan_amount = max_loan_amount

        if not should_confirm:
          # Give a loan to verified users who included a miners fee
          # Higher than the minimum fee
          if user.verified >= 3 and 'InputFee' in data and data['InputFee'] >= 10000:
            should_start_a_loan_from_broker_to_the_user = True

          # or confirm the deposit in case is comming from a green address
          if 'PayeeAddresses' in data:
            try:
              payee_addresses = json.loads(data['PayeeAddresses'])

              if len(payee_addresses) == 1:
                payee_address = payee_addresses[0]

                if payee_address:
                  if GreenAddresses.is_green_address(session, payee_address, self.currency):
                    if 'InputFee' in data and data['InputFee'] > 1000:
                      should_confirm = True
                      should_start_a_loan_from_broker_to_the_user = False
            except Exception:
              pass

      if self.status == '0' or self.status == '1':
        self.status = '2'
        should_update = True

    should_execute_instructions = False
    should_adjust_ledger = False
    if (should_confirm and self.status != '4') or should_start_a_loan_from_broker_to_the_user:
      self.paid_value = amount
      self.percent_fee = percent_fee
      self.fixed_fee = fixed_fee

      if not should_start_a_loan_from_broker_to_the_user:
        self.status = '4'

      if self.instructions:
        should_execute_instructions = True
      should_adjust_ledger = True
      should_update = True

    if should_start_a_loan_from_broker_to_the_user:
      PositionLedger.transfer(session,
                              self.account_id,        # from_account_id
                              self.username,          # from_account_name
                              self.broker_id,         # from_broker_id
                              self.broker_username,   # from_broker_name
                              self.broker_id,         # to_account_id
                              self.broker_username,   # to_account_name
                              self.broker_id,         # to_broker_id
                              self.broker_username,   # to_broker_name
                              self.currency,          # currency
                              loan_amount,            # amount
                              str(self.id),           # reference
                              'D'                     # descriptions
                              )

    if should_adjust_ledger:
      should_payback_the_loan_from_broker_to_the_user = False
      position = 0
      if not should_start_a_loan_from_broker_to_the_user:
        position = Position.get_position(
            session, self.account_id, self.broker_id, self.currency)
        if position < 0:
          should_payback_the_loan_from_broker_to_the_user = True
          position *= -1

      if should_payback_the_loan_from_broker_to_the_user:
        val = self.paid_value - position
        if val <= 0:
          PositionLedger.transfer(session,
                                  self.broker_id,         # from_account_id
                                  self.broker_username,   # from_account_name
                                  self.broker_id,         # from_broker_id
                                  self.broker_username,   # from_broker_name
                                  self.account_id,        # to_account_id
                                  self.username,          # to_account_name
                                  self.broker_id,         # to_broker_id
                                  self.broker_username,   # to_broker_name
                                  self.currency,          # currency
                                  self.paid_value,        # amount
                                  str(self.id),           # reference
                                  'D'                     # descriptions
                                  )
        else:
          PositionLedger.transfer(session,
                                  self.broker_id,         # from_account_id
                                  self.broker_username,   # from_account_name
                                  self.broker_id,         # from_broker_id
                                  self.broker_username,   # from_broker_name
                                  self.account_id,        # to_account_id
                                  self.username,          # to_account_name
                                  self.broker_id,         # to_broker_id
                                  self.broker_username,   # to_broker_name
                                  self.currency,          # currency
                                  position,               # amount
                                  str(self.id),           # reference
                                  'D'                     # descriptions
                                  )

          Ledger.transfer(session,
                          self.broker_id,         # from_account_id
                          self.broker_username,   # from_account_name
                          self.broker_id,         # from_broker_id
                          self.broker_username,   # from_broker_name
                          self.account_id,        # to_account_id
                          self.username,          # to_account_name
                          self.broker_id,         # to_broker_id
                          self.broker_username,   # to_broker_name
                          self.currency,          # currency
                          val,                    # amount
                          str(self.id),           # reference
                          'D'                     # descriptions
                          )
      else:
        val = loan_amount if should_start_a_loan_from_broker_to_the_user else self.paid_value
        Ledger.transfer(session,
                        self.broker_id,         # from_account_id
                        self.broker_username,   # from_account_name
                        self.broker_id,         # from_broker_id
                        self.broker_username,   # from_broker_name
                        self.account_id,        # to_account_id
                        self.username,          # to_account_name
                        self.broker_id,         # to_broker_id
                        self.broker_username,   # to_broker_name
                        self.currency,          # currency
                        val,                    # amount
                        str(self.id),           # reference
                        'D'                     # descriptions
                        )

      total_percent_fee_value = (
          (val - self.fixed_fee) * (float(self.percent_fee)/100.0))
      total_fees = total_percent_fee_value + self.fixed_fee
      if total_fees:
        if not broker:
          broker = Broker.get_broker(session, self.broker_id)
        fee_account = json.loads(broker.accounts)['fees']

        Ledger.transfer(session,
                        self.account_id,        # from_account_id
                        self.username,          # from_account_name
                        self.broker_id,         # from_broker_id
                        self.broker_username,   # from_broker_name
                        fee_account[0],         # to_account_id
                        fee_account[1],         # to_account_name
                        self.broker_id,         # to_broker_id
                        self.broker_username,   # to_broker_name
                        self.currency,          # currency
                        total_fees,             # amount
                        str(self.id),           # reference
                        'DF'                    # descriptions
                        )
        for x in xrange(2, len(fee_account)):
          fwd_fee_account = fee_account[x]
          fwd_fee = total_fees * fwd_fee_account[2]
          Ledger.transfer(session,
                          fee_account[0],         # from_account_id
                          fee_account[1],         # from_account_name
                          self.broker_id,         # from_broker_id
                          self.broker_username,   # from_broker_name
                          fwd_fee_account[0],     # to_account_id
                          fwd_fee_account[1],     # to_account_name
                          self.broker_id,         # to_broker_id
                          self.broker_username,   # to_broker_name
                          self.currency,          # currency
                          fwd_fee,                # amount
                          str(self.id),           # reference
                          'DF'                    # descriptions
                          )

    instruction_to_execute = None
    if should_execute_instructions:
      instruction_to_execute = self.get_instructions()

    if should_update:
      self.data = json.dumps(new_data)
      session.add(self)
      session.flush()

    if self.status == '4':
      template_name = "deposit-complete"
      template_parameters = self.as_dict()

      if self.value:
        formatted_value = Currency.format_number(
            session, self.currency, self.value / 1.e8)
        template_parameters['value'] = formatted_value

      if self.paid_value:
        formatted_paid_value = Currency.format_number(
            session, self.currency, self.paid_value / 1.e8)
        template_parameters['paid_value'] = formatted_paid_value

      UserEmail.create(session=session,
                       user_id=self.user_id,
                       broker_id=self.broker_id,
                       subject="DF",
                       template=template_name,
                       language=self.email_lang,
                       params=json.dumps(template_parameters, cls=JsonEncoder))

    return instruction_to_execute

  def get_instructions(self):
    if self.instructions is None:
      return None

    try:
      now = get_datetime_now()
      instruction_age_in_seconds = (now - self.created).seconds

      instructions_list = json.loads(self.instructions)
      for instruction in instructions_list:
        #
        # Check if the instruction has timed out
        #
        has_timed_out = False
        if 'Timeout' in instruction:
          has_timed_out = instruction_age_in_seconds > instruction['Timeout']

        if has_timed_out:
          on_timeout_action = 'continue'
          if 'onTimeout' in instruction:
            on_timeout_action = instruction['onTimeout']

          if on_timeout_action == 'continue':
            continue
          if on_timeout_action == 'break':
            break

        if 'Filter' in instruction:
          filter = instruction['Filter']
          if 'Value' in filter and filter['Value'] != self.value:
            continue
          if 'PaidValue' in filter and filter['PaidValue'] != self.paid_value:
            continue

            # check if the instruction is a valid instruction
        msg = instruction['Msg']

        if msg['MsgType'] != 'D':
          continue  # invalid instruction

        # replace template variables
        for field, value in msg.iteritems():
          if value == '{$PaidValue}':
            msg[field] = self.paid_value
          if value == '{$Value}':
            msg[field] = self.value
          if value == '{$ClOrdID}':
            msg[field] = self.client_order_id

        return msg
    except Exception as e:
      pass
    return None


class DepositMethods(Base):
  __tablename__ = 'deposit_options'
  id = Column(Integer,    primary_key=True)
  broker_id = Column(Integer,    ForeignKey('users.id'), index=True)
  name = Column(String(15), nullable=False)
  description = Column(String(255), nullable=False)
  disclaimer = Column(String(255), nullable=False)
  type = Column(String(3),  nullable=False)
  broker_deposit_ctrl_num = Column(Integer,    nullable=False)
  currency = Column(String(3),  nullable=False)
  percent_fee = Column(Numeric,    nullable=False, default=0)
  fixed_fee = Column(Integer,    nullable=False, default=0)
  html_template = Column(UnicodeText)
  deposit_limits = Column(Text)
  parameters = Column(Text, nullable=False)
  user_receipt_url = Column(Text)

  def __repr__(self):
    return u"<DepositMethods(id=%r, broker_id=%r, name=%r, description=%r, disclaimer=%r ,"\
           u"type=%r, broker_deposit_ctrl_num=%r, currency=%r, percent_fee=%r, fixed_fee=%r, "\
           u"deposit_limits=%r, html_template=%r, parameters=%r,user_receipt_url=%r)>"\
        % (self.id, self.broker_id, self.name, self.description, self.disclaimer, self.type,
           self.broker_deposit_ctrl_num, self.currency, self.percent_fee, self.fixed_fee,
           self.deposit_limits, self.html_template, self.parameters, self.user_receipt_url)

  @staticmethod
  def get_deposit_method(session, deposit_option_id):
    return session.query(DepositMethods).filter_by(id=deposit_option_id).first()

  @staticmethod
  def get_list(session, broker_id):
    return session.query(DepositMethods).filter_by(broker_id=broker_id)

  def generate_deposit(self, session, user, value, client_order_id, instructions=None):
    self.broker_deposit_ctrl_num += 1
    import uuid
    deposit_id = uuid.uuid4().hex

    deposit = Deposit(
        id=deposit_id,
        user_id=user.id,
        account_id=user.id,
        username=user.username,
        broker_username=user.broker_username,
        broker_id=self.broker_id,
        deposit_option_id=self.id,
        deposit_option_name=self.name,
        type=self.type,
        currency=self.currency,
        broker_deposit_ctrl_num=self.broker_deposit_ctrl_num,
        fixed_fee=self.fixed_fee,
        percent_fee=self.percent_fee,
        value=value,
        email_lang=user.email_lang
    )
    if client_order_id:
      deposit.client_order_id = client_order_id

    if instructions:
      deposit.instructions = json.dumps(instructions)

    deposit.data = self.parameters

    session.add(self)
    session.add(deposit)
    session.flush()

    return deposit
