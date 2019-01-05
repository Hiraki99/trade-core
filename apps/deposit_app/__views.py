# -*- coding: utf-8 -*-

import datetime
from copy import deepcopy
import math
import json
from . import Micro

# from .models import  User, Order, UserPasswordReset, Deposit, DepositMethods, \
#   NeedSecondFactorException, UserAlreadyExistsException, BrokerDoesNotExistsException, \
#   Withdraw, Broker, Instrument, Currency, Balance, Ledger, Position, ApiAccess


def (parameter_list):
  pass


def getProfileMessage(user, profile=None, show_account_info=True):
  if not profile:
    if user.is_broker:
      profile = Broker.get_broker( TradeApplication.instance().db_session,user.id)
    else:
      profile = user

  if user.is_broker:
    profile_message = {
      'Type'               : 'BROKER',
      'Username'           : user.username if show_account_info else 'hidden',
      'Verified'           : user.verified                ,
      'VerificationData'   : user.verification_data if show_account_info else None,
      'TwoFactorEnabled'   : user.two_factor_enabled      ,
      'NeedWithdrawEmail'  : user.withdraw_email_validation,
      'BrokerID'           : profile.id                   ,
      'ShortName'          : profile.short_name           ,
      'BusinessName'       : profile.business_name        ,
      'Address'            : profile.address              ,
      'ZipCode'            : profile.zip_code             ,
      'City'               : profile.city                 ,
      'State'              : profile.state                ,
      'Country'            : profile.country              ,
      'PhoneNumber1'       : profile.phone_number_1       ,
      'PhoneNumber2'       : profile.phone_number_2       ,
      'Skype'              : profile.skype                ,
      'Email'              : profile.email                ,
      'Currencies'         : profile.currencies           ,
      'VerificationForm'   : profile.verification_jotform ,
      'UploadForm'         : profile.upload_jotform       ,
      'TosUrl'             : profile.tos_url              ,
      'FeeStructure'       : json.loads(profile.fee_structure),
      'WithdrawStructure'  : json.loads(profile.withdraw_structure),
      'TransactionFeeBuy'  : profile.transaction_fee_buy  ,
      'TransactionFeeSell' : profile.transaction_fee_sell ,
      'Status'             : profile.status               ,
      'Ranking'            : profile.ranking              ,
      'SupportURL'         : profile.support_url          ,
      'CryptoCurrencies'   : json.loads(profile.crypto_currencies),
      'Accounts'           : json.loads(profile.accounts)
    }
  else:
    profile_message = {
      'Type'               : 'USER',
      'UserID'             : user.id,
      'ID'                 : user.id,
      'Username'           : user.username if show_account_info else 'hidden',
      'Email'              : profile.email if show_account_info else 'hidden',
      'State'              : profile.state,
      'Country'            : profile.country_code,
      'CountryCode'        : profile.country_code,
      'Verified'           : profile.verified,
      'VerificationData'   : profile.verification_data if show_account_info else None,
      'TwoFactorEnabled'   : profile.two_factor_enabled,
      'NeedWithdrawEmail'  : profile.withdraw_email_validation,
      'TransactionFeeBuy'  : profile.transaction_fee_buy,
      'TransactionFeeSell' : profile.transaction_fee_sell,
      'DepositPercentFee'  : profile.deposit_percent_fee,
      'DepositFixedFee'    : profile.deposit_fixed_fee,
      'WithdrawPercentFee' : profile.withdraw_percent_fee,
      'WithdrawFixedFee'   : profile.withdraw_fixed_fee,
      'IsMarketMaker'      : profile.is_market_maker if show_account_info else False
      }
  return profile_message

def processRequestForBalances(session, msg):
  user = session.user
  if msg.has('ClientID'):
    user = User.get_user(TradeApplication.instance().db_session,
                         session.user.id,
                         user_id= int(msg.get('ClientID')) )

    if not user:
      raise NotAuthorizedError()

    if user.broker_id  != session.user.id:
      raise NotAuthorizedError()


  balances = Balance.get_balances_by_account( TradeApplication.instance().db_session, user.account_id )
  response = {
    'MsgType': 'U3',
    'ClientID': user.id,
    'BalanceReqID': msg.get('BalanceReqID')
  }
  for balance in balances:
    if balance.broker_id in response:
      response[balance.broker_id][balance.currency ] = balance.balance
    else:
      response[balance.broker_id] = { balance.currency: balance.balance }
  return json.dumps(response, cls=JsonEncoder)


def processRequestDepositMethod(session, msg):
  deposit_method_id = msg.get('DepositMethodID')

  deposit_method = DepositMethods.get_deposit_method(TradeApplication.instance().db_session, deposit_method_id)
  if not deposit_method:
    response = {'MsgType':'U49', 'DepositMethodReqID': msg.get('DepositMethodReqID'), 'DepositMethodID':-1}

  else:
    response = {
      'MsgType':'U49',
      'DepositMethodReqID': msg.get('DepositMethodReqID'),
      'DepositMethodID':    deposit_method.id,
      'Description':        deposit_method.description,
      'Disclaimer':         deposit_method.disclaimer,
      'Type':               deposit_method.type,
      'DepositLimits':      '{}',
      'HtmlTemplate':       '',
      'Currency':           deposit_method.currency,
      'PercentFee':         float(deposit_method.percent_fee),
      'FixedFee':           deposit_method.fixed_fee,
      'Parameters':         json.loads(deposit_method.parameters)
    }
    if deposit_method.deposit_limits:
      response['DepositLimits'] = json.loads(deposit_method.deposit_limits)
    if deposit_method.html_template:
      response['HtmlTemplate'] = deposit_method.html_template

  return json.dumps(response, cls=JsonEncoder)


def processRequestDepositMethods(session, msg):
  broker_id = msg.get('BrokerID')
  if session.user is None and broker_id is None:
    raise InvalidParameter()
  elif broker_id is None:
    broker_id = session.user.broker_id

  deposit_options = DepositMethods.get_list(TradeApplication.instance().db_session, broker_id )

  deposit_options_group = []

  for deposit_option in deposit_options:
    deposit_options_group.append( {
      'DepositMethodID': deposit_option.id,
      'Description': deposit_option.description,
      'Disclaimer': deposit_option.disclaimer,
      'Type': deposit_option.type,
      'DepositLimits':  json.loads(deposit_option.deposit_limits) ,
      'Currency': deposit_option.currency,
      'PercentFee': float(deposit_option.percent_fee),
      'FixedFee': deposit_option.fixed_fee,
      'UserReceiptURL': deposit_option.user_receipt_url
    } )

  response = {
    'MsgType':'U21',
    'DepositMethodReqID': msg.get('DepositMethodReqID'),
    'DepositMethodGrp': deposit_options_group
  }

  return json.dumps(response, cls=JsonEncoder)

def processRequestDeposit(session, msg):
  deposit_option_id = msg.get('DepositMethodID')
  deposit_id        = msg.get('DepositID')
  currency          = msg.get('Currency')
  input_address     = msg.get('InputAddress')
  destination       = msg.get('Destination')
  secret            = msg.get('Secret')
  client_order_id   = msg.get('ClOrdID')
  instructions      = msg.get('Instructions')
  value             = msg.get('Value')

  should_broadcast = False
  if deposit_option_id:
    if session.user is None :
      raise NotAuthorizedError()

    deposit_option = DepositMethods.get_deposit_method(TradeApplication.instance().db_session, deposit_option_id)
    if not deposit_option:
      response = {'MsgType':'U19', 'DepositID': -1 }
      return json.dumps(response, cls=JsonEncoder)

    verification_level = session.user.verified

    deposit_method_deposit_limits = None
    if deposit_option.deposit_limits:
      deposit_method_deposit_limits = json.loads(deposit_option.deposit_limits)

    if not deposit_method_deposit_limits:
      raise NotAuthorizedError()

    while verification_level > 0:
      if str(verification_level) in deposit_method_deposit_limits:
        break
      verification_level -= 1

    if not deposit_method_deposit_limits[str(verification_level)]["enabled"]:
      raise  NotAuthorizedError()

    min_deposit_value = deposit_method_deposit_limits[str(verification_level)]['min'] if 'min' in deposit_method_deposit_limits[str(verification_level)] else None
    max_deposit_value = deposit_method_deposit_limits[str(verification_level)]['max'] if 'max' in deposit_method_deposit_limits[str(verification_level)] else None

    if min_deposit_value and value < min_deposit_value :
      raise NotAuthorizedError()

    if max_deposit_value and value > max_deposit_value:
      raise NotAuthorizedError()

    deposit = deposit_option.generate_deposit(  TradeApplication.instance().db_session,
                                                session.user,
                                                value,
                                                client_order_id,
                                                instructions )
    TradeApplication.instance().db_session.commit()
    should_broadcast = True
  elif currency:
    deposit = Deposit.create_crypto_currency_deposit(TradeApplication.instance().db_session,
                                                     session.user,
                                                     currency,
                                                     input_address,
                                                     destination,
                                                     secret,
                                                     client_order_id,
                                                     instructions,
                                                     value)
    TradeApplication.instance().db_session.commit()
    should_broadcast = True
  else:
    deposit = Deposit.get_deposit(TradeApplication.instance().db_session, deposit_id)

  if not deposit:
    response = {'MsgType':'U19', 'DepositID': -1 }
    return json.dumps(response, cls=JsonEncoder)

  if should_broadcast:
    deposit_refresh = depositRecordToDepositMessage(deposit)
    deposit_refresh['MsgType'] = 'U23'
    deposit_refresh['DepositReqID'] = msg.get('DepositReqID')
    TradeApplication.instance().publish( deposit.account_id, deposit_refresh  )
    TradeApplication.instance().publish( deposit.broker_id,  deposit_refresh  )


  response_msg = depositRecordToDepositMessage(deposit)
  response_msg['MsgType'] = 'U19'
  response_msg['DepositReqID'] = msg.get('DepositReqID')
  return json.dumps(response_msg, cls=JsonEncoder)

def depositRecordToDepositMessage( deposit, show_account_info = True ):
  deposit_message = dict()
  deposit_message['DepositID']           = deposit.id
  deposit_message['UserID']              = deposit.user_id
  deposit_message['AccountID']           = deposit.account_id
  deposit_message['BrokerID']            = deposit.broker_id
  deposit_message['Username']            = deposit.username if show_account_info else 'hidden'
  deposit_message['DepositMethodID']     = deposit.deposit_option_id
  deposit_message['DepositMethodName']   = deposit.deposit_option_name
  deposit_message['ControlNumber']       = deposit.broker_deposit_ctrl_num
  deposit_message['Type']                = deposit.type
  deposit_message['Currency']            = deposit.currency
  deposit_message['Value']               = deposit.value
  deposit_message['PaidValue']           = deposit.paid_value
  deposit_message['Data']                = json.loads(deposit.data)
  deposit_message['Created']             = deposit.created
  deposit_message['Status']              = deposit.status
  deposit_message['ReasonID']            = deposit.reason_id
  deposit_message['Reason']              = deposit.reason
  deposit_message['PercentFee']          = float(deposit.percent_fee)
  deposit_message['FixedFee']            = deposit.fixed_fee
  deposit_message['ClOrdID']             = deposit.client_order_id
  return deposit_message



def processWithdrawRequest(session, msg):
  reqId           = msg.get('WithdrawReqID')
  client_order_id = msg.get('ClOrdID')

  verification_level = session.user.verified

  percent_fee = 0.
  fixed_fee = 0

  withdraw_structure = json.loads(session.broker.withdraw_structure)
  limits = None
  for withdraw_method in withdraw_structure[msg.get('Currency')]:
    if msg.get('Method') == withdraw_method['method']:
      limits = withdraw_method['limits']
      withdraw_method_percent_fee = withdraw_method['percent_fee']
      if withdraw_method_percent_fee is not None:
        percent_fee = withdraw_method_percent_fee

      withdraw_method_fixed_fee = withdraw_method['fixed_fee']
      if withdraw_method_fixed_fee is not None:
        fixed_fee = withdraw_method_fixed_fee
      break

  if session.user.withdraw_percent_fee is not None:
    if percent_fee:
      percent_fee = min(session.user.withdraw_percent_fee, percent_fee)
    else:
      percent_fee = session.user.withdraw_percent_fee

  if session.user.withdraw_fixed_fee is not None:
    if fixed_fee:
      fixed_fee = min(session.user.withdraw_fixed_fee, fixed_fee)
    else:
      fixed_fee = session.user.withdraw_fixed_fee

  if not limits:
    raise NotAuthorizedError()

  while verification_level > 0:
    if str(verification_level) in limits:
      break
    verification_level -= 1

  if not limits[str(verification_level)]["enabled"]:
    raise  NotAuthorizedError()

  min_amount = limits[str(verification_level)]['min'] if 'min' in limits[str(verification_level)] else None
  max_amount = limits[str(verification_level)]['max'] if 'max' in limits[str(verification_level)] else None

  if min_amount and msg.get('Amount') < min_amount :
    raise NotAuthorizedError()

  if max_amount and msg.get('Amount') > max_amount:
    raise NotAuthorizedError()

  withdraw_record = Withdraw.create(TradeApplication.instance().db_session,
                                    session.user,
                                    session.broker,
                                    msg.get('Currency'),
                                    msg.get('Amount'),
                                    msg.get('Method'),
                                    msg.get('Data', {} ),
                                    client_order_id,
                                    session.email_lang,
                                    percent_fee,
                                    fixed_fee)

  TradeApplication.instance().db_session.commit()

  withdraw_refresh = withdrawRecordToWithdrawMessage(withdraw_record)
  withdraw_refresh['MsgType'] = 'U9'
  TradeApplication.instance().publish( withdraw_record.account_id, withdraw_refresh  )
  TradeApplication.instance().publish( withdraw_record.broker_id,  withdraw_refresh  )


  response = {
    'MsgType':            'U7',
    'WithdrawReqID':      reqId,
    'Status':             withdraw_record.status,
    'WithdrawID':         withdraw_record.id,
  }
  return json.dumps(response, cls=JsonEncoder)

def withdrawRecordToWithdrawMessage( withdraw ):
  withdraw_message = dict()
  withdraw_message['WithdrawID']          = withdraw.id
  withdraw_message['UserID']              = withdraw.user_id
  withdraw_message['BrokerID']            = withdraw.broker_id
  withdraw_message['Username']            = withdraw.username
  withdraw_message['Method']              = withdraw.method
  withdraw_message['Currency']            = withdraw.currency
  withdraw_message['Amount']              = withdraw.amount
  withdraw_message['Data']                = json.loads(withdraw.data)
  withdraw_message['Created']             = withdraw.created
  withdraw_message['Status']              = withdraw.status
  withdraw_message['ReasonID']            = withdraw.reason_id
  withdraw_message['Reason']              = withdraw.reason
  withdraw_message['PercentFee']          = float(withdraw.percent_fee)
  withdraw_message['FixedFee']            = withdraw.fixed_fee
  withdraw_message['PaidAmount']          = withdraw.paid_amount
  withdraw_message['ClOrdID']             = withdraw.client_order_id
  return withdraw_message


def processWithdrawConfirmationRequest(session, msg):
  reqId = msg.get('WithdrawReqID')
  token = msg.get('ConfirmationToken')


  withdraw_id = msg.get('WithdrawID')
  second_factor = msg.get('SecondFactor')

  if second_factor:
    withdraw_data = Withdraw.get_withdraw(TradeApplication.instance().db_session, withdraw_id)
    if not withdraw_data:
      raise InvalidParameter()

    if not session.user.check_second_factor(second_factor) or \
       not withdraw_data.confirm_using_second_factor(TradeApplication.instance().db_session):
      response = {'MsgType':'U25', 'WithdrawReqID': reqId, 'WithdrawID':withdraw_data.id, 'Status':withdraw_data.status}
      return json.dumps(response, cls=JsonEncoder)
  else:
    withdraw_data = Withdraw.user_confirm(TradeApplication.instance().db_session, token)
    if not withdraw_data:
      response = {'MsgType':'U25', 'WithdrawReqID': reqId, 'Status':'0'}
      return json.dumps(response, cls=JsonEncoder)

  TradeApplication.instance().db_session.commit()

  withdraw_refresh = withdrawRecordToWithdrawMessage(withdraw_data)
  withdraw_refresh['MsgType'] = 'U9'
  TradeApplication.instance().publish( withdraw_data.account_id, withdraw_refresh  )
  TradeApplication.instance().publish( withdraw_data.broker_id,  withdraw_refresh  )


  response_u25 = withdrawRecordToWithdrawMessage(withdraw_data)
  response_u25['MsgType'] = 'U25'
  response_u25['WithdrawReqID'] = reqId
  response_u25['WithdrawID'] = withdraw_data.id
  response_u25['Status'] = withdraw_data.status

  return json.dumps(response_u25, cls=JsonEncoder)


def processWithdrawListRequest(session, msg):
  page        = msg.get('Page', 0)
  page_size   = msg.get('PageSize', 100)
  status_list = msg.get('StatusList', ['1', '2'] )
  filter      = msg.get('Filter',[])
  offset      = page * page_size

  user = session.user
  if msg.has('ClientID') and int(msg.get('ClientID')) != session.user.id :
    user = User.get_user(TradeApplication.instance().db_session, session.user.id, user_id= int(msg.get('ClientID')) )
    if user.broker_id  != session.user.id:
      raise NotAuthorizedError()
    if not user:
      raise NotAuthorizedError()

  if user.is_broker:
    if msg.has('ClientID'):
      withdraws = Withdraw.get_list(TradeApplication.instance().db_session, user.id, int(msg.get('ClientID')), status_list, page_size, offset, filter  )
    else:
      withdraws = Withdraw.get_list(TradeApplication.instance().db_session, user.id, None, status_list, page_size, offset, filter  )
  else:
    withdraws = Withdraw.get_list(TradeApplication.instance().db_session, user.broker_id, user.id, status_list, page_size, offset, filter  )

  withdraw_list = []
  columns = [ 'WithdrawID'   , 'Method'   , 'Currency'     , 'Amount' , 'Data',
              'Created'      , 'Status'   , 'ReasonID'     , 'Reason' , 'PercentFee',
              'FixedFee'     , 'PaidAmount', 'UserID'      , 'Username', 'BrokerID' ,
              'ClOrdID']

  for withdraw in withdraws:
    withdraw_list.append( [
      withdraw.id,
      withdraw.method,
      withdraw.currency,
      withdraw.amount,
      json.loads(withdraw.data),
      withdraw.created,
      withdraw.status,
      withdraw.reason_id,
      withdraw.reason,
      float(withdraw.percent_fee),
      withdraw.fixed_fee,
      withdraw.paid_amount,
      withdraw.user_id,
      withdraw.username if session.has_access_to_account_info() else 'hidden',
      withdraw.broker_id,
      withdraw.client_order_id
    ])

  response_msg = {
    'MsgType'           : 'U27', # WithdrawListResponse
    'WithdrawListReqID' : msg.get('WithdrawListReqID'),
    'Page'              : page,
    'PageSize'          : page_size,
    'Columns'           : columns,
    'WithdrawListGrp'   : withdraw_list
  }
  return json.dumps(response_msg, cls=JsonEncoder)

def processBrokerListRequest(session, msg):
  page        = msg.get('Page', 0)
  page_size   = msg.get('PageSize', 100)
  status_list = msg.get('StatusList', ['1'] )
  country     = msg.get('Country', None)
  offset      = page * page_size

  brokers = Broker.get_list(TradeApplication.instance().db_session, status_list, country, page_size, offset)

  broker_list = []
  columns = [ 'BrokerID'        , 'ShortName'      , 'BusinessName'      , 'Address'            , 'City', 'State'     ,
              'ZipCode'         , 'Country'        , 'PhoneNumber1'      , 'PhoneNumber2'       , 'Skype'             ,
              'Currencies'      , 'TosUrl'         , 'FeeStructure'      , 'TransactionFeeBuy'  , 'TransactionFeeSell',
              'Status'          , 'ranking'        , 'Email'             , 'CountryCode'        , 'CryptoCurrencies'  ,
              'WithdrawStructure','SupportURL'     , 'SignupLabel'       , 'AcceptCustomersFrom', 'IsBrokerHub']

  if session.user and session.user.is_system:
    columns.extend(['MandrillApiKey', 'MailerFromName', 'MailerFromEmail', 'MailerSignature', 'MailchimpListID'])

  for broker in brokers:
    broker_data = [
      broker.id                   ,
      broker.short_name           ,
      broker.business_name        ,
      broker.address              ,
      broker.city                 ,
      broker.state                ,
      broker.zip_code             ,
      broker.country              ,
      broker.phone_number_1       ,
      broker.phone_number_2       ,
      broker.skype                ,
      broker.currencies           ,
      broker.tos_url              ,
      json.loads(broker.fee_structure),
      broker.transaction_fee_buy  ,
      broker.transaction_fee_sell ,
      broker.status               ,
      broker.ranking              ,
      broker.email                ,
      broker.country_code         ,
      json.loads(broker.crypto_currencies),
      json.loads(broker.withdraw_structure),
      broker.support_url          ,
      broker.signup_label         ,
      json.loads(broker.accept_customers_from),
      broker.is_broker_hub
    ]
    if session.user and session.user.is_system:
      broker_data.extend([ broker.mandrill_api_key, broker.mailer_from_name, broker.mailer_from_email,
                           broker.mailer_signature, broker.mailchimp_list_id ])

    broker_list.append(broker_data)

  response_msg = {
    'MsgType'           : 'U29',
    'BrokerListReqID'   : msg.get('BrokerListReqID'),
    'Page'              : page,
    'PageSize'          : page_size,
    'Columns'           : columns,
    'BrokerListGrp'     : broker_list
  }
  return json.dumps(response_msg, cls=JsonEncoder)


def processRequestDatabaseQuery(session, msg):
  page        = msg.get('Page', 0)
  page_size   = msg.get('PageSize', 100)
  columns     = msg.get('Columns', [])
  table       = msg.get('Table', '')
  sort_column = msg.get('Sort', '')
  sort_order  = msg.get('SortOrder', 'ASC')
  offset      = page * page_size

  # TODO: Check all parameters to avoid an sql injection :(

  # This is definitively not secure, but this code will only run with inside a system account.
  raw_sql = 'SELECT '
  raw_sql += ','.join(columns)
  raw_sql += ' FROM ' + table

  if sort_column:
    raw_sql += ' ORDER BY ' + sort_column + ' ' + sort_order

  raw_sql += ' LIMIT ' + str(page_size)
  raw_sql += ' OFFSET ' + str(offset)


  result_set = TradeApplication.instance().db_session.execute(raw_sql)
  result = {
    'MsgType' : 'A1',
    'Page': page,
    'PageSize': page_size,
    'Table': table,
    'Columns': columns,
    'ResultSet': [ [ l for l in res ] for res in  result_set ]
  }
  return json.dumps(result, cls=JsonEncoder)


def processProcessWithdraw(session, msg):
  withdraw = Withdraw.get_withdraw(TradeApplication.instance().db_session, msg.get('WithdrawID'))

  if withdraw.broker_id != session.user.id:
    raise  NotAuthorizedError()

  result = False
  if msg.get('Action') == 'CANCEL':
    if withdraw.status == '4' or withdraw == '8':
      raise NotAuthorizedError()  # trying to cancel a completed operation or a cancelled operation

    broker_fees_account = session.user_accounts['fees']
    result = withdraw.cancel( TradeApplication.instance().db_session, msg.get('ReasonID'), msg.get('Reason'),broker_fees_account )
  elif msg.get('Action') == 'PROGRESS':
    data        = msg.get('Data')
    percent_fee = msg.get('PercentFee',0.)
    fixed_fee   = msg.get('FixedFee',0.)

    if percent_fee > float(withdraw.percent_fee):
      raise NotAuthorizedError() # Broker tried to raise their fees manually

    if fixed_fee > float(withdraw.fixed_fee):
      raise NotAuthorizedError() # Broker tried to raise their fees manually

    broker_fees_account = session.user_accounts['fees']

    result = withdraw.set_in_progress( TradeApplication.instance().db_session, percent_fee, fixed_fee, data, broker_fees_account)
  elif msg.get('Action') == 'COMPLETE':
    data        = msg.get('Data')

    broker_fees_account = session.user_accounts['fees']

    result = withdraw.set_as_complete( TradeApplication.instance().db_session, data, broker_fees_account)

  TradeApplication.instance().db_session.commit()

  if result:
    withdraw_refresh = withdrawRecordToWithdrawMessage(withdraw)
    withdraw_refresh['MsgType'] = 'U9'
    TradeApplication.instance().publish( withdraw.account_id, withdraw_refresh  )
    TradeApplication.instance().publish( withdraw.broker_id,  withdraw_refresh  )

  response_msg = {
    'MsgType'             : 'B7',
    'ProcessWithdrawReqID': msg.get('ProcessWithdrawReqID'),
    'WithdrawID'          : msg.get('WithdrawID'),
    'Result'              : result,
    'Status'              : withdraw.status,
    'ReasonID'            : withdraw.reason_id,
    'Reason'              : withdraw.reason
  }
  return json.dumps(response_msg, cls=JsonEncoder)

def processProcessDeposit(session, msg):
  secret       = msg.get('Secret')
  data         = msg.get('Data')

  instruction_msg_after_deposit = None
  deposit = None

  if not secret:
    deposit_id   = msg.get('DepositID')
    deposit = Deposit.get_deposit(TradeApplication.instance().db_session, deposit_id=deposit_id)

    if session.user is None or session.user.is_broker == False:
      if msg.get('Action') != 'CONFIRM':
        raise NotAuthorizedError()

    else:
      if deposit.broker_id != session.user.id:
        raise NotAuthorizedError()
  else:
    amount          = int(msg.get('Amount'))
    deposit_list = Deposit.get_deposit_list_by_secret(TradeApplication.instance().db_session, secret)
    found_deposit_by_secret = False
    for deposit in deposit_list:

      if deposit.status == '0':
        found_deposit_by_secret = True
        break  # get the first deposit that hasn't been confirmed yet

      if deposit.match_deposit_data(TradeApplication.instance().db_session, amount, data):
        found_deposit_by_secret = True
        break

    if not found_deposit_by_secret and deposit is not None:
      # we found deposits using the same secret, but with different data.
      # this means that the user reused the deposit address. Let's create another
      # deposit record based on the last deposit we found and process it.

      # ONLY VERIFIED USERS CAN REUSE THE SAME ADDRESS.
      user = User.get_user(TradeApplication.instance().db_session,
                           broker_id=deposit.broker_id,
                           user_id=deposit.user_id)
      deposit_data = json.loads(deposit.data)

      instructions = None
      if deposit.instructions:
        instructions = json.loads(deposit.instructions)

      if user.verified >= 3:
        deposit = Deposit.create_crypto_currency_deposit(
          session = TradeApplication.instance().db_session,
          user = user,
          currency = deposit.currency,
          input_address = deposit_data['InputAddress'],
          destination = deposit_data['Destination'],
          secret = deposit.secret,
          client_order_id = deposit.client_order_id,
          instructions=instructions,
          value=amount
        )
        deposit_refresh = depositRecordToDepositMessage(deposit)
        deposit_refresh['MsgType'] = 'U23'
        deposit_refresh['DepositReqID'] = msg.get('ProcessDepositReqID')
        TradeApplication.instance().publish( deposit.account_id, deposit_refresh  )
        TradeApplication.instance().publish( deposit.broker_id,  deposit_refresh  )
      else:
        deposit = None



  if not deposit:
    return  json.dumps( { 'MsgType' : 'B1',
                          'ProcessDepositReqID':msg.get('ProcessDepositReqID') ,
                          'ReasonID':'-1'} , cls=JsonEncoder)

  if msg.get('Action') == 'CONFIRM':
    deposit.user_confirm(TradeApplication.instance().db_session, data )
  elif msg.get('Action') == 'CANCEL':
    deposit.cancel( TradeApplication.instance().db_session, msg.get('ReasonID'), msg.get('Reason') )
  elif msg.get('Action') == 'PROGRESS':
    deposit.set_in_progress(TradeApplication.instance().db_session, data)
  elif msg.get('Action') == 'COMPLETE':
    amount          = int(msg.get('Amount'))
    percent_fee     = msg.get('PercentFee', 0.)
    fixed_fee       = msg.get('FixedFee', 0)

    if percent_fee > deposit.percent_fee:
      raise NotAuthorizedError() # Broker tried to raise their  fees manually

    if fixed_fee > deposit.fixed_fee:
      raise NotAuthorizedError() # Broker tried to raise their  fees manually

    instruction_msg_after_deposit = deposit.process_confirmation(TradeApplication.instance().db_session,
                                                                 amount,
                                                                 percent_fee,
                                                                 fixed_fee,
                                                                 data)

  TradeApplication.instance().db_session.commit()

  if instruction_msg_after_deposit:
    msg = JsonMessage( json.dumps(instruction_msg_after_deposit) )

    if session.user:
      session.process_message(msg)
    else:
      user = User.get_user( TradeApplication.instance().db_session, deposit.broker_id, user_id=deposit.user_id)
      session.set_user(user, {'*':[]})
      session.process_message(msg)
      session.set_user(None, None)


  deposit_refresh = depositRecordToDepositMessage(deposit)
  deposit_refresh['MsgType'] = 'U23'
  deposit_refresh['DepositReqID'] = msg.get('DepositReqID')
  TradeApplication.instance().publish( deposit.account_id, deposit_refresh  )
  TradeApplication.instance().publish( deposit.broker_id,  deposit_refresh  )


  result = depositRecordToDepositMessage(deposit)
  result['MsgType'] =  'B1'
  result['ProcessDepositReqID'] = msg.get('ProcessDepositReqID')
  return json.dumps(result, cls=JsonEncoder)


def processLedgerListRequest(session, msg):
  page            = msg.get('Page', 0)
  page_size       = msg.get('PageSize', 100)
  currency        = msg.get('Currency')
  filter          = msg.get('Filter',[])
  offset          = page * page_size

  user = session.user

  broker_id       = user.broker_id
  account_id      = user.id

  if user.is_broker:
    if msg.has('ClientID'):
      account_id = int(msg.get('ClientID'))
    else:
      account_id = None

    if msg.has('BrokerID'):
      if int(msg.get('BrokerID')) != user.id and int(msg.get('BrokerID')) != user.broker_id:
        raise  NotAuthorizedError()
      broker_id = int(msg.get('BrokerID'))


  records = Ledger.get_list(TradeApplication.instance().db_session,
                            broker_id,
                            account_id,
                            page_size,
                            offset,
                            currency,
                            filter  )

  record_list = []
  columns = [ 'LedgerID',       'Currency',     'Operation',
              'AccountID',      'BrokerID',     'PayeeID',
              'PayeeBrokerID',  'Amount',       'Balance',
              'Reference',      'Created',      'Description',
              'AccountName']

  if user.is_broker:
    columns.append('PayeeName')

  for rec in records:
    data = [
      rec.id,
      rec.currency,
      rec.operation,
      rec.account_id,
      rec.broker_id,
      rec.payee_id,
      rec.payee_broker_id,
      rec.amount,
      rec.balance,
      rec.reference,
      rec.created,
      rec.description,
      rec.account_name if session.has_access_to_account_info() else 'hidden'
    ]
    if user.is_broker:
      data.append(rec.payee_name)

    record_list.append(data)

  response_msg = {
    'MsgType'           : 'U35', # LedgerListResponse
    'LedgerListReqID'   : msg.get('LedgerListReqID'),
    'Page'              : page,
    'PageSize'          : page_size,
    'Columns'           : columns,
    'LedgerListGrp'     : record_list
  }
  return json.dumps(response_msg, cls=JsonEncoder)


def processDepositListRequest(session, msg):
  page        = msg.get('Page', 0)
  page_size   = msg.get('PageSize', 100)
  status_list = msg.get('StatusList', ['0', '1', '2', '4', '8'] )
  filter      = msg.get('Filter',[])


  offset      = page * page_size

  user = session.user

  if user.is_broker:
    if msg.has('ClientID'):
      deposits = Deposit.get_list(TradeApplication.instance().db_session, user.id, int(msg.get('ClientID')), status_list, page_size, offset, filter  )
    else:
      deposits = Deposit.get_list(TradeApplication.instance().db_session, user.id, None, status_list, page_size, offset, filter  )
  else:
    deposits = Deposit.get_list(TradeApplication.instance().db_session, user.broker_id, user.id, status_list, page_size, offset, filter  )


  deposit_list = []
  columns = [ 'DepositID'    , 'DepositMethodID', 'DepositMethodName' ,
              'Type'         , 'Currency'       , 'Value'             ,
              'PaidValue'    , 'Data'           , 'Created'           ,
              'ControlNumber', 'PercentFee'     , 'FixedFee'          ,
              'Status'       , 'ReasonID'       , 'Reason'            ,
              'Username'     , 'UserID'         , 'BrokerID'          ,
              'ClOrdID']

  for deposit in deposits:
    deposit_list.append( [
      deposit.id,
      deposit.deposit_option_id,
      deposit.deposit_option_name,
      deposit.type,
      deposit.currency,
      deposit.value,
      deposit.paid_value,
      json.loads(deposit.data),
      deposit.created,
      deposit.broker_deposit_ctrl_num,
      float(deposit.percent_fee),
      deposit.fixed_fee,
      deposit.status,
      deposit.reason_id,
      deposit.reason,
      deposit.username if session.has_access_to_account_info() else 'hidden',
      deposit.user_id,
      deposit.broker_id,
      deposit.client_order_id
    ])

  response_msg = {
    'MsgType'           : 'U31', # DepositListResponse
    'DepositListReqID'  : msg.get('DepositListReqID'),
    'Page'              : page,
    'PageSize'          : page_size,
    'Columns'           : columns,
    'DepositListGrp'    : deposit_list
  }
  return json.dumps(response_msg, cls=JsonEncoder)
