import datetime
from pyblinktrade.message import JsonMessage
from pyblinktrade.json_encoder import JsonEncoder
from copy import deepcopy
import math
import json

from .models import User, Order, UserPasswordReset, Deposit, DepositMethods, \
    NeedSecondFactorException, UserAlreadyExistsException, BrokerDoesNotExistsException, \
    Withdraw, Broker, Instrument, Currency, Balance, Ledger, Position, ApiAccess

from .execution import OrderMatcher

from .decorators import *

from .trade_application import TradeApplication


def processNewOrderSingle(session, msg):
  from errors import NotAuthorizedError, InvalidClientIDError

  if msg.has('ClientID') and not session.user.is_broker:
    if isinstance(msg.get('ClientID'), int):
      if msg.get('ClientID') != session.user.id and str(msg.get('ClientID')) != session.user.username:
        raise NotAuthorizedError()
    else:
      if (msg.get('ClientID').isdigit() and int(msg.get('ClientID')) != session.user.id) and \
         msg.get('ClientID') != session.user.username and \
         msg.get('ClientID') != session.user.email:
        raise NotAuthorizedError()

  if session.user.is_broker:
    if not msg.has('ClientID'):  # it is broker sending an order on behalf of it's client
      raise NotAuthorizedError()

    client = None
    if msg.get('ClientID').isdigit():
      client = User.get_user(TradeApplication.instance(
      ).db_session, session.user.id, user_id=int(msg.get('ClientID')))

    if not client:
      client = User.get_user(TradeApplication.instance(
      ).db_session, session.user.id, username=msg.get('ClientID'))

    if not client:
      client = User.get_user(TradeApplication.instance(
      ).db_session, session.user.id, email=msg.get('ClientID'))

    if not client:
      raise InvalidClientIDError()

    account_user = client
    account_id = client.account_id
    broker_user = session.profile
    fee_account = session.user_accounts['fees']
  else:
    account_id = session.user.account_id
    account_user = session.user
    broker_user = session.broker
    fee_account = session.broker_accounts['fees']

  if not broker_user:
    raise NotAuthorizedError()

  broker_fee = 0
  fee = 0
  if msg.get('Side') in ('1', '3'):  # Buy or Buy Minus ( To be implemented )
    broker_fee = broker_user.transaction_fee_buy
    if account_user.transaction_fee_buy is None:
      fee = broker_user.transaction_fee_buy
    else:
      fee = account_user.transaction_fee_buy
  else:
    broker_fee = broker_user.transaction_fee_sell
    if account_user.transaction_fee_sell is None:
      fee = broker_user.transaction_fee_sell
    else:
      fee = account_user.transaction_fee_sell

  # Adjust the price according to the PIP
  price_currency = msg.get('Symbol')[3:]
  pip = Currency.get_currency(
      TradeApplication.instance().db_session, price_currency).pip
  price = msg.get('Price', 0)
  price = int(math.floor(float(price) / float(pip)) * pip)

  instrument = Instrument.get_instrument(
      TradeApplication.instance().db_session, msg.get('Symbol'))
  instrument_brokers = json.loads(instrument.brokers)
  if account_user.broker_id not in instrument_brokers:
    raise NotAuthorizedError()

  # process the new order.
  order = Order.create(TradeApplication.instance().db_session,
                       user_id=session.user.id,
                       account_id=msg.get('ClientID', account_id),
                       user=session.user,
                       username=session.user.username,
                       account_user=account_user,
                       account_username=account_user.username,
                       broker_id=account_user.broker_id,
                       broker_username=account_user.broker_username,
                       client_order_id=msg.get('ClOrdID'),
                       symbol=msg.get('Symbol'),
                       side=msg.get('Side'),
                       type=msg.get('OrdType'),
                       price=price,
                       order_qty=msg.get('OrderQty'),
                       time_in_force=msg.get('TimeInForce', '1'),
                       fee=fee,
                       fee_account_id=fee_account[0],
                       fee_account_username=fee_account[1],
                       fwd_fees=json.dumps(fee_account[2:]),
                       email_lang=session.email_lang,
                       is_from_market_maker=account_user.is_market_maker,
                       gui_id=None)
  # just to assign an ID for the order.
  TradeApplication.instance().db_session.flush()

  OrderMatcher.get(msg.get('Symbol')).match(TradeApplication.instance().db_session,
                                            order,
                                            TradeApplication.instance().order_matcher_disabled,
                                            broker_fee)
  TradeApplication.instance().db_session.commit()

  return ""


def processCancelOrderRequest(session, msg):
  order_list = []
  if msg.has('OrigClOrdID') or msg.has('ClOrdID'):
    order = Order.get_order_by_client_order_id(TradeApplication.instance(
    ).db_session, session.user.id,  msg.get('OrigClOrdID', msg.get('ClOrdID')))
    if order:
      order_list.append(order)
  elif msg.has('OrderID'):
    order = Order.get_order_by_order_id(
        TradeApplication.instance().db_session,   msg.get('OrderID'))

    if order:
      if order.user_id == session.user.id:  # user/broker cancelling his own order
        order_list.append(order)
      elif order.account_id == session.user.id:  # user cancelling an order sent by his broker
        order_list.append(order)
      elif order.account_user.broker_id == session.user.id:  # broker cancelling an order sent by an user
        order_list.append(order)
  else:
    # user cancelling all the orders he sent.
    orders = Order.get_list(TradeApplication.instance().db_session,
                            ["user_id eq " + str(session.user.id), "has_leaves_qty eq 1"])
    for order in orders:
      order_list.append(order)

  for order in order_list:
    OrderMatcher.get(order.symbol).cancel(
        TradeApplication.instance().db_session, order)
  TradeApplication.instance().db_session.commit()

  return ""


def convertCamelCase2Underscore(name):
  import re
  s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
  return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def processTradersRankRequest(session, msg):
  page = msg.get('Page', 0)
  page_size = msg.get('PageSize', 100)
  filter = msg.get('Filter', [])
  offset = page * page_size

  columns = ['Rank', 'Trader',  'Broker', 'Amount']

  traders_list = Balance.get_balances_by_rank(
      TradeApplication.instance().db_session)

  response_msg = {
      'MsgType': 'U37',
      'DepositListReqID': msg.get('TradersRankReqID'),
      'Page': page,
      'PageSize': page_size,
      'Columns': columns,
      'TradersRankGrp': traders_list
  }

  return json.dumps(response_msg, cls=JsonEncoder)


def processRequestForOpenOrders(session, msg):
  page = msg.get('Page', 0)
  page_size = msg.get('PageSize', 100)
  filter_list = msg.get('Filter', [])
  offset = page * page_size

  if session.user.is_broker:
    filter_list.append("user_id eq " + str(session.user.id))
  else:
    filter_list.append("account_id eq " + str(session.user.id))

  orders = Order.get_list(TradeApplication.instance(
  ).db_session, filter_list, page_size, offset)

  order_list = []
  columns = ['ClOrdID', 'OrderID', 'CumQty', 'OrdStatus', 'LeavesQty', 'CxlQty', 'AvgPx',
             'Symbol', 'Side', 'OrdType', 'OrderQty', 'Price', 'OrderDate', 'Volume', 'TimeInForce']

  for order in orders:
    order_total_value = order.average_price * order.cum_qty
    if order_total_value:
      order_total_value /= 1.e8

    order_list.append([
        order.client_order_id,
        order.id,
        order.cum_qty,
        order.status,
        order.leaves_qty,
        order.cxl_qty,
        order.average_price,
        order.symbol,
        order.side,
        order.type,
        order.order_qty,
        order.price,
        order.created,
        order_total_value,
        order.time_in_force
    ])

  open_orders_response_msg = {
      'MsgType':     'U5',
      'OrdersReqID': msg.get('OrdersReqID'),
      'Page':        page,
      'PageSize':    page_size,
      'Columns':     columns,
      'OrdListGrp': order_list
  }
  return json.dumps(open_orders_response_msg, cls=JsonEncoder)
