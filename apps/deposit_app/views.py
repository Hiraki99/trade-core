from . import Micro

# open account


@Micro.typing('/account/open', methods='POST')
@Micro.json
def open_account(user_id=None, account_no=None, type='0001', description='', currency='VND', balance=0.0):
    """Use for open new account => will insert a row in Account table

    Arguments:
        user_id {[type]} -- [description]
        account_no {[type]} -- [description]

    Keyword Arguments:
        type {str} -- [description] (default: {'0001'})
        description {str} -- [description] (default: {''})
        currency {str} -- [description] (default: {'VND'})
        balance {float} -- [description] (default: {0.0})

    Example json accept:
        {
            "user_id":1,
            "account_no":"000129485",
            "type":"0001",
            "description":"This is open test account",
            "currency":"VND",
            "balance":0.0,
        }
    """

    pass


@Micro.typing('/account/process', methods='POST')
@Micro.json
def process_account_request(parameter_list):
    pass


@Micro.typing('/account/hide', methods='POST')
@Micro.json
def hide_account(parameter_list):
    pass


@Micro.typing('/account/info/view', methods='GET')
@Micro.json
def view_account_info(account=None):
    """Use for view all info of account

    Returns:
        [type] -- [description]
    """

    return ['This is your account']

# view account balance


@Micro.typing('/account/balance/view', methods='GET')
@Micro.json
def view_account_balance():
    """Use for view balance of all account

    Returns:
        [type] -- [description]
    """
    pass

# view account history


@Micro.typing('/account/history/view', methods='GET')
@Micro.json
def view_account_history(account=None):
    """Use for view all transaction history of an account. all deposit and withdraw

    Returns:
        [type] -- [description]
    """
    pass
# deposit money


@Micro.typing('/deposit/open', methods='POST')
@Micro.json
def open_deposit_request(user_id=None, account_id=None, account_no=None, kind='depo', approver_name='', deposit_method='bank_transfer', type='0001', description='', currency='VND', value=0.0, status=0):
    """Use for deposit to an account
    
    Keyword Arguments:
        user_id {[type]} -- [description] (default: {None})
        account_id {[type]} -- [description] (default: {None})
        account_no {[type]} -- [description] (default: {None})
        approver_name {str} -- [description] (default: {''})
        deposit_method {str} -- [description] (default: {'bank_transfer'})
        type {str} -- [description] (default: {'0001'})
        description {str} -- [description] (default: {''})
        currency {str} -- [description] (default: {'VND'})
        value {float} -- [description] (default: {0.0})
        status {int} -- [description] (default: {0})
    Example json:
        {
            "user_id": 132,
            "account_id": 100,
            "account_no": "091823829" # no more than 12 string,
            "kind":"depo",
            "approver_name": "",
            "deposit_method": "bank_transfer",
            "type": "0001",
            "description": "Test deposit",
            "currency": "VND",
            "value": 1000.5,
            "status": "0",
        }
    """
    pass


@Micro.typing('/deposit/cancel', methods='POST')
@Micro.json
def cancel_deposit_request(parameter_list):
    pass


@Micro.typing('/deposit/view', methods='GET')
@Micro.json
def view_deposit_request(account_no=None):
    """Use for view deposit request by account
    
    Keyword Arguments:
        deposit_id {[type]} -- [description] (default: {None})
    """
    pass


@Micro.typing('/deposit/process', methods='POST')
@Micro.json
def process_deposit_request(parameter_list):
    pass

# withdraw money


@Micro.typing('/withdraw/open', methods='POST')
@Micro.json
def open_withdraw_request(parameter_list):
    pass


@Micro.typing('/withdraw/cancel', methods='POST')
@Micro.json
def cancel_withdraw_request():
    pass


@Micro.typing('/withdraw/view', methods='GET')
@Micro.json
def view_withdraw_request(parameter_list):
    pass


@Micro.typing('/withdraw/process', methods='POST')
@Micro.json
def process_withdraw_request(parameter_list):
    pass
