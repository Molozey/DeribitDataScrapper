import AvailableCurrencies


def order_request(order_side: str, instrument_name: str, amount: int,
                  order_type: str, order_tag: str = 'defaultTag', order_price=None):

    _msg = \
        {
            "jsonrpc": "2.0",
            "id": 5275,
            "method": f"private/{order_side}",
            "params": {
                "instrument_name": instrument_name,
                "amount": amount,
                "type": order_type,
                "label": order_tag
            }
        }
    if order_type == "limit":
        _msg["params"]["price"] = order_price

    return _msg


def auth_message(client_id: str, client_secret: str):
    _msg = \
        {
            "jsonrpc": "2.0",
            "id": 9929,
            "method": "public/auth",
            "params": {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret
            }
        }
    return _msg


def request_order_updates_to_currency(currency: AvailableCurrencies.Currency):
    _msg = \
        {"jsonrpc": "2.0",
         "method": "private/subscribe",
         "id": 42,
         "params": {
             "channels": [f"user.orders.future.{currency.currency}.100ms"]}
         }
    return _msg