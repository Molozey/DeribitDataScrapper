
from Scrapper import *
from oldDeribitAPI import DeribitConnectionOld
import datetime


oldConnection = DeribitConnectionOld("")
result = oldConnection.get_instrument_last_prices("BTC-25SEP20", number_of_last_trades=500,
                                                  number_of_requests=10,
                                                  date_of_start_loading_data=int(datetime.datetime(year=2021, month=1, day=1, hour=1, minute=1, second=1).timestamp() * 1000))

pprint(result)