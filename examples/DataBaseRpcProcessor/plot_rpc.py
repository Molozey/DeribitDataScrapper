from saveRPC import ORDERBOOK_FILE, TRADES_FILE
import matplotlib.pylab as plt
import pandas as pd

rpc_ORDERBOOK_FILE = "RPC_" + ORDERBOOK_FILE
rpc_TRADES_FILE = "RPC_" + TRADES_FILE

order_book_shape = pd.read_csv(ORDERBOOK_FILE, header=None)
order_book_shape.columns = ["TIME", "SHAPE"]
order_book_shape["TIME"] = pd.to_datetime(order_book_shape["TIME"])
# order_book_shape.set_index("TIME")
plt.figure(figsize=(12, 6))
plt.title("ORDER_BOOK_SHAPE")
plt.plot(order_book_shape["TIME"], order_book_shape["SHAPE"])
plt.show()

trades_shape = pd.read_csv(TRADES_FILE, header=None)
trades_shape.columns = ["TIME", "SHAPE"]
trades_shape["TIME"] = pd.to_datetime(trades_shape["TIME"])
# order_book_shape.set_index("TIME")
plt.figure(figsize=(12, 6))
plt.title("trades_SHAPE")
plt.plot(trades_shape["TIME"], trades_shape["SHAPE"])
plt.show()


rpc_order_book_shape = pd.read_csv(rpc_ORDERBOOK_FILE, header=None)
rpc_order_book_shape.columns = ["TIME", "SHAPE", "UPD"]
rpc_order_book_shape["TIME"] = pd.to_datetime(rpc_order_book_shape["TIME"])
# order_book_shape.set_index("TIME")
plt.figure(figsize=(12, 6))
plt.title("RPC ORDER_BOOK")
plt.plot(rpc_order_book_shape["TIME"], rpc_order_book_shape["SHAPE"])
plt.show()


rpc_trades_shape = pd.read_csv(rpc_TRADES_FILE, header=None)
rpc_trades_shape.columns = ["TIME", "SHAPE", "UPD"]
rpc_trades_shape["TIME"] = pd.to_datetime(rpc_trades_shape["TIME"])
# order_book_shape.set_index("TIME")
plt.figure(figsize=(12, 6))
plt.title("RPC TRADES")
plt.plot(rpc_trades_shape["TIME"], rpc_trades_shape["SHAPE"])
plt.show()