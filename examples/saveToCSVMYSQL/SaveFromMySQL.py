import pandas as pd
import mysql.connector

host = "localhost"
user = "root"
password = "password"
database = "DeribitOrderBook"
# Connect to MySQL database
mydb = mysql.connector.connect(
  host=host,
  user=user,
  password=password,
  database=database
)

# Query MySQL table
mycursor = mydb.cursor()
mycursor.execute("SELECT * FROM Trades_table_test")
results = mycursor.fetchall()

# Convert results to dataframe
df = pd.DataFrame(results, columns=[i[0] for i in mycursor.description])

# Save dataframe to CSV file
df.to_csv("trades.csv", index=False)
