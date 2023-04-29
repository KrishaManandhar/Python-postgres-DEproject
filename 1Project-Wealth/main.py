import psycopg2
import pandas as pd

hostname = "localhost"
database = "Project1"
username = "postgres"
pwd = "admin987"
port_id = 5432
# cur = None
# conn = None
# try:
#     # Creation of connection to database
#     conn = psycopg2.connect(host = hostname,
#                             dbname= database,
#                             user = username,
#                             password = pwd,
#                             port = port_id)
#     # creation of cursor to database
#     cur = conn.cursor()
#
#
#
# # If try section fails, catch the error and prints it out
# except Exception as error:
#     print(error)
#
# # though the error occurs, code inside finally runs
# finally:
#     # Close the cursor
#     if cur is not None:
#         cur.close()
#     # Close the connection
#     if conn is not None:
#         conn.close()

AccountsCountry = pd.read_csv("data/Wealth-AccountsCountry.csv")
AccountsCountry_columns = AccountsCountry[['Code','Short Name','Table Name', 'Currency Unit']]
# print(AccountsCountry_columns.head(5))

AccountData = pd.read_csv("data/Wealth-AccountData.csv")
AccountData_columns = AccountData
# print(AccountData_columns.head(5))

AccountSeries = pd.read_csv("data/Wealth-AccountSeries.csv")
AccountSeries_columns = AccountSeries[['Code','Topic','Indicator Name','Long definition']]
# print(AccountSeries_columns.head(5))