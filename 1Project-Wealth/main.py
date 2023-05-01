import psycopg2
import pandas as pd

# hostname = "localhost"
# database = "postgres"
# username = "postgres"
# pwd = "admin987"
# port_id = 5432

def create_database():
    try :
        #Connect to default databasse i.e "postgres"
        conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=admin987 port=5432")
        conn.set_session(autocommit=True)
        cur = conn.cursor()

        # Drop database named "wealth" if exists
        cur.execute("DROP DATABASE IF EXISTS wealth")
        # create database named "wealth"
        cur.execute("CREATE DATABASE wealth")

        # Close the connection of the default database
        cur.close()
        conn.close()

        # Create connection to database named "wealth"
        conn = psycopg2.connect("host=localhost dbname=wealth user=postgres password=admin987 port=5432")
        cur = conn.cursor()

    # if connection not established then show error
    except Exception as error:
        print(error)


    return conn, cur


# Data
AccountsCountry = pd.read_csv("data/Wealth-AccountsCountry.csv")
AccountsCountry_columns = AccountsCountry[['Code','Short Name','Table Name', 'Currency Unit']]
# print(AccountsCountry_columns.columns)

AccountsData = pd.read_csv("data/Wealth-AccountData.csv")
AccountsData_columns = AccountsData
AccountsData_columns.rename(columns={'Country Code':'Code'},inplace=True)
# print(AccountsData_columns.columns)

AccountsSeries = pd.read_csv("data/Wealth-AccountSeries.csv")
AccountsSeries_columns = AccountsSeries[['Code','Topic','Indicator Name','Long definition']]
AccountsSeries_columns.rename(columns={'Indicator Name':'Series Name'},inplace=True)
#print(AccountsSeries_columns.columns)



# Calling the create_database
conn, cur = create_database()

# Creating tables
accounts_country_table = ("""CREATE TABLE IF NOT EXISTS accountscountry(
Code VARCHAR,
short_name VARCHAR,
table_name VARCHAR,
currency_unit VARCHAR
)""")
cur.execute(accounts_country_table)
conn.commit()

accounts_data_table = ("""CREATE TABLE IF NOT EXISTS accountsdata(
country_name VARCHAR,
Code VARCHAR,
series_name VARCHAR,
series_code VARCHAR,
year_1996 NUMERIC, year_1997 NUMERIC, year_1998 NUMERIC, year_1999 NUMERIC,
year_2000 NUMERIC, year_2001 NUMERIC, year_2002 NUMERIC, year_2003 NUMERIC, year_2004 NUMERIC, year_2005 NUMERIC, year_2006 NUMERIC, year_2007 NUMERIC, year_2008 NUMERIC,
year_2009 NUMERIC, year_2010 NUMERIC, year_2012 NUMERIC, year_2013 NUMERIC, year_2014 NUMERIC, year_2015 NUMERIC, year_2016 NUMERIC, year_2017 NUMERIC, year_2018 NUMERIC
)""")
cur.execute(accounts_data_table)
conn.commit()

accounts_series_table = ("""CREATE TABLE IF NOT EXISTS accountseries(
series_code VARCHAR,
topic VARCHAR,
series_name VARCHAR,
long_definition VARCHAR
)""")
cur.execute(accounts_series_table)
conn.commit()

# Insert Values from data to tables

accounts_country_insert_query = "INSERT INTO accountscountry (Code, short_name, table_name, currency_unit) VALUES (%s,%s,%s,%s)"
for i, row in AccountsCountry_columns.iterrows():
    cur.execute(accounts_country_insert_query, list(row))
conn.commit()

accounts_data_insert_query = """INSERT INTO accountsdata (country_name,
Code,
series_name,
series_code,
year_1996, year_1997, year_1998, year_1999,
year_2000, year_2001, year_2002, year_2003, year_2004, year_2005, year_2006, year_2007, year_2008,
year_2009, year_2010, year_2012, year_2013, year_2014, year_2015, year_2016, year_2017, year_2018)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
# for i, row in AccountsData_columns.iterrows():
#     cur.execute(accounts_data_insert_query, list(row))
# conn.commit()

accounts_series_insert_query ="INSERT INTO accountseries(series_code, topic, series_name, long_definition) VALUES (%s,%s,%s,%s)"
# for i, row in AccountsSeries_columns.iterrows():
#     cur.execute(accounts_series_insert_query, list(row))
# conn.commit()