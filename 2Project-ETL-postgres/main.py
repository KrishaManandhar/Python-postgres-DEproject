import glob                           # this module helps in selecting files
import pandas as pd                   # this module helps in processing csv files
import xml.etree.ElementTree as ET    # this module helps in processing xml files
from datetime import datetime
import psycopg2                       # postgres adapter

# ----------CSV Extract Function------------
def extracting_from_csv(file_to_process):
  dataframe = pd.read_csv(file_to_process)
  return dataframe

# ---------JSON Extract Function------------
def extracting_from_json(file_to_process):
  dataframe = pd.read_json(file_to_process,lines=True) # lines: True helps when there is \n in json otherwise it will give trailing error
  return dataframe

# --------Xml Extract Function--------------
def extracting_from_xml(file_to_process):
  dataframe = pd.DataFrame(columns=["name","height","weight"])
  tree = ET.parse(file_to_process)
  root = tree.getroot()
  for person in root:   # dictionary making
    name = person.find("name").text
    height = float(person.find("height").text)
    weight = float(person.find("weight").text)
    dataframe = dataframe._append({"name":name,"height":height,"weight":weight}, ignore_index = True)  # Here ignore_index=True is necessary to append  dictionary to dataframe
  return dataframe

# ------------ Extract function ----------------------------
def extract():
    extracted_data = pd.DataFrame(columns=['name', 'height', 'weight'])

    for csvfile in glob.glob("data/*.csv"):  # selecting all csv files
        extracted_data = extracted_data._append(extracting_from_csv(csvfile),ignore_index=True)  # adding every csv file in extracted_data

    for jsonfile in glob.glob("data/*.json"):
        extracted_data = extracted_data._append(extracting_from_json(jsonfile), ignore_index=True)

    for xmlfile in glob.glob("data/*.xml"):
        extracted_data = extracted_data._append(extracting_from_xml(xmlfile), ignore_index=True)

    return extracted_data

# ------------- Transform ---------------------
def transform(data):
  #Convert datatype of column to float
  data.height = data.height.astype(float)
  #Convert height in inches to millimeter
  data['height'] = round(data.height*0.0254,2)

  #Convert datatype of column to float
  data.weight = data.weight.astype(float)
  #Convert weight in pounds to kilograms
  data['weight'] = round(data.weight*0.0254,2)

  return data

# ---------------------- Database Creation -------------------------------
def create_database():
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=abc port=5432")
        conn.set_session(autocommit=True)
        cur = conn.cursor()

        cur.execute("DROP DATABASE IF EXISTS source")
        cur.execute("CREATE DATABASE source")

        cur.close()
        conn.close()

        conn = psycopg2.connect("host=localhost dbname=source user=postgres password=abc port=5432")
        cur = conn.cursor()

    except Exception as error:
        print(error)

    return conn, cur

# ------------------- Create Table person --------------------------------
def person_table(conn,cur):
    person_table = ("""CREATE TABLE IF NOT EXISTS person(
    name VARCHAR,
    height VARCHAR,
    weight VARCHAR
    )""")
    cur.execute(person_table)
    conn.commit()


# ------------------- Create Table person --------------------------------
def log_table(conn, cur):
  time_table = ("""CREATE TABLE IF NOT EXISTS log(
  timestamp VARCHAR,
  message VARCHAR
  )""")
  cur.execute(time_table)
  conn.commit()
#
# # ------------------ Insert dataframe rows ----------------------------

def person_table_insert(data, conn, cur):
    insert_query = 'INSERT INTO person (name, height, weight) VALUES(%s,%s,%s)'
    for index, row in data.iterrows():
        #print(list(row))
        cur.execute(insert_query, list(row))
        conn.commit()


# # -------------------- insert log message in log table ------------
def log_table_insert(conn,cur,message):
    timestamp_format = '%Y-%h-%d-%H-:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)

    log_insert_query = "INSERT INTO log (timestamp, message) VALUES(%s,%s)"
    cur.execute(log_insert_query,(timestamp,message))
    conn.commit()

# Integrated all functions above
def main():
    con, cursor = create_database()
    person_table(con,cursor)
    log_table(con,cursor)
    log_message = "Database, Person Table and Log Table Created"
    log_table_insert(con,cursor,log_message)

    extracted_data = extract()
    log_message = "Data Extracted"
    log_table_insert(con,cursor,log_message)

    transformed_data = transform(extracted_data)
    log_message = "Data Transformed"
    log_table_insert(con,cursor,log_message)

    person_table_insert(transformed_data, con, cursor)
    log_message = "Inserted Transformed Data To Person Table"
    log_table_insert(con,cursor,log_message)

    log_message = "Connection Closed"
    log_table_insert(con,cursor,log_message)
    cursor.close()
    con.close()

# Function call :
main()
