import psycopg2

hostname = "localhost"
database = "DEproject1"
username = "postgres"
pwd = "*****"
port_id = 5432
conn = None
cur = None
try :
    #Creation of connection to database
    conn = psycopg2.connect(host= hostname, dbname = database, user = username, password= pwd, port = port_id)
    #creation of cursor to database
    cur = conn.cursor()

    #drop the table first and create table everytime. this solves the tuple to be repeated problem
    cur.execute('DROP TABLE IF EXISTS customer')

    #Creation of the table
    customerTable = '''CREATE TABLE IF NOT EXISTS customer(
        id  int PRIMARY KEY,
        name varchar(35) NOT NULL,
        city varchar(20),
        cus_order varchar(20)
    )'''
    #execute the create table query
    cur.execute(customerTable)

    #-------INSERT-----------

    #insert the values to the table
    insert_query = 'INSERT INTO customer (id, name, city, cus_order) VALUES(%s,%s,%s,%s) '
    insert_values = [(1,'Johny','California','Copy'),(2,'Sony','Washington','Pen'),(3,'Hony','Seatle','Pencil')]

    # to insert single tuple values
    # cur.execute(insertValues,insert_values)

    #to insert multiple tuples values
    for record in insert_values:
        cur.execute(insert_query,record)

    #---------Update-------
    update_query = "UPDATE customer SET city='Michigan' where id=2"
    cur.execute(update_query)

    #---------Delete--------
    delete_query = "DELETE FROM customer where name = %s"
    delete_value = ('Sony',)
    cur.execute(delete_query, delete_value)

    #All the transaction we so in database need to be committed in order save changes in database
    #commit save any transaction to database
    conn.commit()


# if connection not established then show error
except Exception as error:
    print(error)

# though error occurs, code inside finally runs
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()