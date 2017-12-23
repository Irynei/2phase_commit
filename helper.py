import psycopg2


def connect_to_db():
    account_conn = psycopg2.connect(dbname="account", user="postgres", password="postgres")
    fly_conn = psycopg2.connect(dbname="fly", user="postgres", password="postgres")
    hotel_conn = psycopg2.connect(dbname="hotel", user="postgres", password="postgres")
    return account_conn, fly_conn, hotel_conn,


def create_tables(account_cursor, fly_cursor, hotel_cursor):
    # create tables in corresponding dbs
    create_account_table(account_cursor)
    create_fly_booking_table(fly_cursor)
    create_hotel_booking_table(hotel_cursor)

    # insert test data
    account_cursor.execute(insert_account(), ('Peter', 1000))
    fly_cursor.execute(insert_fly_booking(), ('Peter', 'LMN 123', 'NY', 'KY', '12/12/2017'))
    hotel_cursor.execute(insert_hotel_booking(), ('Peter', 'Astoria', '12/12/2017', '27/12/2017'))


def create_fly_booking_table(cursor):
    query = """
    CREATE TABLE fly_booking (
    id serial PRIMARY KEY ,
    client_name character varying,
    fly_number character varying,
    from_city character varying,
    to_city character varying,
    date date);
    """
    cursor.execute("DROP TABLE IF EXISTS fly_booking;")
    cursor.execute(query)


def create_hotel_booking_table(cursor):
    query = """
    CREATE TABLE hotel_booking (
    id serial PRIMARY KEY,
    client_name character varying,
    hotel_name character varying,
    arrival date,
    departure date);
    """
    cursor.execute("DROP TABLE IF EXISTS hotel_booking;")
    cursor.execute(query)


def create_account_table(cursor):
    query = """
    CREATE TABLE account (
    id serial PRIMARY KEY,
    client_name character varying,
    amount integer,
    CONSTRAINT account_amount_check CHECK ((amount >= 0)));
    """
    cursor.execute("DROP TABLE IF EXISTS account;")
    cursor.execute(query)

def get_all(conn, table):
    cursor = conn.cursor()
    cursor.execute("SELECT * from {0};".format(table))
    conn.commit()
    return cursor.fetchall()

def insert_fly_booking():
    return """
    INSERT INTO fly_booking (client_name, fly_number, from_city, to_city, date)
    VALUES(%s,%s,%s,%s,%s);
    """

def insert_hotel_booking():
    return """
    INSERT INTO hotel_booking (client_name, hotel_name, arrival, departure)
    VALUES(%s,%s,%s,%s);
    """

def insert_account():
    return """
    INSERT INTO account (client_name, amount)
    VALUES(%s,%s);
    """

def update_account():
    return """
    UPDATE account SET amount=amount - {} WHERE id=1;
    """
