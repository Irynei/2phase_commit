import sys
from helper import *


def book_flight_and_hotel(account_conn, fly_conn, hotel_conn):
    # begin 2 phase transaction
    fly_conn.tpc_begin(fly_conn.xid(100, 'transaction ID', 'connection 1'))
    hotel_conn.tpc_begin(hotel_conn.xid(100, 'transaction ID', 'connection 2'))
    account_conn.tpc_begin(account_conn.xid(100, 'transaction ID', 'connection 3'))

    try:
        # try to decrease amount by 300
        account_conn.cursor().execute(update_account().format(300))
        account_conn.tpc_prepare()

        # create record in fly bookings
        fly_conn.cursor().execute(insert_fly_booking(), ('Ivan', 'HIU312', 'NY', 'KUI', '27/12/2017'))
        fly_conn.tpc_prepare()

        # create record in hotel bookings
        hotel_conn.cursor().execute(insert_hotel_booking(), ('Ivan', 'Hilton', '27/12/2017', '13/01/2018'))
        hotel_conn.tpc_prepare()

        # 2 phase commit
        fly_conn.tpc_commit()
        account_conn.tpc_commit()
        hotel_conn.tpc_commit()
    except Exception as e:
        print(">>>ERROR: Booking was not performed due to error")
        print(e)
        # in case of error everything should be rolled back
        fly_conn.tpc_rollback()
        account_conn.tpc_rollback()
        hotel_conn.tpc_rollback()
        sys.exit(1)
    else:
        print(">>>Booking performed successfully")

def main():
    account_conn, fly_conn, hotel_conn = connect_to_db()
    fly_cur, hotel_cur, account_cur = fly_conn.cursor(), hotel_conn.cursor(), account_conn.cursor()
    create_tables(account_cur, fly_cur, hotel_cur)

    for i in range(4):
        print('Accounts: ', get_all(account_conn, "account"))
        print('Fly Bookings: ', get_all(fly_conn, "fly_booking"))
        print('Hotel Bookings: ', get_all(hotel_conn, "hotel_booking"))
        print('-' * 50)
        book_flight_and_hotel(account_conn, fly_conn, hotel_conn)



if __name__ == '__main__':
    main()
