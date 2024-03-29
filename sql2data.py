from datetime import timedelta
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import get_token_file

con_conf = get_token_file.get_token('connection_config.txt').split()
connection = psycopg2.connect(database=con_conf[0],
                              user=con_conf[1],
                              password=con_conf[2],
                              host=con_conf[3],
                              port=con_conf[4])
connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = connection.cursor()


def is_table_exist(table_name):
    query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')"
    cursor.execute(query, )
    result = cursor.fetchall()[0][0]
    return result


def create_table_pid():
    cursor.execute("CREATE TABLE pid (scrypt_name VARCHAR(255), pid INT)")
    cursor.execute("CREATE UNIQUE INDEX pid_scrypt_name_idx ON public.pid (scrypt_name)")
    return


def create__acc_id():
    cursor.execute("CREATE TABLE acc_id (acc_name VARCHAR(255), acc_id VARCHAR(255))")
    cursor.execute("CREATE UNIQUE INDEX acc_id_pkey ON public.acc_id USING btree (acc_name)")
    return


def create_balances():
    cursor.execute("CREATE TABLE balances (figi VARCHAR(255), quantity INT)")
    cursor.execute("""CREATE UNIQUE INDEX balances_pkey ON public.balances USING btree (figi)""")
    return


def create_shares():
    cursor.execute("CREATE TABLE shares (figi VARCHAR(50), ticker VARCHAR(50),"
                   "lot INT, currency VARCHAR(50), instrument_name VARCHAR(255),"
                   "exchange VARCHAR(50), sector VARCHAR(255), trading_status INT, min_price_increment NUMERIC,"
                   "uid VARCHAR(255))")
    cursor.execute("""CREATE UNIQUE INDEX shares_figi_idx ON public.shares USING btree (figi)""")
    return


def create_candles(table_name):
    cursor.execute(f"CREATE TABLE {table_name} (figi VARCHAR(50), interval INT,"
                   f"open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC,"
                   f"volume INT, candle_time TIMESTAMPTZ)")
    cursor.execute(f"""CREATE UNIQUE INDEX {table_name}_candle_time_idx ON public.{table_name} 
                      USING btree (candle_time, figi, "interval")""")
    return


def create_control_list():
    cursor.execute("CREATE TABLE control_list (ticker VARCHAR(50),"
                   "start_direction VARCHAR(50), start_price NUMERIC, target_price NUMERIC,"
                   "loss_percent NUMERIC, price_change_percent NUMERIC, stop_loss NUMERIC,"
                   "price_start_position_hours NUMERIC, price_position_days NUMERIC, lots_qnt NUMERIC)")

    return


def create_analyzed_shares():
    cursor.execute("CREATE TABLE analyzed_shares (figi VARCHAR(50), ticker VARCHAR(50), profit NUMERIC,"
                   "start_time TIMESTAMPTZ, start_direction VARCHAR(50), start_case TEXT, start_price NUMERIC,"
                   "price NUMERIC, target_price NUMERIC, loss_price NUMERIC, loss_percent NUMERIC,"
                   "target_percent NUMERIC, position_hours NUMERIC, position_days NUMERIC)")
    cursor.execute("""CREATE UNIQUE INDEX analyzed_shares_figi_idx ON public.analyzed_shares USING btree (figi)""")

    return


def create_orders_history():
    cursor.execute("CREATE TABLE orders_history (ticker VARCHAR(50), "
                   "figi VARCHAR(50), direction VARCHAR(50), price NUMERIC,"
                   "quantity NUMERIC, order_id VARCHAR(50), order_time TIMESTAMPTZ)")
    return


def create_events_list():
    cursor.execute("CREATE TABLE events_list (ticker VARCHAR(50), event_case TEXT, "
                   "figi VARCHAR(50), direction VARCHAR(50), price NUMERIC,"
                   "price_position NUMERIC, pseudo_profit NUMERIC, deal_qnt NUMERIC,"
                   "trend_near NUMERIC, trend_far NUMERIC, event_time TIMESTAMPTZ)")
    cursor.execute("""CREATE UNIQUE INDEX events_list_figi_idx 
                    ON public.events_list USING btree (figi, event_time, "event_case")""")
    return


def shares_from_sql():
    cursor.execute(
        '''SELECT *
        FROM shares 
        WHERE exchange LIKE 'MOEX%'
        ORDER BY ticker''')
    results = cursor.fetchall()
    return results


def acc_id_from_sql(acc_name):
    query = '''SELECT acc_id FROM acc_id WHERE acc_name = %s'''
    cursor.execute(query, (acc_name,))
    results = cursor.fetchall()[0][0]
    return results


def pid_from_sql(scrypt_name):
    query = '''SELECT pid FROM pid WHERE scrypt_name = %s'''
    cursor.execute(query, (scrypt_name,))
    results = cursor.fetchall()[0][0]
    return results


def get_all_by_figi_interval(table_name, figi, interval, time_from):
    query = f'''
            SELECT * FROM {table_name} 
            WHERE figi = %s
            AND interval = %s
            AND candle_time > %s
            AND EXTRACT(DOW FROM candle_time) NOT IN (0, 6)
            AND EXTRACT(HOUR FROM candle_time) IN (7, 8, 9, 10, 11, 12, 13, 14, 15)
            ORDER BY candle_time DESC
            '''
    cursor.execute(query, (figi, interval, time_from))
    result = cursor.fetchall()
    return result


def get_last_event(table_name, figi):
    query = f'''
            SELECT * FROM {table_name} 
            WHERE figi = %s
            ORDER BY event_time DESC LIMIT 1
            '''
    cursor.execute(query, (figi,))
    result = cursor.fetchall()
    return result


def get_last_candle_attribute(table_name, attribute):
    query = f'''SELECT *
            FROM (
                SELECT {attribute} FROM {table_name}
                WHERE interval = 4
            ) subquery
            ORDER BY candle_time DESC LIMIT 1
            '''
    cursor.execute(query)
    result = cursor.fetchall()
    return result


def get_last_price(figi):
    query = f'''SELECT close
            FROM (
                SELECT * FROM candles
                WHERE figi = %s
                AND interval = 4
            ) subquery
            ORDER BY candle_time DESC LIMIT 1
            '''
    cursor.execute(query, (figi,))
    result = cursor.fetchall()
    return result


def get_last_time(table_name):
    query = f'''SELECT *
            FROM (
                SELECT event_time FROM {table_name}
            ) subquery
            ORDER BY event_time DESC LIMIT 1
            '''
    cursor.execute(query)
    result = cursor.fetchall()
    return result


def get_1min_candles(figi, minutes_qnt, from_data, table_name):
    start_minutes_time = from_data - timedelta(minutes=minutes_qnt)
    end_minutes_time = from_data
    query_1min = f'''
                SELECT *
                FROM (
                    SELECT open, high, low, close, volume, candle_time FROM {table_name}
                    WHERE figi = %s 
                    AND interval = 1
                    AND candle_time >= %s
                ) subquery
                WHERE candle_time <= %s
                ORDER BY candle_time ASC
                '''
    cursor.execute(query_1min, (figi, start_minutes_time, end_minutes_time))
    candles_1min = cursor.fetchall()
    return candles_1min


def get_day_candles(figi, table_name):
    # запрос дневных свечек за полгода для определения price_position
    query_days = f'''
            SELECT *
            FROM (
                SELECT open, high, low, close, volume, candle_time FROM {table_name} 
                WHERE figi = %s 
                AND interval = 5 
                AND EXTRACT(DOW FROM candle_time) NOT IN (0, 6)
                ORDER BY candle_time DESC LIMIT 180
            ) subquery
            ORDER BY candle_time ASC
            '''
    cursor.execute(query_days, (figi,))
    candles_1day = cursor.fetchall()
    return candles_1day


def get_info_by_figi(table_name, column_name, figi):
    query = f'''SELECT {column_name} FROM {table_name} WHERE figi = %s'''
    cursor.execute(query, (figi,))
    results = cursor.fetchall()
    return results


def get_info_by_ticker(table_name, column_name, ticker):
    query = f'''SELECT {column_name} FROM {table_name} WHERE ticker = %s'''
    cursor.execute(query, (ticker,))
    results = cursor.fetchall()
    return results


def share_from_control_list_by_ticker(ticker):
    query = f'''SELECT * FROM control_list WHERE ticker = %s'''
    cursor.execute(query, (ticker,))
    results = cursor.fetchall()
    return results


def analyzed_share_by_figi(figi):
    query = '''SELECT * FROM analyzed_shares WHERE figi = %s'''
    cursor.execute(query, (figi,))
    results = cursor.fetchall()
    return results


def tickers_from_control_list():
    query = f'''SELECT ticker FROM control_list'''
    cursor.execute(query, )
    results = cursor.fetchall()
    return results


def get_sorted_list_by_figi(table_name, column_name_1, column_name_2, column_name_3, figi, time_from):
    query = f'''
            SELECT {column_name_1}, {column_name_2}, {column_name_3} 
            FROM (
                SELECT * FROM {table_name}
                WHERE figi = %s
            ) subquery
            WHERE event_time >= '{time_from}'
            ORDER by event_time ASC'''
    cursor.execute(query, (figi,))
    results = cursor.fetchall()
    return results


def check_empty_table(table_name):
    query = f'''SELECT COUNT(*) FROM {table_name}'''
    cursor.execute(query)
    results = cursor.fetchall()[0][0]
    if results == 0:
        results = True
    else:
        results = False
    return results


def candles_to_finta(figi, x_time, table_name):
    # Вытаскиваем часовой интервал
    query = f'''
            SELECT *
            FROM (
                SELECT open, high, low, close, volume, candle_time FROM {table_name} 
                WHERE figi = %s 
                AND interval = 4 
                AND EXTRACT(DOW FROM candle_time) NOT IN (0, 6)
                AND EXTRACT(HOUR FROM candle_time) IN (7, 8, 9, 10, 11, 12, 13, 14, 15)
                AND candle_time <= %s
                ORDER BY candle_time DESC LIMIT 60
            ) subquery
            ORDER BY candle_time ASC
            '''
    cursor.execute(query, (figi, x_time))
    candles = cursor.fetchall()
    return candles


def distinct_figi_events():
    query = '''SELECT DISTINCT figi FROM events_list'''
    cursor.execute(query)
    results = cursor.fetchall()
    return results


def all_from_events():
    query = '''SELECT * FROM events_list ORDER BY event_time'''
    cursor.execute(query, )
    results = cursor.fetchall()
    return results
