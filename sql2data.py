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
    cursor.execute("CREATE TABLE pid (scrypt_name VARCHAR(255), pid VARCHAR(8))")
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
                   "exchange VARCHAR(50), sector VARCHAR(255), trading_status INT, min_price_increment FLOAT,"
                   "uid VARCHAR(255))")
    cursor.execute("""CREATE UNIQUE INDEX shares_figi_idx ON public.shares USING btree (figi)""")
    return


def create_candles(table_name):
    cursor.execute(f"CREATE TABLE {table_name} (figi VARCHAR(50), interval INT,"
                   f"open FLOAT4, high FLOAT4, low FLOAT4, close FLOAT4,"
                   f"volume INT, candle_time TIMESTAMPTZ)")
    cursor.execute(f"""CREATE UNIQUE INDEX {table_name}_candle_time_idx ON public.{table_name} 
                      USING btree (candle_time, figi, "interval")""")
    return


def create_events_list():
    cursor.execute("CREATE TABLE events_list (ticker VARCHAR(50), event_case TEXT, "
                   "figi VARCHAR(50), direction VARCHAR(50), price NUMERIC,"
                   "ef NUMERIC, rsi NUMERIC, bbpb NUMERIC, price_position NUMERIC,"
                   "dif_roc NUMERIC, case_time TIMESTAMPTZ)")
    cursor.execute("""CREATE UNIQUE INDEX events_list_figi_idx 
                    ON public.events_list USING btree (figi, case_time, "event_case")""")
    return


def shares_from_sql():
    cursor.execute(
        '''SELECT *
        FROM shares 
        WHERE exchange LIKE 'MOEX%'
        ORDER BY ticker''')
    results = cursor.fetchall()
    return results


def get_figi_list_from_shares():
    cursor.execute(
        '''SELECT figi
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


def num_rows_1m_from_sql(table_name, figi):
    query = f'''
            SELECT COUNT(*) FROM {table_name} 
            WHERE figi = {figi}
            AND interval = 1
            AND EXTRACT(DOW FROM candle_time) NOT IN (0, 6)
            AND EXTRACT(HOUR FROM candle_time) IN (7, 8, 9, 10, 11, 12, 13, 14, 15)
            '''
    cursor.execute(query)
    results = cursor.fetchall()[0][0]
    return results


def get_all_by_figi_interval(table_name, figi, interval):
    query = f'''
            SELECT * FROM {table_name} 
            WHERE figi = %s
            AND interval = %s
            AND EXTRACT(DOW FROM candle_time) NOT IN (0, 6)
            AND EXTRACT(HOUR FROM candle_time) IN (7, 8, 9, 10, 11, 12, 13, 14, 15)
            ORDER BY candle_time DESC
            '''
    cursor.execute(query, (figi, interval))
    result = cursor.fetchall()
    return result


def get_last_event(table_name, figi):
    query = f'''
            SELECT * FROM {table_name} 
            WHERE figi = %s
            ORDER BY case_time DESC LIMIT 1
            '''
    cursor.execute(query, (figi,))
    result = cursor.fetchall()
    return result


def get_last_candle(table_name):
    query = f'''SELECT *
            FROM (
                SELECT candle_time FROM {table_name}
                WHERE interval = 4
            ) subquery
            ORDER BY candle_time DESC LIMIT 1
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


def get_1hour_candles(figi, hours_qnt, from_data, table_name):
    start_hours_time = from_data - timedelta(hours=hours_qnt)
    end_hours_time = from_data
    query_1hours = f'''
                    SELECT *
                    FROM (
                        SELECT open, high, low, close, volume, candle_time FROM {table_name} 
                        WHERE figi = %s 
                        AND interval = 4
                        AND candle_time >= %s
                    ) subquery
                    WHERE candle_time <= %s
                    ORDER BY candle_time ASC
                    '''
    cursor.execute(query_1hours, (figi, start_hours_time, end_hours_time))
    candles_1hour = cursor.fetchall()
    return candles_1hour


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


def get_sorted_list_by_figi(table_name, column_name, figi):
    query = f'''SELECT {column_name} FROM {table_name} WHERE figi = %s ORDER by case_time ASC'''
    cursor.execute(query, (figi,))
    results = cursor.fetchall()
    return results


def get_time_duration(table_name, column_name, figi):
    query = f'''SELECT (MAX({column_name}) - MIN({column_name})) AS duration 
                FROM (
                    SELECT {column_name} FROM {table_name} WHERE figi = s%
                ) subquery
                ORDER BY {column_name} ASC'''
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
    query = '''SELECT * FROM events_list ORDER BY case_time'''
    cursor.execute(query, )
    results = cursor.fetchall()
    return results
