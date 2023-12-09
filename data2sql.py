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


def units_nano_merge(units, nano):
    return units + (nano / 1000000000)


def account_id2sql(acc_id, acc_name):
    query = """
            INSERT INTO acc_id (acc_name, acc_id)
            VALUES (%s, %s)
            ON CONFLICT(acc_name) DO UPDATE SET
                acc_id = EXCLUDED.acc_id
            """
    cursor.execute(query, (acc_name, acc_id))
    connection.commit()


def update_pid(scrypt_name, connection_pid):
    query = f"""
            INSERT INTO pid (scrypt_name, pid)
            VALUES (%s, %s)
            ON CONFLICT(scrypt_name) DO UPDATE SET
            pid = EXCLUDED.pid
            """
    cursor.execute(query, (scrypt_name, connection_pid))
    connection.commit()


def shares2sql(shares_list):
    query = f"""
        INSERT INTO shares (figi, ticker, lot, currency, instrument_name, exchange, sector, trading_status, 
        min_price_increment, uid, pseudo_profit, time_in, last_direction, deal_qnt)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(figi) DO UPDATE SET
                ticker = EXCLUDED.ticker,
                lot = EXCLUDED.lot,
                currency = EXCLUDED.currency,
                instrument_name = EXCLUDED.instrument_name,
                exchange = EXCLUDED.exchange,
                sector = EXCLUDED.sector,
                trading_status = EXCLUDED.trading_status,
                min_price_increment = EXCLUDED.min_price_increment,
                uid = EXCLUDED.uid
        """
    cursor.executemany(query, shares_list)
    connection.commit()
    return


def analyze_events2shares2sql(pseudo_profit, time_in, last_direction, deal_qnt, figi):
    query = '''UPDATE shares 
                SET 
                pseudo_profit = %s,
                time_in = %s,
                last_direction = %s,
                deal_qnt = %s
                WHERE figi = %s'''
    cursor.execute(query, (pseudo_profit, time_in, last_direction, deal_qnt, figi))
    connection.commit()
    return


def clear_table(table_name):
    query = f"DELETE FROM {table_name}"
    cursor.execute(query)
    connection.commit()
    return


def history_candles2sql(table_name, candles_list):
    query = f"""
        INSERT INTO {table_name} (figi, interval, open, high, low, close, volume, candle_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(figi, interval, candle_time) DO UPDATE SET
                close = EXCLUDED.close,
                low = EXCLUDED.low,
                high = EXCLUDED.high,
                volume = EXCLUDED.volume
        """
    cursor.executemany(query, candles_list)
    connection.commit()
    return


def candles2sql(trades2candles_list):
    query = """
        INSERT INTO candles (figi, interval, open, high, low, close, volume, candle_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(figi, interval, candle_time) DO UPDATE SET
                close = EXCLUDED.close,
                low = CASE WHEN EXCLUDED.close < candles.low THEN EXCLUDED.close ELSE candles.low END,
                high = CASE WHEN EXCLUDED.close > candles.high THEN EXCLUDED.close ELSE candles.high END,
                volume = candles.volume + %s
        """
    cursor.executemany(query, trades2candles_list)
    connection.commit()
    return


def securities2sql(table_name, securities_list):
    query = f"""
        INSERT INTO {table_name} (figi, quantity)
            VALUES(%s, %s)
            ON CONFLICT(figi) DO UPDATE SET
                quantity = EXCLUDED.quantity
        """
    cursor.executemany(query, securities_list)
    connection.commit()
    return


def operation_executed2sql(operation_list):
    # перед тем как перезаполнить очищаем нашу таблицу
    cursor.execute("DELETE FROM operation_executed;")
    # команда для sql
    sql = "INSERT INTO operation_executed VALUES(%s, %s, %s, %s, %s, %s)"
    # вносим данные в sql
    cursor.executemany(sql, operation_list)
    # Сохраняем изменения
    connection.commit()
    return


def operation_in_progress2sql(operation_list):
    # перед тем как перезаполнить очищаем нашу таблицу
    cursor.execute("DELETE FROM operation_in_progress;")
    # команда для sql
    sql = "INSERT INTO operation_in_progress VALUES(%s, %s, %s, %s, %s, %s)"
    # вносим данные в sql
    cursor.executemany(sql, operation_list)
    # Сохраняем изменения
    connection.commit()
    return


def events_list2sql(events_list):
    query = """
    INSERT INTO events_list (ticker, event_case, figi, direction, price, ef, rsi, bbpb, price_position, dif_roc, event_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(figi, event_time, event_case) DO UPDATE SET
                price = EXCLUDED.price,
                ef = EXCLUDED.ef,
                rsi = EXCLUDED.rsi,
                bbpb = EXCLUDED.bbpb,
                price_position = EXCLUDED.price_position,
                dif_roc = EXCLUDED.dif_roc
                """
    cursor.executemany(query, events_list)
    connection.commit()
    return
