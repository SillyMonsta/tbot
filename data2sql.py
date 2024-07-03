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
    return


def update_pid(scrypt_name, connection_pid):
    query = f"""
            INSERT INTO pid (scrypt_name, pid)
            VALUES (%s, %s)
            ON CONFLICT(scrypt_name) DO UPDATE SET
            pid = EXCLUDED.pid
            """
    cursor.execute(query, (scrypt_name, connection_pid))
    connection.commit()
    return


def update_analyzed_shares_column(ticker, column_name, value):
    query = f"UPDATE analyzed_shares SET {column_name} = %s WHERE ticker = %s"
    cursor.execute(query, (value, ticker))
    connection.commit()


def update_analyzed_shares_column_by_figi(figi, column_name, value):
    query = f"UPDATE analyzed_shares SET {column_name} = %s WHERE figi = %s"
    cursor.execute(query, (value, figi))
    connection.commit()


def update_analyzed_shares_from_telegram(buy, fast_buy, sell, vol, req_vol, ticker):
    query = f"UPDATE analyzed_shares SET buy = %s, fast_buy = %s, sell = %s, vol = %s, req_vol = %s WHERE ticker = %s"
    cursor.execute(query, (buy, fast_buy, sell, vol, req_vol, ticker))
    connection.commit()


def analyzed_shares2sql(analyzed_shares_list):
    query = f"""
        INSERT INTO analyzed_shares (figi, ticker, profit, start_time, start_direction, start_case, start_price, 
        price, target_price, loss_price, loss_percent, target_percent, position_hours, position_days,
        buy, fast_buy, sell, vol, req_vol)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(figi) DO UPDATE SET
                profit = EXCLUDED.profit,
                start_time = EXCLUDED.start_time,
                start_direction = EXCLUDED.start_direction,
                start_case = EXCLUDED.start_case,
                start_price = EXCLUDED.start_price,
                price = EXCLUDED.price,
                target_price = EXCLUDED.target_price,
                loss_price = EXCLUDED.loss_price,
                loss_percent = EXCLUDED.loss_percent,
                target_percent = EXCLUDED.target_percent,
                position_hours = EXCLUDED.position_hours,
                position_days = EXCLUDED.position_days,
                buy = EXCLUDED.buy,
                fast_buy = EXCLUDED.fast_buy,
                sell = EXCLUDED.sell,
                vol = EXCLUDED.vol,
                req_vol = EXCLUDED.req_vol
        """
    cursor.executemany(query, analyzed_shares_list)
    connection.commit()
    return


def update_analyzed_shares(to_update):
    query = f"UPDATE analyzed_shares SET profit = %s, price = %s, target_price = %s, loss_price = %s, " \
            f"loss_percent = %s, target_percent = %s, position_hours = %s, position_days = %s " \
            f"WHERE ticker = %s"
    cursor.execute(query, to_update)
    connection.commit()
    return


def shares2sql(shares_list):
    query = f"""
        INSERT INTO shares (figi, ticker, lot, currency, instrument_name, exchange, sector, 
        min_price_increment, uid)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(figi) DO UPDATE SET
                ticker = EXCLUDED.ticker,
                lot = EXCLUDED.lot,
                currency = EXCLUDED.currency,
                instrument_name = EXCLUDED.instrument_name,
                exchange = EXCLUDED.exchange,
                sector = EXCLUDED.sector,
                min_price_increment = EXCLUDED.min_price_increment,
                uid = EXCLUDED.uid
        """
    cursor.executemany(query, shares_list)
    connection.commit()
    return


def order2sql(order):
    query = """
        INSERT INTO orders (order_id, status, ticker, direction, order_case, price, quantity, order_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
    cursor.executemany(query, order)
    connection.commit()
    return


def trade2sql(trade):
    query = """
        INSERT INTO trades (figi, direction, price, quantity, trade_time)
            VALUES (%s, %s, %s, %s, %s)
        """
    cursor.executemany(query, trade)
    connection.commit()
    return


def ave_trades2sql(ave_trades_list):
    query = """
        INSERT INTO ave_trades (figi, ticker, ave_sell, ave_buy, sum_sell, sum_buy, lot_sells, lot_buys)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(figi) DO UPDATE SET
                ave_sell = EXCLUDED.ave_sell,
                ave_buy = EXCLUDED.ave_buy,
                sum_sell = EXCLUDED.sum_sell,
                sum_buy = EXCLUDED.sum_buy,
                lot_sells = EXCLUDED.lot_sells,
                lot_buys = EXCLUDED.lot_buys
        """
    cursor.executemany(query, ave_trades_list)
    connection.commit()
    return


def update_ave_trades(to_update):
    query = f"UPDATE ave_trades SET ave_sell = %s, ave_buy = %s, sum_sell = %s, sum_buy = %s " \
            f"WHERE figi = %s"
    cursor.execute(query, to_update)
    connection.commit()
    return


def update_lot_trades(to_update):
    query = f"UPDATE ave_trades SET lot_sells = %s, lot_buys = %s " \
            f"WHERE figi = %s"
    cursor.execute(query, to_update)
    connection.commit()
    return


def order_status2sql(order_id, status):
    query = f"""
        UPDATE orders SET status = {status} WHERE order_id = %s
        """
    cursor.execute(query, (order_id,))
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


def balance2sql(table_name, securities_list):
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
    INSERT INTO events_list (ticker, event_case, figi, direction, price, price_position, pseudo_profit, deal_qnt, trend_near, trend_far, event_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(figi, event_time, event_case) DO UPDATE SET
                price = EXCLUDED.price,
                pseudo_profit = EXCLUDED.pseudo_profit,
                trend_near = EXCLUDED.trend_near,
                deal_qnt = EXCLUDED.deal_qnt,
                price_position = EXCLUDED.price_position,
                trend_far = EXCLUDED.trend_far
                """
    cursor.executemany(query, events_list)
    connection.commit()
    return


def lot_trades_list2sql(lot_trades_list):
    query = """
    INSERT INTO lot_trades_list (ticker, lot_trades_case, figi, direction, price, lot_trades_time)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT(figi, lot_trades_time, lot_trades_case) DO UPDATE SET
                price = EXCLUDED.price
                """
    cursor.executemany(query, lot_trades_list)
    connection.commit()
    return
