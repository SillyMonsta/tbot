
import datetime
import sql2data
import requests
import time
from tinkoff.invest.utils import now


def prepare_stream_connection():
    # проверяем есть ли в базе данных таблица acc_id если нет, то создаем, запрашиваем и заполняем
    if sql2data.is_table_exist('acc_id') is False:
        sql2data.create__acc_id()
        requests.request_account_id()
    # проверяем есть ли в базе данных таблица shares если нет, то создаем, запрашиваем и заполняем
    if sql2data.is_table_exist('shares') is False:
        sql2data.create_shares()
        requests.request_shares()
    # проверяем есть ли в базе данных таблица balances если нет, то создаем
    if sql2data.is_table_exist('balances') is False:
        sql2data.create_balances()
    # проверяем есть ли в базе данных таблица events_list если нет, то создаем
    if sql2data.is_table_exist('events_list') is False:
        sql2data.create_events_list()
    # проверяем есть ли в базе данных таблица candles если нет, то создаем
    if sql2data.is_table_exist('candles') is False:
        sql2data.create_candles('candles')
        history_candle_days = [0, 8, 240]
    else:
        try:
            date_last_candle = sql2data.get_last_candle('candles')[0][0]
            days_from_last_candle = ((now() - datetime.datetime.fromisoformat(
                str(date_last_candle))).total_seconds()) / 86400
            history_candle_days = [0, days_from_last_candle, days_from_last_candle]
        except IndexError:
            history_candle_days = [0, 8, 240]
    # формируем список всех акций
    shares = sql2data.shares_from_sql()
    figi_list = []
    for figi_row in shares:
        figi_list.append(figi_row[0])
    requests.request_history_candles(figi_list, history_candle_days, 1, 'candles')
    requests.request_balance()
    return figi_list


prev_now = now()
prev_message = ''
while True:
    # Получаем текущую дату и время
    nowtimemomet = now()
    # Проверяем, что сегодня не выходной
    if nowtimemomet.weekday() < 5:  # 0 - понедельник, 6 - воскресенье
        if 7 <= nowtimemomet.hour < 16:  # время находится в диапазоне от 7.00 до 15.00 по Гринвичу
            figi_list = prepare_stream_connection()
            requests.stream_connection(figi_list)
            message = 'stream_connection stop correctly'
            if message != prev_message:
                print(message, str(datetime.datetime.now())[:19])
            seconds_wait = 10
        # время вне диапазона
        else:
            message = 'time to rest. trade will start at 7:00 GMT'
            if message != prev_message:
                print(message, str(datetime.datetime.now())[:19])
            seconds_wait = 10
    # выходные
    else:
        message = 'weekends. trade will start on monday at 7:00 GMT'
        if message != prev_message:
            print(message, str(datetime.datetime.now())[:19])
        seconds_wait = 360
    prev_now = nowtimemomet
    prev_message = message
    time.sleep(seconds_wait)
