import requests
import data2sql
import sql2data
from decimal import Decimal
from finta import TA
from tinkoff.invest.utils import now
import pandas as pd
import numpy
import datetime
from datetime import timedelta
import pytz


def get_price_position(figi, table_name):
    # запрос дневных свечек за полгода для определения price_position
    candles_1day = sql2data.get_day_candles(figi, table_name)

    cl = candles_1day[-1][3]
    hi_list = []
    lo_list = []
    for row in candles_1day:
        hi_list.append(row[1])
        lo_list.append(row[2])

    max_hi = max(hi_list)
    min_lo = min(lo_list)

    price_position = (cl / (max_hi - min_lo)) - (min_lo / (max_hi - min_lo))
    return price_position


def check_last_event(figi, direction, case, x_price, x_time):
    last_event = sql2data.get_last_event('events_list', figi)
    if last_event:
        last_case_time = last_event[0][10]
        case_time = x_time
        delta_last_case = case_time - last_case_time
        seconds_from_last_case = int(delta_last_case.total_seconds())
        if direction == 'BUY':
            price_difference_p = (last_event[0][4] - x_price) / last_event[0][4]
        else:
            price_difference_p = (x_price - last_event[0][4]) / last_event[0][4]

        if (seconds_from_last_case > 900 and price_difference_p > 0.001) \
                or last_event[0][3] != direction \
                or last_event[0][1] != case:
            return_data = True
        else:
            return_data = False

    else:
        return_data = True
    return return_data


def prepare_events_extraction():
    # проверяем есть ли в базе данных таблица candles_extraction если нет, то создаем
    if sql2data.is_table_exist('candles_extraction') is False:
        sql2data.create_candles('candles_extraction')
    # проверяем есть ли в базе данных таблица events_list если нет, то создаем
    if sql2data.is_table_exist('events_list') is False:
        sql2data.create_events_list()
        history_candle_days = [20 + 3, 20 + 8, 20 + 240]
        date_last_event_time = now() - timedelta(days=20)
    else:
        # если таблица events_list есть, то определяем глубину извлечения по последнему эвенту в events_list
        try:
            date_last_event_time = sql2data.get_last_time('events_list')[0][0]
            days_from_last_event = ((now() - datetime.datetime.fromisoformat(
                str(date_last_event_time))).total_seconds()) / 86400
            history_candle_days = [days_from_last_event, days_from_last_event, days_from_last_event]
        # если таблица пуста, то запрашиваем на глубину 20 дней
        except IndexError:
            history_candle_days = [20 + 3, 20 + 8, 20 + 240]
            date_last_event_time = now() - timedelta(days=20)
    # запускаем events_extraction
    events_extraction(history_candle_days, date_last_event_time)
    return


def prepare_stream_connection():
    # проверяем есть ли в базе данных таблица pid если нет, то создаем
    if sql2data.is_table_exist('pid') is False:
        sql2data.create_table_pid()
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
    # проверяем есть ли в базе данных таблица candles если нет, то создаем
    if sql2data.is_table_exist('candles') is False:
        sql2data.create_candles('candles')
        history_candle_days = [0, 8, 240]
    # если таблица candles есть, проверяем дату последней свечи в таблице, что-бы понять сколько нужно исторических свеч
    else:
        try:
            date_last_candle = sql2data.get_last_candle_attribute('candles', 'candle_time')[0][0]
            seconds_from_last_candle = (now() - datetime.datetime.fromisoformat(str(date_last_candle))).total_seconds()
            hours_from_last_candle = seconds_from_last_candle / 3600
            # если 7 утра или понедельник и прошло 15 часов или 63 обнуляем days_from_last_candle
            if (abs(hours_from_last_candle - 15) < 0.1 and int(now().hour) == 7) \
                    or (abs(hours_from_last_candle - 63) < 0.1 and int(now().weekday()) == 0):
                days_from_last_candle = 0
            else:
                days_from_last_candle = seconds_from_last_candle / 86400
            history_candle_days = [0, days_from_last_candle, days_from_last_candle]
        except IndexError:
            history_candle_days = [0, 8, 240]

    # формируем список всех акций
    shares = sql2data.shares_from_sql()
    figi_list = []
    for figi_row in shares:
        figi_list.append(figi_row[0])

    if sum(history_candle_days):
        requests.request_history_candles(figi_list, history_candle_days, 1, 'candles')
    requests.request_balance()
    return figi_list


def update_last_candle(hi, lo, cl, vo, new_candles):
    hi_new = []
    lo_new = []
    vo_new = []
    for row in new_candles:
        hi_new.append(row[1])
        lo_new.append(row[2])
        vo_new.append(row[4])
    # вычисляем изменения свечки
    else:
        try:
            if max(hi_new) > hi:
                hi = max(hi_new)
            if min(lo_new) < lo:
                lo = min(lo_new)
            cl = new_candles[-1][3]
            vo = vo + sum(vo_new)
        except ValueError:
            print('empty new_candles')
    return hi, lo, cl, vo


def analyze_candles(figi, events_extraction_case, x_time, table_name):
    candles = sql2data.candles_to_finta(figi, x_time, table_name)
    events_list = []
    op = []
    hi = []
    lo = []
    cl = []
    vo = []
    for row in candles:
        op.append(float(row[0]))
        hi.append(float(row[1]))
        lo.append(float(row[2]))
        cl.append(float(row[3]))
        vo.append(float(row[4]))
    # для уверенности в том что все списки построены до конца
    else:
        # если это извлечение, то надо обновить последнюю часовую свечу
        if events_extraction_case:
            # вычисляем разницу минут между последней в candles свечой, и свечой от которой идёт отсчёт(назад)
            date1 = x_time
            date2 = candles[-1][5]
            delta = date1 - date2
            quantity_of_minutes_total = int(delta.total_seconds() / 60)
            quantity_of_minutes = quantity_of_minutes_total - int(quantity_of_minutes_total / 60) * 60
            # если есть разница значит прошло столько минут
            if quantity_of_minutes:
                # запрашиваем в sql столько минутных свечек
                new_candles = sql2data.get_1min_candles(figi, quantity_of_minutes, date1, table_name)
                # обновляем последнюю свечку через функцию
                new_candle = update_last_candle(hi[-1], lo[-1], cl[-1], vo[-1], new_candles)
                hi[-1] = float(new_candle[0])
                lo[-1] = float(new_candle[1])
                cl[-1] = float(new_candle[2])
                vo[-1] = float(new_candle[3])

        last_price = Decimal(cl[-1])

        dict_ohlcv = {
            'open': op,
            'high': hi,
            'low': lo,
            'close': cl,
            'volume': vo
        }
        ohlcv = pd.DataFrame(data=dict_ohlcv)

        rsi = TA.RSI(ohlcv)
        ef = TA.EFI(ohlcv)
        roc = TA.ROC(ohlcv)
        pb = TA.PERCENT_B(ohlcv)
        max_ef = numpy.nanmax(ef)
        min_ef = numpy.nanmin(ef)
        prev_ef = ef[len(ef) - 2]
        last_ef = ef[len(ef) - 1]
        last_rsi = rsi[len(ef) - 1]
        last_pb = pb[len(pb) - 1]
        prev_roc = roc[len(roc) - 2]
        last_roc = roc[len(roc) - 1]
        try:
            if last_roc != 0 and prev_roc != 0:
                dif_roc = (last_roc - prev_roc) / last_roc
            else:
                dif_roc = 0
        except TypeError:
            dif_roc = 0

        sell_strength = 0
        case = ''
        if last_pb > 1:
            sell_strength += 1
            case = case + ' PB>1'

        if last_ef - prev_ef < 0 and prev_ef >= max_ef * 0.7 and last_rsi > 70:
            sell_strength += 1
            case = case + ' maxEF&RSI'

        if dif_roc > 1:
            sell_strength += 1
            case = case + ' difROC>1'

        if sell_strength >= 2:
            if check_last_event(figi, 'SELL', case, last_price, x_time):
                price_position = get_price_position(figi, table_name)
                share = sql2data.get_info_by_figi('shares', '*', figi)[0]
                events_list.append((share[1], case, figi, 'SELL', last_price, round(last_ef), round(last_rsi),
                                    round(last_pb, 3), round(price_position, 3), round(dif_roc, 3),
                                    x_time.replace(microsecond=0)))
                data2sql.events_list2sql(events_list)
                analyze_events(figi)
                print(share[1], case, 'SELL', cl[-1], x_time.replace(microsecond=0))

        buy_strength = 0
        if last_pb < 0:
            buy_strength += 1
            case = case + ' PB<0'

        if 0 < last_ef - prev_ef and prev_ef <= min_ef * 0.7 and last_rsi < 30:
            buy_strength += 1
            case = case + ' minEF&RSI'

        if dif_roc < -1:
            buy_strength += 1
            case = case + ' difROC<-1'

        if buy_strength >= 2:
            if check_last_event(figi, 'BUY', case, last_price, x_time):
                price_position = get_price_position(figi, table_name)
                share = sql2data.get_info_by_figi('shares', '*', figi)[0]
                events_list.append((share[1], case, figi, 'BUY', last_price, round(last_ef), round(last_rsi),
                                    round(last_pb, 3), round(price_position, 3), round(dif_roc, 3),
                                    x_time.replace(microsecond=0)))
                data2sql.events_list2sql(events_list)
                analyze_events(figi)
                print(share[1], case, 'BUY', cl[-1], x_time.replace(microsecond=0))
    return


def analyze_sametime_cases():
    results = sql2data.all_from_events()
    prev_case_time = ''
    prev_direction = ''
    prev_ticker = ''

    for result in results:
        figi = result[2]
        direction = result[3]
        price = result[4]
        price_position = result[8]
        case_time = result[10]
        ticker = result[0]

        # ищем сделки совершенные в одно и тоже, или почти одно и тоже, время
        if prev_case_time:
            date1 = datetime.datetime.fromisoformat(case_time)
            date2 = datetime.datetime.fromisoformat(prev_case_time)
            delta = date1 - date2
            dif_in_sec = delta.total_seconds()

            if dif_in_sec < 60:
                print(prev_ticker, prev_case_time, prev_direction, ticker, case_time, prev_direction, dif_in_sec)

        prev_case_time = case_time
        prev_ticker = ticker
        prev_direction = direction
        prev_figi = figi
        prev_price_position = price_position
        prev_price = price

    return


def analyze_events(figi):
    time_start = now() - datetime.timedelta(days=20)
    directions_list = sql2data.get_sorted_list_by_figi('events_list', 'direction', figi, time_start)
    price_list = sql2data.get_sorted_list_by_figi('events_list', 'price', figi, time_start)
    case_time_list = sql2data.get_sorted_list_by_figi('events_list', 'event_time', figi, time_start)

    if directions_list:
        prev_direction = directions_list[0][0]
        list_prices = []
        for di in range(0, len(directions_list)):
            price = price_list[di][0]
            direction = directions_list[di][0]
            case_time = case_time_list[di][0]
            if di == 0:
                list_prices.append((direction, price, case_time))
            if prev_direction != direction:
                list_prices.append((direction, price, case_time))
            prev_direction = direction
        else:
            prev_deal = ('', 0, '')
            profit_currency_list = []
            profit_share_list = []
            duration_long_deal_list = []
            duration_short_deal_list = []
            for deal in list_prices:
                deal_direction = deal[0]
                deal_price = deal[1]
                deal_time = deal[2]
                prev_deal_direction = prev_deal[0]
                prev_deal_price = prev_deal[1]
                prev_deal_time = prev_deal[2]

                # лонг
                if deal_direction == 'SELL' and prev_deal_direction == 'BUY':
                    delta = deal_time - prev_deal_time
                    profit_currency_list.append(round((deal_price - prev_deal_price) / deal_price, 4))
                    duration_long_deal_list.append(delta.total_seconds() / 3600)
                # шорт
                if deal_direction == 'BUY' and prev_deal_direction == 'SELL':
                    delta = deal_time - prev_deal_time
                    profit_share_list.append(round((prev_deal_price - deal_price) / prev_deal_price, 4))
                    duration_short_deal_list.append(delta.total_seconds() / 3600)

                prev_deal = deal
            else:
                deal_qnt = len(profit_share_list) + len(profit_currency_list)

                if profit_share_list and profit_currency_list:
                    average_profit = sum(profit_share_list) / len(profit_share_list) + \
                                     sum(profit_currency_list) / len(profit_currency_list)
                else:
                    average_profit = sum(profit_share_list + profit_share_list)

                if len(list_prices):
                    last_direction = list_prices[-1][0]
                else:
                    last_direction = ''
                if average_profit == 0 and list_prices[-1][1]:
                    last_price = sql2data.get_last_price(figi)[0][0]
                    if last_direction == 'BUY':
                        average_profit = (last_price - list_prices[-1][1]) / last_price
                    if last_direction == 'SELL':
                        average_profit = (list_prices[-1][1] - last_price) / list_prices[-1][1]

                ticker = sql2data.get_info_by_figi('shares', 'ticker', figi)[0][0]

                data2sql.analyze_events2shares2sql(average_profit, list_prices[0][2], last_direction, deal_qnt, figi)
                print(ticker,  # figi,
                      'pseudo_profit', average_profit,
                      'time_in', list_prices[0][2],
                      'last_direction', last_direction,
                      'deal_qnt', deal_qnt)
                print()
    return


def events_extraction(history_candle_days, time_from):
    date_start_events_extraction = datetime.datetime.now()
    # достаём весь список акций (торгующихся на МОЭКС)
    shares = sql2data.shares_from_sql()
    # Запускаем тест для каждой из акций
    for figi_row in shares:
        #try:
        figi = figi_row[0]
        ticker = figi_row[1]
        requests.request_history_candles([figi], history_candle_days, 0, 'candles_extraction')
        # запрашиваем количество минутных свечей и отнимаем у них 60, точка старта тестирования
        figi_1m_candles = sql2data.get_all_by_figi_interval('candles_extraction', figi, 1, time_from)
        days_from_last_event = ((now() - datetime.datetime.fromisoformat(
            str(time_from))).total_seconds()) / 86400
        if days_from_last_event < 20:
            index = len(figi_1m_candles) - 1
        else:
            index = len(figi_1m_candles) - 60
        # через полученный индекс получаем время с которого начнем
        try:
            x_time = figi_1m_candles[index][7]
            print('start events_extraction x_time:', x_time, ticker, figi, str(datetime.datetime.now())[:19])
        except IndexError:
            print(ticker, 'error getting x_time: empty 1m_candles_list', datetime.datetime.now())
            continue
        # запускаем цикл теста от заданной даты в сторону возрастания, в качестве шага каждая минутная свеча
        for index_row1m in range(index, 0, -1):
            analyze_candles(figi, True, x_time, 'candles_extraction')
            x_time = figi_1m_candles[index_row1m][7]
        #except Exception as e:
        #    print(e)
        #    continue

    print('events_extraction done', str(datetime.datetime.now())[:19],
          '  time spent:', datetime.datetime.now() - date_start_events_extraction)

    # запускаем анализ полученных эвентов
    results = sql2data.distinct_figi_events()
    for result in results:
        figi = result[0]
        analyze_events(figi)
    print('analyze_events done, shares updated', str(datetime.datetime.now())[:19])
    return
