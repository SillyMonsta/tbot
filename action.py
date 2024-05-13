import tinkoff_requests
import data2sql
import sql2data
from decimal import Decimal
from finta import TA
from tinkoff.invest.utils import now
import pandas as pd
import numpy
import datetime
from datetime import timedelta
import write2file
import time


def make_multiple(num, divisor):
    result = (num // divisor) * divisor
    decimals = len(str(divisor).split('.')[1]) if '.' in str(divisor) else 0
    return Decimal(round(result, decimals))


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


def check_trading_status(figi, events_extraction_case):
    if events_extraction_case:
        trading_status = True
    elif tinkoff_requests.request_trading_status(figi) == 5:
        trading_status = True
    else:
        trading_status = False
    return trading_status


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
            write2file.write(str(datetime.datetime.now())[:19] +
                             ' action.py --> update_last_candle --> ValueError --> empty new_candles', 'log.txt')
    return hi, lo, cl, vo


def prepare_stream_connection():
    # проверяем есть ли в базе данных таблица pid если нет, то создаем
    if sql2data.is_table_exist('pid') is False:
        sql2data.create_table_pid()
        data2sql.update_pid('stream_connection', 0)
    # проверяем есть ли в базе данных таблица acc_id если нет, то создаем, запрашиваем и заполняем
    if sql2data.is_table_exist('acc_id') is False:
        sql2data.create__acc_id()
        tinkoff_requests.request_account_id()
    # проверяем есть ли в базе данных таблица shares если нет, то создаем, запрашиваем и заполняем
    if sql2data.is_table_exist('shares') is False:
        sql2data.create_shares()
        tinkoff_requests.request_shares()
    # проверяем есть ли в базе данных таблица balances если нет, то создаем
    if sql2data.is_table_exist('balance') is False:
        sql2data.create_balance()
    if sql2data.is_table_exist('analyzed_shares') is False:
        sql2data.create_analyzed_shares()
    if sql2data.is_table_exist('orders') is False:
        sql2data.create_orders()
    # проверяем есть ли в базе данных таблица candles если нет, то создаем
    if sql2data.is_table_exist('candles') is False:
        sql2data.create_candles('candles')
        history_candle_days = [0, 8, 240]
    # если таблица candles есть,
    else:
        # если пид не ноль значит остановка была нештатной, надо проверить свечи
        if sql2data.pid_from_sql('stream_connection'):
            history_candle_days = [0, 8, 240]
        # иначе, если пид ноль значит стрим был остановлен штатно, свечи не проверяем
        else:
            history_candle_days = [0, 0, 0]

    # формируем список всех акций
    shares = sql2data.shares_from_sql()
    figi_list = []
    for figi_row in shares:
        figi_list.append(figi_row[0])

    if sum(history_candle_days):
        tinkoff_requests.request_history_candles(figi_list, history_candle_days, 1, 'candles')
    tinkoff_requests.request_balance()
    return figi_list


def check_orders(ticker):
    not_finished_orders = sql2data.get_orders_id_state(ticker)
    for order in not_finished_orders:
        order_id = order[0]
        old_order_status = order[1]
        order_time = order[7]
        delta_time = (now() - order_time).total_seconds()
        # Тестовые ордера специально ставятся со статусом 6, который потом меняется на 7
        if old_order_status == 6:
            new_order_status = 7
        else:
            # Ордер стоящий больше получаса отменяем
            if delta_time > 1800:
                tinkoff_requests.cancel(order_id)
            # Если ордер не тестовый, то запрашиваем его текущий статус
            new_order_status = tinkoff_requests.get_order_status(order_id)
        # Обновляем статус ордера в таблице orders
        data2sql.order_status2sql(order_id, new_order_status)
    return


def fast_sell(rub_vol, x_time, events_extraction_case):
    figi = sql2data.get_figi_to_fast_sell(rub_vol)[0][0]
    analyzed_share = sql2data.analyzed_share_by_figi(figi)
    lot = sql2data.get_info_by_figi('shares', 'lot', figi)[0][0]
    ticker = analyzed_share[0][1]
    direction = 'SELL'
    price = analyzed_share[0][7]
    old_vol = analyzed_share[0][17]

    vol = int(rub_vol / price / lot) + 1

    if events_extraction_case:
        # тестовый ордер
        order_response = [str(time.time()), 6]
    else:
        order_response = tinkoff_requests.market_order(figi, direction, vol)
    order_id = order_response[0]
    status = order_response[1]
    if order_id and status:
        if events_extraction_case:
            rub_balance = sql2data.get_rub_balance()[0][0]
            data2sql.update_analyzed_shares_column_by_figi(figi, 'vol', old_vol - vol)
            data2sql.balance2sql('balance', [('rub', rub_balance + price * vol)])
        else:
            tinkoff_requests.request_balance()
        data2sql.order2sql([(order_id, status, ticker, direction, 'FAST_SELL', price, vol, x_time)])
        sold = True
    else:
        # здесь надо добавить запрос открытых ордеров, если среди открытых ордеров нет того
        # который только что пытались поставить значит ордер не состоялся
        sold = False
    return sold


def check_and_trade(figi, ticker, start_price, start_direction, direction, last_price, case, x_time,
                    max_hi_hours, min_lo_hours, table_name, buy, fast_buy, sell, vol, req_vol, events_extraction_case,
                    true_analyzed_shares):
    if check_last_event(figi, direction, case, last_price, x_time):
        min_price_increment = sql2data.get_info_by_figi('shares', 'min_price_increment', figi)[0][0]
        lot = sql2data.get_info_by_figi('shares', 'lot', figi)[0][0]

        position_hours = (last_price / (max_hi_hours - min_lo_hours)) - \
                         (min_lo_hours / (max_hi_hours - min_lo_hours))
        position_days = get_price_position(figi, table_name)

        now_case = (direction, last_price, x_time)

        result_analyze_events = analyze_events(figi, now_case)

        if direction == 'SELL':
            current_profit = (start_price - last_price) / start_price
            profit = result_analyze_events[0]
            target_percent = -((max_hi_hours - min_lo_hours) / max_hi_hours) * Decimal(0.7)
            loss_percent = None
            loss_price = None
            if sell and vol and check_trading_status(figi, events_extraction_case):
                if events_extraction_case:
                    # тестовый ордер
                    order_response = [str(time.time()), 6]
                else:
                    # реальный ордер
                    order_response = tinkoff_requests.market_order(figi, direction, vol / lot)
                order_id = order_response[0]
                status = order_response[1]
                if order_id and status:
                    data2sql.order2sql([(order_id, status, ticker, direction, case, last_price, vol, x_time)])
                    if events_extraction_case:
                        rub_balance = sql2data.get_rub_balance()[0][0]
                        data2sql.update_analyzed_shares_column_by_figi(figi, 'vol', 0)
                        data2sql.balance2sql('balance', [('rub', rub_balance + last_price * vol)])
                    else:
                        tinkoff_requests.request_balance()

        else:
            current_profit = (last_price - start_price) / last_price
            profit = result_analyze_events[1]
            target_percent = ((max_hi_hours - min_lo_hours) / max_hi_hours) * Decimal(0.7)
            loss_percent = target_percent * Decimal(1.4)
            loss_price = make_multiple(last_price - last_price * loss_percent, min_price_increment)
            if buy and check_trading_status(figi, events_extraction_case) and vol < req_vol:
                dif_vol = req_vol - vol
                rub_balance = sql2data.get_rub_balance()[0][0]
                if rub_balance > last_price * dif_vol:
                    if events_extraction_case:
                        # тестовый ордер
                        order_response = [str(time.time()), 6]
                    else:
                        # реальный ордер
                        order_response = tinkoff_requests.market_order(figi, direction, dif_vol / lot)
                    order_id = order_response[0]
                    status = order_response[1]
                    if order_id and status:
                        data2sql.order2sql([(order_id, status, ticker, direction, case, last_price, dif_vol, x_time)])
                        if events_extraction_case:
                            data2sql.update_analyzed_shares_column_by_figi(figi, 'vol', vol + dif_vol)
                            data2sql.balance2sql('balance', [('rub', rub_balance - last_price * dif_vol)])
                        else:
                            tinkoff_requests.request_balance()

        deal_qnt = result_analyze_events[2]
        trend_far = result_analyze_events[3]
        trend_near = result_analyze_events[5]

        target_price = make_multiple(last_price + last_price * target_percent, min_price_increment)

        if direction == start_direction and true_analyzed_shares:
            to_update = (profit, last_price, target_price, loss_price,
                         loss_percent, target_percent, position_hours, position_days, ticker)
            data2sql.update_analyzed_shares(to_update)
        else:
            data2sql.analyzed_shares2sql([(figi, ticker, current_profit, x_time, direction, case, last_price,
                                           last_price, target_price, loss_price, loss_percent, target_percent,
                                           position_hours, position_days, buy, fast_buy, sell, vol, req_vol)])

        data2sql.events_list2sql([(ticker, case, figi, direction, last_price, round(position_days, 3),
                                   round(profit, 3), deal_qnt, round(trend_near, 3), round(trend_far, 3), x_time)])
        trade = True
    else:
        trade = False

    return trade


def analyse_ohlcv(ohlcv):
    sell_strength = 0
    buy_strength = 0
    sell_case = ''
    buy_case = ''

    rsi = TA.RSI(ohlcv)
    ef = TA.EFI(ohlcv)
    roc = TA.ROC(ohlcv, 5)
    pb = TA.PERCENT_B(ohlcv)

    max_ef = numpy.nanmax(ef)
    min_ef = numpy.nanmin(ef)
    prev_ef = ef[len(ef) - 2]
    last_ef = ef[len(ef) - 1]
    last_rsi = rsi[len(ef) - 1]
    last_pb = pb[len(pb) - 1]
    prev_roc = roc[len(roc) - 2]
    last_roc = roc[len(roc) - 1]

    roc_level_u = min(sorted([x for x in roc.tolist()[:-1] if not numpy.isnan(x)], reverse=True)[:3])
    roc_level_l = max(sorted([x for x in roc.tolist()[:-1] if not numpy.isnan(x)], reverse=False)[:3])

    if last_pb > 1:
        sell_strength += 1
        sell_case = sell_case + ' PB>1'

    if (last_ef - prev_ef < 0 and prev_ef >= max_ef * 0.7) and last_rsi > 70:
        sell_strength += 1
        sell_case = sell_case + ' maxEF&RSI'

    if (last_roc < roc_level_u and (prev_roc > roc_level_u or prev_roc == roc_level_u)) \
            or (last_roc < 0 and (prev_roc > 0 or prev_roc == 0)):
        sell_strength += 1
        sell_case = sell_case + ' ROC-s'

    if (last_roc > roc_level_l and (prev_roc < roc_level_l or prev_roc == roc_level_l)) \
            or (last_roc > 0 and (prev_roc < 0 or prev_roc == 0)):
        buy_strength += 1
        buy_case = buy_case + ' ROC-b'

    if last_pb < 0:
        buy_strength += 1
        buy_case = buy_case + ' PB<0'

    if (0 < last_ef - prev_ef and prev_ef <= min_ef * 0.7) and last_rsi < 30:
        buy_strength += 1
        buy_case = buy_case + ' minEF&RSI'

    return sell_strength, buy_strength, sell_case, buy_case


def analyze_candles(figi, events_extraction_case, x_time, table_name):
    candles = sql2data.candles_to_finta(figi, x_time, table_name)
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

        last_price = candles[-1][3]

        dict_ohlcv = {
            'open': op,
            'high': hi,
            'low': lo,
            'close': cl,
            'volume': vo
        }
        ohlcv = pd.DataFrame(data=dict_ohlcv)

        strength_case = analyse_ohlcv(ohlcv)

        sell_strength = strength_case[0]
        buy_strength = strength_case[1]
        sell_case = strength_case[2]
        buy_case = strength_case[3]

        max_hi_hours = Decimal(max(hi))
        min_lo_hours = Decimal(min(lo))

        analyzed_share = sql2data.analyzed_share_by_figi(figi)

        if analyzed_share:
            min_price_increment = sql2data.get_info_by_figi('shares', 'min_price_increment', figi)[0][0]
            sold = False

            ticker = analyzed_share[0][1]
            start_time = analyzed_share[0][3]
            start_direction = analyzed_share[0][4]
            start_price = analyzed_share[0][6]
            loss_price = analyzed_share[0][9]
            loss_percent = analyzed_share[0][10]
            prev_position_hours = analyzed_share[0][12]
            prev_position_days = analyzed_share[0][13]
            buy = analyzed_share[0][14]
            fast_buy = analyzed_share[0][15]
            sell = analyzed_share[0][16]
            vol = analyzed_share[0][17]
            req_vol = analyzed_share[0][18]

            # проверяем есть ли в таблице orders неисполненные ордера
            check_orders(ticker)

            position_hours = (last_price / (max_hi_hours - min_lo_hours)) - \
                             (min_lo_hours / (max_hi_hours - min_lo_hours))
            position_days = get_price_position(figi, table_name)

            # если start_direction BUY то определяем target_percent, loss_percent и loss_price
            if start_direction == 'BUY':
                profit = (last_price - start_price) / last_price
                target_percent = ((max_hi_hours - min_lo_hours) / max_hi_hours) * Decimal(0.7)
                if loss_percent is None:
                    loss_percent = target_percent * Decimal(1.4)
                # если last_price опустилась ниже loss_price то STOP_LOSS
                if last_price <= loss_price:
                    # обнуляем buy после стоп-лоса, исключаем, на некоторое время, актив из списка покупаемых
                    buy = 0
                    sold = check_and_trade(figi, ticker, start_price, start_direction,
                                           'SELL', last_price, 'STOP_LOSS', x_time, max_hi_hours, min_lo_hours,
                                           table_name, buy, fast_buy, sell, vol, req_vol, events_extraction_case)
                    # если продажа STOP_LOSS состоялась обнуляем sell_strength
                    if sold and sell_strength >= 2:
                        sell_strength = 0
                # если цена поднялась и расстояние между loss_price и ценой стало больше, пересчитываем loss_price
                elif (last_price - loss_price) / last_price > loss_percent:
                    loss_price = make_multiple(last_price - last_price * loss_percent, min_price_increment)
            # если SELL то определяем target_percent (отрицательный), loss_percent и loss_price при покупке не нужны
            else:
                target_percent = -((max_hi_hours - min_lo_hours) / max_hi_hours) * Decimal(0.7)
                profit = (start_price - last_price) / start_price
                loss_percent = None
                loss_price = None

            target_price = make_multiple(start_price + start_price * target_percent, min_price_increment)

            # если цена поднялась выше порога target<
            if last_price >= target_price and start_direction == 'BUY':
                sell_case = sell_case + ' target<'
                sell_strength += 1
            # если цена опустилась ниже порога target>
            if last_price <= target_price and start_direction == 'SELL':
                buy_case = buy_case + ' target>'
                buy_strength += 1

            # если рост на масштабе дней развернулся и цена превысила target
            if prev_position_days == 1 and sell_case.split(' ')[-1] == 'target<':
                if position_days < Decimal(0.98):
                    sell_strength += 1
                    sell_case = sell_case + ' days_rvrs'
                else:
                    position_days = Decimal(1)
            # если падение на масштабе дней развернулось и цена опустилась ниже target
            if prev_position_days == 0 and buy_case.split(' ')[-1] == 'target>':
                if position_days > Decimal(0.02):
                    buy_strength += 1
                    buy_case = buy_case + ' days_rvrs'
                else:
                    position_days = 0

            # если рост на масштабе часов развернулся и цена превысила target
            if prev_position_hours == 1 and sell_case.split(' ')[-1] == 'target<':
                if position_hours < Decimal(0.98):
                    sell_strength += 1
                    sell_case = sell_case + ' hours_rvrs'
                else:
                    position_hours = Decimal(1)
            # если падение на масштабе дней развернулось и цена опустилась ниже target
            if prev_position_hours == 0 and buy_case.split(' ')[-1] == 'target>':
                if position_hours > Decimal(0.02):
                    buy_strength += 1
                    buy_case = buy_case + ' hours_rvrs'
                else:
                    position_hours = 0

            # условия для торговли
            if sell_strength >= 2 \
                    and (sell_case.split(' ')[-1] == 'target<' or sell_case.split(' ')[-2] == 'target<') \
                    and position_days != 1 and position_days != 0 \
                    and position_hours != 1 and position_hours != 0:
                sold = check_and_trade(figi, ticker, start_price, start_direction, 'SELL', last_price, sell_case,
                                       x_time, max_hi_hours, min_lo_hours, table_name, buy, fast_buy, sell, vol,
                                       req_vol, events_extraction_case, True)

            if buy_strength >= 2 \
                    and position_days != 1 and position_days != 0 \
                    and position_hours != 1 and position_hours != 0:
                sold = check_and_trade(figi, ticker, start_price, start_direction, 'BUY', last_price, buy_case, x_time,
                                       max_hi_hours, min_lo_hours, table_name, buy, fast_buy, sell, vol,
                                       req_vol, events_extraction_case, True)

            # если торговли не было, то просто обновляем данные в таблице analyzed_shares
            if sold is False:
                to_update = (profit, last_price, target_price, loss_price,
                             loss_percent, target_percent, position_hours, position_days, ticker)
                data2sql.update_analyzed_shares(to_update)

        # если акции нет в таблице analyzed_shares
        else:
            ticker = sql2data.get_info_by_figi('shares', 'ticker', figi)[0][0]
            start_price = last_price
            lot = sql2data.get_info_by_figi('shares', 'lot', figi)[0][0]

            if events_extraction_case:
                buy = 1
                fast_buy = 0
                sell = 1
            else:
                buy = None
                fast_buy = None
                sell = None
            vol = None
            req_vol = None

            if sell_strength >= 2:
                if events_extraction_case:
                    vol = 10000 / last_price / lot / 2
                    req_vol = 10000 / last_price / lot
                start_direction = 'SELL'
                check_and_trade(figi, ticker, start_price, start_direction, 'SELL', last_price, sell_case, x_time,
                                max_hi_hours, min_lo_hours, table_name, buy, fast_buy, sell, vol, req_vol,
                                events_extraction_case, False)
            if buy_strength >= 2:
                if events_extraction_case:
                    rub_balance = sql2data.get_rub_balance()[0][0]
                    data2sql.balance2sql('balance', [('rub', rub_balance + 10000)])
                start_direction = 'BUY'
                check_and_trade(figi, ticker, start_price, start_direction, 'BUY', last_price, buy_case, x_time,
                                max_hi_hours, min_lo_hours, table_name, buy, fast_buy, sell, vol, req_vol,
                                events_extraction_case, False)
    return


def analyze_events(figi, now_case):
    time_start = now() - datetime.timedelta(days=30)
    list_dir_price_time = sql2data.get_sorted_list_by_figi('events_list', 'direction', 'price', 'event_time', figi,
                                                           time_start)
    # добавляем текущий случай, то, чего ещё нет в таблице events_list
    list_dir_price_time.append(now_case)
    last_prices_same_dir = []
    trend = 0
    periodicity = 0
    deal_qnt = 0
    pp_long = 0  # pseudo_profit_long
    pp_short = 0  # pseudo_profit_short
    ave_pp_short = 0
    ave_pp_long = 0
    if list_dir_price_time:
        prev_direction = list_dir_price_time[0][0]
        list_cases = []
        for di in range(0, len(list_dir_price_time)):
            direction = list_dir_price_time[di][0]
            price = list_dir_price_time[di][1]
            case_time = list_dir_price_time[di][2]
            if di == 0:
                list_cases.append((direction, price, case_time))
                last_prices_same_dir = [price]
            if prev_direction != direction:
                list_cases.append((direction, price, case_time))
                last_prices_same_dir = [price]
            else:
                last_prices_same_dir.append(price)
            prev_direction = direction
        prev_prev_deal = ('', 0, '')
        prev_deal = ('', 0, '')
        profit_long_list = []
        profit_short_list = []
        for deal in list_cases:
            deal_direction = deal[0]
            deal_price = deal[1]
            deal_time = deal[2]
            prev_deal_direction = prev_deal[0]
            prev_deal_price = prev_deal[1]
            prev_deal_time = prev_deal[2]

            # лонг
            if deal_direction == 'SELL' and prev_deal_direction == 'BUY':
                profit_long_list.append(round((deal_price - prev_deal_price) / deal_price, 4))
                if prev_prev_deal[1]:
                    trend = (deal_price - prev_prev_deal[1]) / deal_price
                    periodicity = (deal_time - prev_prev_deal[2]).total_seconds() / 3600
            # шорт
            if deal_direction == 'BUY' and prev_deal_direction == 'SELL':
                profit_short_list.append(round((prev_deal_price - deal_price) / prev_deal_price, 4))
                if prev_prev_deal[1]:
                    trend = (deal_price - prev_prev_deal[1]) / deal_price
                    periodicity = (deal_time - prev_prev_deal[2]).total_seconds() / 3600

            prev_prev_deal = prev_deal
            prev_deal = deal

        deal_qnt = len(profit_short_list) + len(profit_long_list)

        if profit_short_list:
            pp_short = profit_short_list[-1]
            ave_pp_short = sum(profit_short_list) / len(profit_short_list)
        if profit_long_list:
            pp_long = profit_long_list[-1]
            ave_pp_long = sum(profit_long_list) / len(profit_long_list)

    average_profit = (ave_pp_short + ave_pp_long) / 2

    near_trend = (last_prices_same_dir[-1] - last_prices_same_dir[0]) / last_prices_same_dir[-1]

    return pp_long, pp_short, deal_qnt, trend, periodicity, near_trend, average_profit


def prepare_events_extraction():
    # проверяем есть ли в базе данных таблица candles_extraction если нет, то создаем
    if sql2data.is_table_exist('candles_extraction') is False:
        sql2data.create_candles('candles_extraction')
    # проверяем есть ли в базе данных таблица orders
    if sql2data.is_table_exist('orders') is False:
        sql2data.create_orders()
    # проверяем есть ли в базе данных таблица analyzed_shares
    if sql2data.is_table_exist('analyzed_shares') is False:
        sql2data.create_analyzed_shares()
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


def events_extraction(history_candle_days, time_from):
    data2sql.update_pid('events_extraction', 1)
    date_start_events_extraction = datetime.datetime.now()
    # достаём весь список акций (торгующихся на МОЭКС)
    shares = sql2data.shares_from_sql()
    # Запускаем тест для каждой из акций
    for figi_row in shares:
        figi = figi_row[0]
        ticker = figi_row[1]
        try:
            tinkoff_requests.request_history_candles([figi], history_candle_days, 0, 'candles_extraction')
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
                write2file.write(str(datetime.datetime.now())[:19] +
                                 ' start events_extraction for: ' + str(ticker) + ' from ' + str(x_time), 'log.txt')

            except IndexError:
                write2file.write(str(datetime.datetime.now())[:19] +
                                 ' action.py --> events_extraction --> IndexError: empty 1m_candles_list '
                                 + str(ticker), 'log.txt')
                continue
            # запускаем цикл теста от заданной даты в сторону возрастания, в качестве шага каждая минутная свеча
            for index_row1m in range(index, 0, -1):
                analyze_candles(figi, True, x_time, 'candles_extraction')
                x_time = figi_1m_candles[index_row1m][7]
        except Exception as e:
            write2file.write(str(datetime.datetime.now())[:19] +
                             ' action.py --> events_extraction --> Exception: ' + str(e), 'log.txt')
            continue
    write2file.write(str(datetime.datetime.now())[:19] +
                     ' events_extraction DONE  time spent: ' +
                     str(datetime.datetime.now() - date_start_events_extraction), 'log.txt')
    data2sql.update_pid('events_extraction', 0)
    return
