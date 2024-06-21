import get_token_file
import tinkoff_requests
import telebot
import os
import data2sql
import write2file
import datetime
import sql2data
import time
from tinkoff.invest.utils import now
import matplotlib.pyplot as plt
import pandas as pd
from io import BytesIO

bot_token = get_token_file.get_token('telegram_bot_token.txt').split()[0]

bot = telebot.TeleBot(bot_token)

# вписываем в таблицу "pid" pid процесса
pid = os.getpid()
data2sql.update_pid('telegram', pid)


def readlog(name, num_lines):
    lines = write2file.read(name, num_lines)
    message = ''
    for line in lines:
        message = message + '\n' + line
    return message


def graphs_to_telegram(figi, limit):
    # извлекаем свечки по указанному фиги
    # candles = sql2data.candles_to_finta(figi, now(), 'candles')
    candles = sql2data.candles_to_graph(figi, now(), limit)

    op = []
    hi = []
    lo = []
    cl = []
    vo = []
    list_dates = []
    indexes = []
    i = 0
    for row in candles:
        op.append(float(row[0]))
        hi.append(float(row[1]))
        lo.append(float(row[2]))
        cl.append(float(row[3]))
        vo.append(float(row[4]))
        i += 1
        list_dates.append(str(row[5])[:19])
        indexes.append(i)

    # формируем из двух списков словарь, в качестве ключей округлённые до часов даты, значения - индексы
    candle_dates = dict(zip(list_dates, indexes))

    date_from = candles[0][5]
    list_lot_trades_from_date = sql2data.lot_trades_from_date(figi, date_from)
    list_events_from_date = sql2data.events_from_date(figi, date_from)

    stock_prices = pd.DataFrame({'open': op,
                                 'close': cl,
                                 'high': hi,
                                 'low': lo}, index=pd.Series(indexes))

    plt.figure()

    up = stock_prices[stock_prices.close >= stock_prices.open]
    down = stock_prices[stock_prices.close < stock_prices.open]
    col1 = 'red'
    col2 = 'green'
    width = .5
    width2 = .1

    # Plotting up prices of the stock
    plt.bar(up.index, up.close - up.open, width, bottom=up.open, color=col1)
    plt.bar(up.index, up.high - up.close, width2, bottom=up.close, color=col1)
    plt.bar(up.index, up.low - up.open, width2, bottom=up.open, color=col1)

    # Plotting down prices of the stock
    plt.bar(down.index, down.close - down.open, width, bottom=down.open, color=col2)
    plt.bar(down.index, down.high - down.open, width2, bottom=down.open, color=col2)
    plt.bar(down.index, down.low - down.close, width2, bottom=down.close, color=col2)

    for trade in list_lot_trades_from_date:
        case = trade[1]
        direction = trade[3]
        price = trade[4]
        date = str(tinkoff_requests.adapt_date4interval(trade[5], 4))[:19]
        try:
            plt.scatter(candle_dates[date], price, color='red' if direction == 'SELL' else 'green', marker='o')
            plt.text(candle_dates[date], price, case, verticalalignment='bottom', horizontalalignment='right',
                     fontsize=8)
        except KeyError:
            pass

    for event in list_events_from_date:
        case = event[1]
        direction = event[3]
        price = event[4]
        date = str(tinkoff_requests.adapt_date4interval(event[10], 4))[:19]
        try:
            plt.scatter(candle_dates[date], price, color='red' if direction == 'SELL' else 'green', marker='o')
            plt.text(candle_dates[date], price, case, verticalalignment='bottom', horizontalalignment='right',
                     fontsize=8)
        except KeyError:
            pass
    # rotating the x-axis tick labels at 30degree
    # towards right
    # period = len(indexes) / 6
    # n = 0
    # for n in range(0,5):

    plt.xticks(rotation=30, ha='right')
    # plt.xticks (ticks=, labels=list_dates)
    # plt.xticks(pd.Series(list_dates), rotation=30, ha='right')

    # Сохранение графика в буфере
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)

    user_id = 1138331624
    bot.send_photo(chat_id=user_id, photo=buffer)

    plt.close()
    return


def analyzed_share_string(ticker):
    analyzed_share = sql2data.analyzed_share_by_ticker(ticker)
    share_string = str(ticker + '\n'
                       + 'profit ' + str(round(analyzed_share[0][2], 3)) + '\n'
                       + 'start at\n' + str(analyzed_share[0][3]) + '\n'
                       + str(analyzed_share[0][4]) + str(analyzed_share[0][5]) + '\n'
                       + 'start_price ' + str(analyzed_share[0][6]) + '\n'
                       + 'price ' + str(analyzed_share[0][7]) + '\n'
                       + 'target_price ' + str(analyzed_share[0][8]) + '\n'
                       + 'loss_price ' + str(analyzed_share[0][9]) + '\n'
                       + 'position_hours ' + str(round(analyzed_share[0][12], 3)) + '\n'
                       + 'position_days ' + str(round(analyzed_share[0][13], 3)) + '\n'
                       + 'buy ' + str(analyzed_share[0][14]) + '\n'
                       + 'fast_buy ' + str(analyzed_share[0][15]) + '\n'
                       + 'sell ' + str(analyzed_share[0][16]) + '\n'
                       + 'vol ' + str(analyzed_share[0][17]) + '\n'
                       + 'req_vol ' + str(analyzed_share[0][18]))
    return share_string


def last_events_string(row_count):
    events_string = ''
    for row_index in range(0, row_count):
        events = sql2data.get_last_events_row_cunt('events_list', row_count)
        ticker = events[row_index][0]
        case = events[row_index][1]
        direction = events[row_index][3]
        price = events[row_index][4]
        position_days = events[row_index][5]
        profit = events[row_index][6]
        deal_qnt = events[row_index][7]
        trend_near = events[row_index][8]
        trend_far = events[row_index][9]
        case_time = events[row_index][10]
        event_string = (ticker + '\n' +
                        direction + ' ' + case +
                        '\nprice ' + str(price) +
                        '\nposition_days ' + str(position_days) +
                        '\nprofit ' + str(profit) +
                        '\ndeal_qnt ' + str(deal_qnt) +
                        '\ntrend_near ' + str(trend_near) +
                        '\ntrend_far ' + str(trend_far) +
                        '\n' + str(case_time))
        events_string = event_string + '\n\n' + events_string
    return events_string


def last_orders_string(row_count):
    orders_string = ''
    for row_index in range(0, row_count):
        orders = sql2data.get_last_orders_row_cunt(row_count)

        order_status = orders[row_index][1]
        ticker = orders[row_index][2]
        direction = orders[row_index][3]
        order_case = orders[row_index][4]
        order_price = orders[row_index][5]
        order_qnt = orders[row_index][6]
        order_time = orders[row_index][7]

        order_string = (ticker + '     status ' + str(order_status) +
                        '\n' + direction + ' ' + order_case +
                        '\nprice ' + str(order_price) + '    qnt ' + str(order_qnt) +
                        '\n' + str(order_time))
        orders_string = order_string + '\n\n' + orders_string
    return orders_string


def last_event_ticker_string(ticker):
    event = sql2data.get_last_event_ticker('events_list', ticker)
    ticker = event[0][0]
    case = event[0][1]
    direction = event[0][3]
    price = event[0][4]
    position_days = event[0][5]
    profit = event[0][6]
    deal_qnt = event[0][7]
    trend_near = event[0][8]
    trend_far = event[0][9]
    case_time = event[0][10]
    event_string = (ticker + '\n' +
                    direction + ' ' + case +
                    '\nprice ' + str(price) +
                    '\nposition_days ' + str(position_days) +
                    '\nprofit ' + str(profit) +
                    '\ndeal_qnt ' + str(deal_qnt) +
                    '\ntrend_near ' + str(trend_near) +
                    '\ntrend_far ' + str(trend_far) +
                    '\n' + str(case_time))
    return event_string


def start_telegram_connection():
    try:
        write2file.write(str(datetime.datetime.now())[:19] + ' START telegram', 'log.txt')
        bot.polling()
    except Exception as e:
        # write2file.write(str(datetime.datetime.now())[:19] +
        #                 ' telegram_connection --> Exception: ' + str(e),
        #                 'log.txt')
        time.sleep(20)
        start_telegram_connection()


# Обработчик входящих сообщений
@bot.message_handler(func=lambda message: True)
def handle_message(message):
    # Получаем текст сообщения
    user_message = message.text
    user_id = message.from_user.id
    # если сообщения от нужного пользователя
    if user_id == 1138331624:
        notice = 'Ошибка ввода.\nДоступные команды:' \
                 '\n\n1.Переписать значение в analyzed_shares:\nupdate [ticker] [buy] [fast_buy] [sell] [vol] [req_vol]' \
                 '\n\n2.Получить данные по акции из analyzed_shares:\nget [ticker]' \
                 '\n\n3.Получить последние строки из events_list:\nevents [row_count]' \
                 '\n\n4.Получить последние строки из orders:\norders [row_count]' \
                 '\n\n5.Получить последние строки из log.txt:\nlog [num_lines]' \
                 '\n\n6.Получить из events_list последний event по акции:\nlast-event [ticker]' \
                 '\n\n7.Получить график свечей по акции, и за количество часов:\ngraph [ticker] [limit]'
        feedback = notice
        try:
            if user_message.split(' ')[0] == 'update':
                ticker = user_message.split(' ')[1]
                buy = user_message.split(' ')[2]
                fast_buy = user_message.split(' ')[3]
                sell = user_message.split(' ')[4]
                vol = user_message.split(' ')[5]
                req_vol = user_message.split(' ')[6]
                tickers = sql2data.analyzed_share_tickers_list()
                if (ticker,) not in tickers:
                    feedback = 'Ошибка ввода. Нет такой акции в таблице analyzed_share'
                elif not vol.isdigit() or not req_vol.isdigit():
                    feedback = 'Ошибка ввода. vol и req_vol целые числа (INT)'
                else:
                    data2sql.update_analyzed_shares_from_telegram(buy, fast_buy, sell, vol, req_vol, ticker)
                    feedback = analyzed_share_string(ticker)

            if user_message.split(' ')[0] == 'get':
                ticker = user_message.split(' ')[1]
                tickers = sql2data.analyzed_share_tickers_list()
                if (ticker,) not in tickers:
                    feedback = 'Ошибка ввода. Нет такой акции в таблице analyzed_share'
                else:
                    feedback = analyzed_share_string(ticker)

            if user_message.split(' ')[0] == 'events':
                row_count = user_message.split(' ')[1]
                if not row_count.isdigit():
                    feedback = 'Ошибка ввода. row_count целое число (INT)'
                else:
                    feedback = last_events_string(int(row_count))

            if user_message.split(' ')[0] == 'orders':
                row_count = user_message.split(' ')[1]
                if not row_count.isdigit():
                    feedback = 'Ошибка ввода. row_count целое число (INT)'
                else:
                    feedback = last_orders_string(int(row_count))

            if user_message.split(' ')[0] == 'last-event':
                ticker = user_message.split(' ')[1]
                feedback = last_event_ticker_string(ticker)

            if user_message.split(' ')[0] == 'log':
                num_lines = user_message.split(' ')[1]
                if not num_lines.isdigit():
                    feedback = 'Ошибка ввода. num_lines целое число (INT)'
                else:
                    feedback = readlog('log.txt', num_lines)

            if user_message.split(' ')[0] == 'graph':
                ticker = user_message.split(' ')[1]
                tickers = sql2data.analyzed_share_tickers_list()
                limit = user_message.split(' ')[2]
                if (ticker,) not in tickers or not limit.isdigit():
                    feedback = "неверная акция или лимит"
                else:
                    figi = sql2data.get_info_by_ticker('shares', 'figi', ticker)[0][0]
                    graphs_to_telegram(figi, int(limit))
                    feedback = analyzed_share_string(ticker)

        except Exception:
            feedback = notice

        bot.send_message(message.chat.id, feedback)


# Запуск телеграм соединения
start_telegram_connection()
