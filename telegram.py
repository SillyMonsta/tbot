import get_token_file
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
from matplotlib.patches import FancyArrowPatch
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


def graphs_to_telegram(figi):
    # извлекаем свечки по указанному фиги
    candles = sql2data.candles_to_finta(figi, now(), 'candles')
    op = []
    hi = []
    lo = []
    cl = []
    vo = []
    candle_dates = []
    for row in candles:
        op.append(float(row[0]))
        hi.append(float(row[1]))
        lo.append(float(row[2]))
        cl.append(float(row[3]))
        vo.append(float(row[4]))
        candle_dates.append(float(row[5]))

    data = {
        'date': candle_dates,
        'open': op,
        'high': hi,
        'low': lo,
        'close': cl,
        'volume': vo
    }

    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])

    # Создаем график свечей
    fig, ax = plt.subplots()
    ax.plot(df['date'], df['high'], linestyle='-', color='black')
    ax.plot(df['date'], df['low'], linestyle='-', color='black')
    ax.vlines(df['date'], df['low'], df['high'], color='black')
    for index, row in df.iterrows():
        if row['close'] > row['open']:
            color = 'green'
        else:
            color = 'red'
        ax.add_patch(FancyArrowPatch((row['date'], row['open']), (row['date'], row['close']),
                                     arrowstyle='->', mutation_scale=15, color=color))

    # Наложение стрелок на точки (предположим, что у нас есть данные точек)
    points = {
        'date': ['2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04'],
        'price': [105, 112, 107, 123]
    }

    df_points = pd.DataFrame(points)
    df_points['date'] = pd.to_datetime(df_points['date'])

    for index, row in df_points.iterrows():
        ax.add_patch(FancyArrowPatch((row['date'], row['price']), (row['date'], row['price']),
                                     arrowstyle='-', mutation_scale=15, color='purple'))

    ax.set_title('OHLCV Candlestick Chart')

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
        time.sleep(120)
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
                 '\n\n3.Получить последние строки из orders:\norders [row_count]' \
                 '\n\n3.Получить последние строки из log.txt:\nlog [num_lines]' \
                 '\n\n4.Получить из events_list последний event по акции:\nlast-event [ticker]'
        feedback = notice
        #try:
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

        elif user_message.split(' ')[0] == 'get':
            ticker = user_message.split(' ')[1]
            tickers = sql2data.analyzed_share_tickers_list()
            if (ticker,) not in tickers:
                feedback = 'Ошибка ввода. Нет такой акции в таблице analyzed_share'
            else:
                feedback = analyzed_share_string(ticker)

        elif user_message.split(' ')[0] == 'events':
            row_count = user_message.split(' ')[1]
            if not row_count.isdigit():
                feedback = 'Ошибка ввода. row_count целое число (INT)'
            else:
                feedback = last_events_string(int(row_count))

        elif user_message.split(' ')[0] == 'orders':
            row_count = user_message.split(' ')[1]
            if not row_count.isdigit():
                feedback = 'Ошибка ввода. row_count целое число (INT)'
            else:
                feedback = last_orders_string(int(row_count))

        elif user_message.split(' ')[0] == 'last-event':
            ticker = user_message.split(' ')[1]
            feedback = last_event_ticker_string(ticker)

        elif user_message.split(' ')[0] == 'log':
            num_lines = user_message.split(' ')[1]
            if not num_lines.isdigit():
                feedback = 'Ошибка ввода. num_lines целое число (INT)'
            else:
                feedback = readlog('log.txt', num_lines)

        elif user_message.split(' ')[0] == 'graph':
            ticker = user_message.split(' ')[1]
            tickers = sql2data.analyzed_share_tickers_list()
            figi = sql2data.get_info_by_ticker('shares', 'figi', ticker)[0][0]
            if (ticker,) not in tickers:
                feedback = 'Ошибка ввода. Нет такой акции в таблице analyzed_share'
            else:
                graphs_to_telegram(figi)
                feedback = analyzed_share_string(ticker)

        #except Exception:
        #    feedback = notice

        bot.send_message(message.chat.id, feedback)


# Запуск телеграм соединения
start_telegram_connection()
