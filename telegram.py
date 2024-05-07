import get_token_file
import telebot
import os
import data2sql
import write2file
import datetime
import sql2data

bot_token = get_token_file.get_token('telegram_bot_token.txt').split()[0]

bot = telebot.TeleBot(bot_token)

# вписываем в таблицу "pid" pid процесса
pid = os.getpid()
data2sql.update_pid('telegram', pid)
write2file.write(str(datetime.datetime.now())[:19] + ' START telegram', 'log.txt')


def analyzed_share_string(ticker):
    analyzed_share = sql2data.analyzed_share_by_ticker(ticker)
    share_string = str(ticker + '\n'
                       + 'profit ' + str(round(analyzed_share[0][2], 3)) + '\n'
                       + 'start_time ' + str(analyzed_share[0][3]) + '\n'
                       + 'start_direction ' + str(analyzed_share[0][4]) + '\n'
                       + 'start_case ' + str(analyzed_share[0][5]) + '\n'
                       + 'start_price ' + str(analyzed_share[0][6]) + '\n'
                       + 'price ' + str(analyzed_share[0][7]) + '\n'
                       + 'target_price ' + str(analyzed_share[0][8]) + '\n'
                       + 'loss_price ' + str(analyzed_share[0][9]) + '\n'
                       + 'prev_position_hours ' + str(round(analyzed_share[0][12], 3)) + '\n'
                       + 'prev_position_days ' + str(round(analyzed_share[0][13], 3)) + '\n'
                       + 'buy ' + str(analyzed_share[0][14]) + '\n'
                       + 'fast_buy ' + str(analyzed_share[0][15]) + '\n'
                       + 'fast_sell ' + str(analyzed_share[0][16]) + '\n'
                       + 'vol ' + str(analyzed_share[0][17]) + '\n'
                       + 'req_vol ' + str(analyzed_share[0][18]))
    return share_string


# Обработчик входящих сообщений
@bot.message_handler(func=lambda message: True)
def handle_message(message):
    # Получаем текст сообщения
    user_message = message.text
    user_id = message.from_user.id
    # если сообщения от нужного пользователя
    if user_id == 1138331624:
        feedback = 'Ошибка ввода.\nДоступные команды:\n\n1.Переписать значение в analyzed_shares:\n' \
                       'rec [ticker] [column] [value]\n\n2.Получить данные по акции из analyzed_shares:\nget [ticker]'
        try:
            input_columns = ['buy', 'fast_buy', 'sell', 'req_vol']
            if user_message.split(' ')[0] == 'rec':
                ticker = user_message.split(' ')[1]
                column_name = user_message.split(' ')[2]
                value = user_message.split(' ')[3]
                tickers = sql2data.analyzed_share_tickers_list()
                if (ticker,) not in tickers:
                    feedback = 'Нет такой акции в таблице analyzed_share'
                elif column_name not in input_columns:
                    feedback = 'Менять значение можно только в колонках:\nbuy, fast_buy, sell, req_vol'
                elif not value.isdigit():
                    feedback = 'Значение должно быть целым числом (INT)'
                else:
                    data2sql.update_analyzed_shares_column(ticker, column_name, int(value))
                    feedback = analyzed_share_string(ticker)

            elif user_message.split(' ')[0] == 'get':
                ticker = user_message.split(' ')[1]
                tickers = sql2data.analyzed_share_tickers_list()
                if (ticker,) not in tickers:
                    feedback = 'нет такой акции в таблице analyzed_share'
                else:
                    feedback = analyzed_share_string(ticker)
        except Exception:
            feedback = 'Ошибка ввода.\nДоступные команды:\n\n1.Переписать значение в analyzed_shares:\n' \
                       'rec [ticker] [column] [value]\n\n2.Получить данные по акции из analyzed_shares:\nget [ticker]'

        bot.send_message(message.chat.id, feedback)


# Запуск бота
bot.polling()
