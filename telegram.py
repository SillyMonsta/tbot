import get_token_file
import telebot
import os
import data2sql
import write2file
import datetime

bot_token = get_token_file.get_token('telegram_bot_token.txt').split()[0]

bot = telebot.TeleBot(bot_token)

# вписываем в таблицу "pid" pid процесса
pid = os.getpid()
data2sql.update_pid('telegram', pid)
write2file.write(str(datetime.datetime.now())[:19] + ' START telegram', 'log.txt')


# Обработчик входящих сообщений
@bot.message_handler(func=lambda message: True)
def handle_message(message):
    # Получаем текст сообщения
    user_message = message.text
    user_id = message.from_user.id
    # если сообщения от нужного пользователя
    if user_id == 1138331624:
        # Отправляем ответное сообщение пользователю
        bot.send_message(message.chat.id, f"Вы написали: {user_message}")


# Запуск бота
bot.polling()
