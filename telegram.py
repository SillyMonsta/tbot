import get_token_file

import telebot

bot_token = get_token_file.get_token('telegram_bot_token.txt').split()[0]

bot = telebot.TeleBot(bot_token)


# Обработчик для команды /start
@bot.message_handler(commands=['start'])
def handle_start(message):
    bot.send_message(message.chat.id, "Привет! Я бот. Чем могу помочь?")


# Обработчик для всех остальных сообщений
@bot.message_handler(func=lambda message: True)
def handle_message(message):
    # Получаем текст сообщения от пользователя
    user_message = message.text

    # Отправляем ответное сообщение пользователю
    bot.send_message(message.chat.id, f"Вы написали: {user_message}")


# Запуск бота
bot.polling()
