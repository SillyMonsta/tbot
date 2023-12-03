import time
from decimal import Decimal
import datetime
import data2sql
import sql2data
import action
import get_token_file
from tinkoff.invest.exceptions import InvestError
from tinkoff.invest.utils import now, decimal_to_quotation
from tinkoff.invest import (
    CandleInterval,
    Client,
    OrderType,
    OperationState,
    TradeInstrument,
    MarketDataRequest,
    SubscribeTradesRequest,
    SubscriptionAction
)

acc_name = 'Брокерский счёт'
TOKEN = get_token_file.get_token('token.txt').split()[0]


def request_account_id():
    with Client(TOKEN) as client:
        try:
            acc_id = client.users.get_accounts().accounts[0].id
            acc_import_name = client.users.get_accounts().accounts[0].name
            data2sql.account_id2sql(acc_id, acc_import_name)
        except InvestError as error:
            print(error)


# проверяем есть ли в базе данных таблица acc_id если нет, то создаем, запрашиваем и заполняем
if sql2data.is_table_exist('acc_id') is False:
    sql2data.create__acc_id()
    request_account_id()

account_id = sql2data.acc_id_from_sql(acc_name)


def adapt_date4interval(format_date, interval):
    if interval == 1:
        rounded_time = format_date.replace(second=0, microsecond=0)
    elif interval == 4:
        rounded_time = format_date.replace(minute=0, second=0, microsecond=0)
    elif interval == 5:
        rounded_time = format_date.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        rounded_time = format_date
    return rounded_time


def units_nano_merge(units, nano):
    return units + (nano / 1000000000)


def request_shares():
    print('request shares', str(datetime.datetime.now())[:19])
    shares_list = []
    with Client(TOKEN) as client:
        try:
            all_shares = client.instruments.shares(instrument_status=1)
            for share in all_shares.instruments:
                if share.api_trade_available_flag and share.for_qual_investor_flag is False:
                    one_row = (
                        share.figi,
                        share.ticker,
                        share.lot,
                        share.currency,
                        share.name,
                        share.exchange,
                        share.sector,
                        share.trading_status,
                        units_nano_merge(share.min_price_increment.units, share.min_price_increment.nano),
                        share.uid
                    )
                    shares_list.append(one_row)
            data2sql.shares2sql(shares_list)
        except InvestError as error:
            print(error)
    return


def request_history_candles(figi_list, history_candle_days, start_range, table_name):
    print('request history_candles', str(datetime.datetime.now())[:19])
    interval_list = [1, 4, 5]
    history_candle_intervals = [CandleInterval.CANDLE_INTERVAL_1_MIN,
                                CandleInterval.CANDLE_INTERVAL_HOUR,
                                CandleInterval.CANDLE_INTERVAL_DAY]
    for figi in figi_list:
        # количество дней и интервалы берем из списков history_candle_intervals и history_candle_days
        try:
            for x in range(start_range, 3):
                candles_list = []
                with Client(TOKEN) as client:
                    for candle in client.get_all_candles(
                            figi=figi,
                            from_=now() - datetime.timedelta(days=history_candle_days[x]),
                            interval=history_candle_intervals[x],
                    ):
                        one_row = (
                            figi,
                            history_candle_intervals[x],
                            units_nano_merge(candle.open.units, candle.open.nano),
                            units_nano_merge(candle.high.units, candle.high.nano),
                            units_nano_merge(candle.low.units, candle.low.nano),
                            units_nano_merge(candle.close.units, candle.close.nano),
                            candle.volume,
                            adapt_date4interval(candle.time, interval_list[x])
                        )
                        candles_list.append(one_row)
                data2sql.history_candles2sql(table_name, candles_list)
        except InvestError as error:
            print('request history_candles', error)
    return


def request_balance():
    money_list = []
    securities_list = []
    with Client(TOKEN) as client:
        try:
            positions = client.operations.get_positions(account_id=account_id)
            for m in positions.money:
                one_row_m = (m.currency, units_nano_merge(m.units, m.nano))
                money_list.append(one_row_m)
            for s in positions.securities:
                one_row_s = (s.figi, s.balance)
                securities_list.append(one_row_s)
            data2sql.securities2sql('balances', money_list + securities_list)
        except InvestError as error:
            print(error)
    return


def limit_order(figi, direction, quantity, order_price):
    order_id = str(time.time())
    with Client(TOKEN) as client:
        try:
            response = client.orders.post_order(
                figi=figi,
                quantity=quantity,
                price=decimal_to_quotation(Decimal(order_price)),  # !!!цена за лот!!!
                direction=direction,
                account_id=account_id,
                order_type=OrderType.ORDER_TYPE_LIMIT,
                order_id=order_id
            )
            print(response)
            print("order_id=", response.order_id)
        except InvestError as error:
            print(error)
    return order_id


def cancel(order_id):
    with Client(TOKEN) as client:
        try:
            response = client.orders.cancel_order(
                account_id=account_id,
                order_id=order_id
            )
            print(response)
        except InvestError as error:
            print(error)
    return


def operation_executed():
    operation_list = []
    with Client(TOKEN) as client:
        try:
            operations = client.operations.get_operations(
                account_id=account_id,
                state=OperationState.OPERATION_STATE_EXECUTED,
            )
            for op in operations.operations:
                if op.type == 'Пополнение брокерского счёта':
                    one_row = (
                        op.id, 'rub', 'cash_in', units_nano_merge(op.payment.units, op.payment.nano), op.quantity,
                        op.date)
                    operation_list.append(one_row)
                elif op.type == 'Вывод денежных средств':
                    one_row = (
                        op.id, 'rub', 'cash_out', units_nano_merge(op.payment.units, op.payment.nano), op.quantity,
                        op.date)
                    operation_list.append(one_row)
                elif op.type == 'Покупка ценных бумаг':
                    one_row = (
                        op.id, op.figi, 'buy', units_nano_merge(op.price.units, op.price.nano), op.quantity, op.date)
                    operation_list.append(one_row)
                elif op.type == 'Продажа ценных бумаг':
                    one_row = (
                        op.id, op.figi, 'sell', units_nano_merge(op.price.units, op.price.nano), op.quantity, op.date)
                    operation_list.append(one_row)
            data2sql.operation_executed2sql(operation_list)
        except InvestError as error:
            print(error)
    return


def operation_in_progress():
    operation_list = []
    with Client(TOKEN) as client:
        try:
            operations = client.operations.get_operations(
                account_id=account_id,
                state=OperationState.OPERATION_STATE_PROGRESS,
            )
            for op in operations.operations:
                if op.type == 'Покупка ценных бумаг':
                    one_row = (
                        op.id, op.figi, 'buy', units_nano_merge(op.price.units, op.price.nano), op.quantity, op.date)
                    operation_list.append(one_row)
                if op.type == 'Продажа ценных бумаг':
                    one_row = (
                        op.id, op.figi, 'sell', units_nano_merge(op.price.units, op.price.nano), op.quantity, op.date)
                    operation_list.append(one_row)
            data2sql.operation_in_progress2sql(operation_list)
        except InvestError as error:
            print(error)
    return


def get_order_request():
    money_list = []
    securities_list = []
    with Client(TOKEN) as client:
        try:
            orders = client.orders.get_orders(account_id=account_id)

        except InvestError as error:
            print(error)
    return orders.orders


def stream_connection(figi_list):
    def request_iterator():
        for figi in figi_list:
            yield MarketDataRequest(
                subscribe_trades_request=SubscribeTradesRequest(
                    subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                    instruments=[TradeInstrument(figi=figi, )], ))
        while True:
            time.sleep(1)

    xtime = now()
    trades2candles_list = []
    with Client(TOKEN) as client:
        try:

            for marketdata in client.market_data_stream.market_data_stream(request_iterator()):
                if marketdata.trade is not None:
                    price = units_nano_merge(marketdata.trade.price.units, marketdata.trade.price.nano)
                    # формируем данные для запроса sql по обновлению свечек для двух интервалов
                    for x in [4, 5]:
                        one_row = (
                            marketdata.trade.figi,  # figi
                            x,  # interval
                            price,  # price в качестве open
                            price,  # price в качестве high
                            price,  # price в качестве low
                            price,  # price в качестве close
                            marketdata.trade.quantity,  # quantity в качестве volume
                            adapt_date4interval(marketdata.trade.time, x),  # rounded time
                            marketdata.trade.quantity,  # quantity для добавления к volume
                        )
                        trades2candles_list.append(one_row)
                    # если с предыдущей отправки прошло 3 секунды
                    if xtime + datetime.timedelta(seconds=3) < now():
                        # отправляем список трейдо-свечки, по которым была торговля за этот период
                        data2sql.candles2sql(trades2candles_list)
                        # создаём временный список фиги
                        temp_figi_list = [] + figi_list
                        # создаём список фиг по которым были сделки за этот период, чтобы одни и те-же фиги не повторялись
                        for row_trade in trades2candles_list:
                            figi_to_analyze = row_trade[0]
                            if figi_to_analyze in temp_figi_list:
                                temp_figi_list.remove(figi_to_analyze)
                                # анализируем свечки с помощью индикаторов, ищем удачные моменты для торговли
                                action.analyze_candles(figi_to_analyze, False, now(), 'candles')
                        # обнуляем время и список для начала нового периода
                        xtime = now()
                        trades2candles_list = []

        except Exception as e:
            print('stream_connection error:', e, str(datetime.datetime.now())[:19])
            figi_list = action.prepare_stream_connection()
            time.sleep(10)
            stream_connection(figi_list)
