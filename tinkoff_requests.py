import time
from decimal import Decimal
import datetime
import data2sql
import sql2data
import action
import get_token_file
import write2file
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
            write2file.write(
                str(datetime.datetime.now())[:19] + ' tinkoff_requests.py --> request_account_id --> InvestError: '
                + str(error), 'log.txt')


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
                        units_nano_merge(share.min_price_increment.units, share.min_price_increment.nano),
                        share.uid,
                    )
                    shares_list.append(one_row)
            data2sql.shares2sql(shares_list)
        except InvestError as error:
            write2file.write(
                str(datetime.datetime.now())[:19] + ' tinkoff_requests.py --> request_shares --> InvestError: '
                + str(error), 'log.txt')
    return


def request_trading_status(figi):
    with Client(TOKEN) as client:
        try:
            trading_status = client.instruments.share_by(id_type=1, id=figi).instrument.trading_status.value
        except InvestError as error:
            trading_status = 0
            write2file.write(str(datetime.datetime.now())[:19] + ' tinkoff_requests.py --> request_trading_status '
                             + str(error), 'log.txt')
    return trading_status


def request_history_candles(figi_list, history_candle_days, start_range, table_name):
    write2file.write(str(datetime.datetime.now())[:19] + ' request history_candles for ' + table_name, 'log.txt')
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
            write2file.write(
                str(datetime.datetime.now())[:19] + ' tinkoff_requests.py --> history_candles --> InvestError: '
                + str(error), 'log.txt')
    return


def request_balance():
    with Client(TOKEN) as client:
        try:
            positions = client.operations.get_positions(account_id=account_id)
            money_list = []
            for m in positions.money:
                one_row_m = (m.currency, units_nano_merge(m.units, m.nano))
                money_list.append(one_row_m)
            else:
                data2sql.balance2sql('balance', money_list)

            for s in positions.securities:
                data2sql.update_analyzed_shares_column_by_figi(s.figi, 'vol', s.balance)

        except InvestError as error:
            write2file.write(
                str(datetime.datetime.now())[:19] + ' tinkoff_requests.py --> request_balance --> InvestError: '
                + str(error), 'log.txt')
    return


def get_order_status(order_id):
    with Client(TOKEN) as client:
        try:
            response = client.orders.get_order_state(
                account_id=account_id,
                order_id=order_id,
                order_type=OrderType.ORDER_TYPE_MARKET
            )
            status = response.execution_report_status.value
        except InvestError as error:
            write2file.write(
                str(datetime.datetime.now())[:19] + ' tinkoff_requests.py --> limit_order --> InvestError: '
                + str(error), 'log.txt')
    return status


def market_order(figi, direction, quantity):
    order_id = str(time.time())
    with Client(TOKEN) as client:
        try:
            response = client.orders.post_order(
                figi=figi,
                quantity=quantity,
                direction=direction,
                account_id=account_id,
                order_type=OrderType.ORDER_TYPE_MARKET,
                order_id=order_id
            )
            status = response.execution_report_status.value
            order_id = response.order_id
            write2file.write(str(datetime.datetime.now())[:19] + ' tinkoff_requests.py --> market_order DONE: order_id='
                             + str(response.order_id), 'log.txt')
        except InvestError as error:
            status = None
            write2file.write(
                str(datetime.datetime.now())[:19] + ' tinkoff_requests.py --> market_order --> InvestError: '
                + str(error), 'log.txt')
    return order_id, status


def limit_order(figi, direction, quantity, order_price):
    order_id = str(time.time())
    with Client(TOKEN) as client:
        try:
            response = client.orders.post_order(
                figi=figi,
                quantity=quantity,
                price=decimal_to_quotation(Decimal(order_price)),  # !!!цена за лот (order_price = last_price * lot)!!!
                direction=direction,
                account_id=account_id,
                order_type=OrderType.ORDER_TYPE_LIMIT,
                order_id=order_id
            )
            status = response.execution_report_status.value
            order_id = response.order_id
            write2file.write(str(datetime.datetime.now())[:19] + ' tinkoff_requests.py --> limit_order DONE: order_id='
                             + str(response.order_id), 'log.txt')
        except InvestError as error:
            write2file.write(
                str(datetime.datetime.now())[:19] + ' tinkoff_requests.py --> limit_order --> InvestError: '
                + str(error), 'log.txt')
    return order_id, status


def cancel(order_id):
    with Client(TOKEN) as client:
        try:
            response = client.orders.cancel_order(
                account_id=account_id,
                order_id=order_id
            )
            write2file.write(str(datetime.datetime.now())[:19] + ' tinkoff_requests.py --> cancel DONE: '
                             + str(response), 'log.txt')
        except InvestError as error:
            write2file.write(str(datetime.datetime.now())[:19] + ' tinkoff_requests.py --> cancel --> InvestError: '
                             + str(error), 'log.txt')
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
            write2file.write(
                str(datetime.datetime.now())[:19] + ' tinkoff_requests.py --> operation_executed --> InvestError: '
                + str(error), 'log.txt')
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
            write2file.write(str(datetime.datetime.now())[:19] +
                             ' tinkoff_requests.py --> operation_in_progress --> InvestError: '
                             + str(error), 'log.txt')
    return


def get_orders_request():
    with Client(TOKEN) as client:
        try:
            orders = client.orders.get_orders(account_id=account_id)
        except InvestError as error:
            write2file.write(str(datetime.datetime.now())[:19] +
                             ' tinkoff_requests.py --> get_orders_request --> InvestError: '
                             + str(error), 'log.txt')
    return orders.orders


def stream_connection(figi_list):
    write2file.write(str(datetime.datetime.now())[:19] + ' START stream_connection', 'log.txt')

    def request_iterator():
        yield MarketDataRequest(
            subscribe_trades_request=SubscribeTradesRequest(
                subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                instruments=[TradeInstrument(figi=figi, ) for figi in figi_list]))
        while True:
            time.sleep(1)

    xtime = now()
    trades2candles_list = []
    with Client(TOKEN) as client:
        try:
            for marketdata in client.market_data_stream.market_data_stream(request_iterator()):
                if marketdata.trade:
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
                    # если с предыдущей отправки прошло 5 секунды
                    if xtime + datetime.timedelta(seconds=5) < now():
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
            write2file.write(str(datetime.datetime.now())[:19] +
                             ' tinkoff_requests.py --> stream_connection --> Exception: '
                             + str(e), 'log.txt')
            # формируем список всех акций
            shares = sql2data.shares_from_sql()
            figi_list = []
            for figi_row in shares:
                figi_list.append(figi_row[0])
            time.sleep(10)
            stream_connection(figi_list)
