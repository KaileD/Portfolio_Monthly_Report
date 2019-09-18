import tushare
import pandas
pandas.core.common.is_list_like = pandas.api.types.is_list_like
import datetime
import os
import time
import pandas_datareader as pdr
import yfinance as yf


def other_stock_price_intraday(df, ticker, folder, starting_date, end_date):
    intraday = pdr.data.get_data_yahoo(ticker, start=starting_date, end=end_date)

    file = folder + '/' + ticker + '.csv'
    if os.path.exists(file):
        history = pandas.read_csv(file, index_col=0)
        intraday.append(history)
    # inverse
    intraday.sort_index(inplace=True)
    intraday.index.name = 'timestamp'

    # Save
    intraday.to_csv(file, encoding='utf-8', index=True)

    print('Intraday for [' + ticker + '] got')

    ending_price = intraday.iloc[-1][0]

    new_date = end_date.replace(day=1)
    new_date = new_date - datetime.timedelta(days=1)
    intraday2 = pdr.data.get_data_yahoo(ticker, start=starting_date, end=new_date)

    starting_price = intraday2.iloc[-1][0]
    diff = ending_price - starting_price
    percentage = diff / starting_price

    print(starting_price, ending_price, diff, percentage)
    return [starting_price, ending_price, diff, percentage]


def stock_price_intraday(df, ticker, folder, starting_date, end_date):
    # get intraday online
    intraday = tushare.get_hist_data(ticker, start=starting_date, end=end_date)

    # append if the history exists
    file = folder + '/' + ticker + '.csv'
    if os.path.exists(file):
        history = pandas.read_csv(file, index_col=0)
        intraday.append(history)

    # inverse
    intraday.sort_index(inplace=True)
    intraday.index.name = 'timestamp'

    # Save
    intraday.to_csv(file, encoding='utf-8', index=True)
    # print('Intraday for [' + ticker + '] got')
    starting_price = intraday.loc[starting_date]['close']
    ending_price = intraday.loc[end_date]['close']
    diff = ending_price - starting_price
    percentage = diff/starting_price

    print(starting_price, ending_price, diff, percentage)
    return [starting_price, ending_price, diff, percentage]


def last_trading_day(date_str, date) -> str:
    alldays = tushare.trade_cal()
    tradingdays = alldays[alldays['isOpen'] == 1]  # 开盘日
    if date_str in tradingdays['calendarDate'].values:
        return date_str
    else:
        new_date = date - datetime.timedelta(days=1)
        return last_trading_day(new_date.strftime('%Y-%m-%d'), new_date)


yf.pdr_override()  # 修正命令
# tickers = ['300775', '002957', '688005', '688029', '688002']  # TODO: 科创板数据
tickers = ['300773', '300184', '603882', '600981', '300159', '002584', '002308', '002712', '603713', '300413', '300482', '603659', '600511', '002409', '603259', '600704', '603056', '000710', '600715', '601360', '002405', '300750', '300123', '002027']
other_tickers = ['1860.HK', '1801.HK', '0780.HK', '1476.HK', '2269.HK', '1110.HK', '3690.HK', '6100.HK', '1877.HK', 'HUNTF', 'SYRS', 'ROKU', 'BILI', 'LX', 'STG', 'PPDF', 'UXIN', 'CANG', 'AML.L']
df = []

today = datetime.datetime.today()

# get the last trading day of last two months
first = today.replace(day=1)
last_month = first - datetime.timedelta(days=1)
last_month_trading_date = last_trading_day(last_month.strftime('%Y-%m-%d'), last_month)
last_last_month = last_month.replace(day=1)
new_date = last_last_month - datetime.timedelta(days=1)
last_last_month_trading_date = last_trading_day(new_date.strftime('%Y-%m-%d'), new_date)
print("dates got")

for i, ticker in enumerate(tickers):
    try:
        # print('Intraday', i, '/', len(tickers))
        data = stock_price_intraday(df, ticker, folder='~/Desktop/Prosnav/auto_update/Data', starting_date=last_last_month_trading_date, end_date=last_month_trading_date)
        df.append(data)
        time.sleep(2)
    except:
        pass

print("CN Intraday for all stocks got")

# TODO: 对每个市场trading day调整
other_tickers = ['1860.HK', '1801.HK', '0780.HK', '1476.HK', '2269.HK', '1110.HK', '3690.HK', '6100.HK', '1877.HK', 'HUNTF', 'SYRS', 'ROKU', 'BILI', 'LX', 'STG', 'PPDF', 'UXIN', 'CANG', 'AML.L']

new_date = first - datetime.timedelta(days=60)
for i, ticker in enumerate(other_tickers):
    try:
        data = other_stock_price_intraday(df, ticker, folder='~/Desktop/Prosnav/auto_update/Data', starting_date=new_date, end_date=last_month)
        df.append(data)
        time.sleep(2)
    except:
        pass

df = pandas.DataFrame(df)
df.to_csv('~/Desktop/Prosnav/auto_update/result.csv', encoding='utf-8', index=True)
