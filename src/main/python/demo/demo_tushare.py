import tushare as ts


def getdata(token):

    pro = ts.pro_api()
    df = pro.daily(trade_date='20200325')