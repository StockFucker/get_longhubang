#!/usr/bin/python2
# coding:utf8

def is_trade_day(date):
    # date 日期，日期格式 20160101
    # 是交易日 返回1 否则返回0
    trade_days = []
    with open('trddate.txt') as f:
        for i in f:
            if i not in trade_days:
                trade_days.append(i.strip())
    if date in trade_days:
        return 1
    else:
        return 0
    
def get_trade_days():
    trade_days = []
    with open('trddate.txt') as f:
        for i in f:
            if i not in trade_days:
                trade_days.append(i.strip())
    days = trade_days
    days = [day[:4] + "-" + day[4:6] + "-" + day[6:] for day in days]
    days = [day for day in days if day < "2017-01-30"]
    return days

if __name__ == '__main__':
    # print is_trade_day('20170206')
    days = get_trade_days()
    print(days)
