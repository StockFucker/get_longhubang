# __author__ = 'fit'`
# -*- coding: utf-8 -*-
import pandas as pd
import urllib
import requests
import json

url = 'http://data.eastmoney.com/DataCenter_V3/stock2016/jymx.ashx?pagesize=1000&page=0&js=var%20zfJhIYoh&param=&sortRule=-1&sortType=&gpfw=0&code=80100524&rt=25287274'

r=requests.get(url)
data = r.text[4:]
data = data.replace('true','0')
# print(data)
exec(data)
# print zfJhIYoh
# data = json.loads(zfJhIYoh)
print(zfJhIYoh)
df = pd.DataFrame(zfJhIYoh["data"])
# print(df)
df["code"] = df["SCode"].apply(lambda x:x + ".XSHG" if x[0] == "6" else x + ".XSHE")
print(df["code"].head(3))
df.to_csv("jtl.csv")