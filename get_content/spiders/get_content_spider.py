# __author__ = 'fit'`
# -*- coding: utf-8 -*-
from get_content.model import *
from get_content.items import *
from tradeday import *
import urllib
import re
import pymysql
import pandas as pd
import scrapy


class get_content_spider(scrapy.Spider):
    name = "get_content"
    allowed_domains = ["eastmoney.com"]

    # 获取数据表中最晚的那天的日期
    def get_lastest_date(self):
        return "2018-01-29"
        con = pymysql.connect(host="120.77.149.105", port=3306, user="root", passwd="1991311", charset="utf8",
                              db="mysql")
        cur = con.cursor()
        sql = "select date from mysql order by date desc limit 0,1"
        cur.execute(sql)
        result = cur.fetchone()
        cur.close()
        con.close()
        if result is None or len(result) == 0:
            print 'maybe table longhubang is wrong!'
            exit(-1)
        return result[0]

    def build_url_by_loss_date(self):
        # date_list = ['2017-01-03', '2017-01-04', '2017-01-05', '2017-01-06', '2017-01-09', '2017-01-10', '2017-01-11', '2017-01-12', '2017-01-13', '2017-01-16', '2017-01-17', '2017-01-18', '2017-01-19', '2017-01-20', '2017-01-23', '2017-01-24', '2017-01-25', '2017-01-26', '2017-02-03', '2017-02-06', '2017-02-07', '2017-02-08', '2017-02-09', '2017-02-10', '2017-02-13', '2017-02-14', '2017-02-15', '2017-02-16', '2017-02-17', '2017-02-20', '2017-02-21', '2017-02-22', '2017-02-23', '2017-02-24', '2017-02-27', '2017-02-28', '2017-03-01', '2017-03-02', '2017-03-03', '2017-03-06', '2017-03-07', '2017-03-08', '2017-03-09', '2017-03-10', '2017-03-13', '2017-03-14', '2017-03-15', '2017-03-16', '2017-03-17', '2017-03-20', '2017-03-21', '2017-03-22', '2017-03-23', '2017-03-24', '2017-03-27', '2017-03-28', '2017-03-29', '2017-03-30', '2017-03-31', '2017-04-05', '2017-04-06', '2017-04-07', '2017-04-10', '2017-04-11', '2017-04-12', '2017-04-13', '2017-04-14', '2017-04-17', '2017-04-18', '2017-04-19', '2017-04-20', '2017-04-21', '2017-04-24', '2017-04-25', '2017-04-26', '2017-04-27', '2017-04-28', '2017-05-02', '2017-05-03', '2017-05-04', '2017-05-05', '2017-05-08', '2017-05-09', '2017-05-10', '2017-05-11', '2017-05-12', '2017-05-15', '2017-05-16', '2017-05-17', '2017-05-18', '2017-05-19', '2017-05-22', '2017-05-23', '2017-05-24', '2017-05-25', '2017-05-26', '2017-05-31', '2017-06-01', '2017-06-02', '2017-06-05', '2017-06-06', '2017-06-07', '2017-06-08', '2017-06-09', '2017-06-12', '2017-06-13', '2017-06-14', '2017-06-15', '2017-06-16', '2017-06-19', '2017-06-20', '2017-06-21', '2017-06-22', '2017-06-23', '2017-06-26', '2017-06-27', '2017-06-28', '2017-06-29', '2017-06-30', '2017-07-03', '2017-07-04', '2017-07-05', '2017-07-06', '2017-07-07', '2017-07-10', '2017-07-11', '2017-07-12', '2017-07-13', '2017-07-14', '2017-07-17', '2017-07-18', '2017-07-19', '2017-07-20', '2017-07-21', '2017-07-24', '2017-07-25', '2017-07-26', '2017-07-27', '2017-07-28', '2017-07-31', '2017-08-01', '2017-08-02', '2017-08-03', '2017-08-04', '2017-08-07', '2017-08-08', '2017-08-09', '2017-08-10', '2017-08-11', '2017-08-14', '2017-08-15', '2017-08-16', '2017-08-17', '2017-08-18', '2017-08-21', '2017-08-22', '2017-08-23', '2017-08-24', '2017-08-25', '2017-08-28', '2017-08-29', '2017-08-30', '2017-08-31', '2017-09-01', '2017-09-04', '2017-09-05', '2017-09-06', '2017-09-07', '2017-09-08', '2017-09-11', '2017-09-12', '2017-09-13', '2017-09-14', '2017-09-15', '2017-09-18', '2017-09-19', '2017-09-20', '2017-09-21', '2017-09-22', '2017-09-25', '2017-09-26', '2017-09-27', '2017-09-28', '2017-09-29', '2017-10-09', '2017-10-10', '2017-10-11', '2017-10-12', '2017-10-13', '2017-10-16', '2017-10-17', '2017-10-18', '2017-10-19', '2017-10-20', '2017-10-23', '2017-10-24', '2017-10-25', '2017-10-26', '2017-10-27', '2017-10-30', '2017-10-31', '2017-11-01', '2017-11-02', '2017-11-03', '2017-11-06', '2017-11-07', '2017-11-08', '2017-11-09', '2017-11-10', '2017-11-13', '2017-11-14', '2017-11-15', '2017-11-16', '2017-11-17', '2017-11-20', '2017-11-21', '2017-11-22', '2017-11-23', '2017-11-24', '2017-11-27', '2017-11-28', '2017-11-29', '2017-11-30', '2017-12-01', '2017-12-04', '2017-12-05', '2017-12-06', '2017-12-07', '2017-12-08', '2017-12-11', '2017-12-12', '2017-12-13', '2017-12-14', '2017-12-15', '2017-12-18', '2017-12-19', '2017-12-20', '2017-12-21', '2017-12-22', '2017-12-25', '2017-12-26', '2017-12-27', '2017-12-28', '2017-12-29', '2018-01-01', '2018-01-02', '2018-01-03', '2018-01-04', '2018-01-05', '2018-01-08', '2018-01-09', '2018-01-10', '2018-01-11', '2018-01-12', '2018-01-15', '2018-01-16', '2018-01-17', '2018-01-18', '2018-01-19', '2018-01-22', '2018-01-23', '2018-01-24', '2018-01-25', '2018-01-26', '2018-01-29']
        date_list = ['2017-01-03']
        ret_urls = []
        i = 1.0
        length = len(date_list)
        for date in date_list:
            print "build urls", i / length, "complete!"
            html_url = '''http://data.eastmoney.com/DataCenter_V3/stock2016/TradeDetail/pagesize=200,page=1,sortRule=-1,sortType=,startDate=%s,endDate=%s''' % (
                date, date)
            html_url += ",gpfw=0,js=var%20data_tab_1.html"
            wp = urllib.urlopen(html_url)
            content = wp.read()
            code_list = re.findall(r"SCode\":\"\d\d\d\d\d\d", content)
            code_list = list(set(code_list))
            code_list = map(lambda x: x[-6:], code_list)
            url_list = map(lambda x: '''http://data.eastmoney.com/stock/lhb,%s,%s.html''' % (date, x), code_list)
            ret_urls.extend(url_list)
            print url_list
            i += 1
        return ret_urls

    def start_requests(self):
        url_list = self.build_url_by_loss_date();
        for url in url_list:
            yield self.make_requests_from_url(url)

    def parse(self, response):
        stock_code = response.url.split(',')[2][0:6]
        date = response.url.split(',')[1]
        list = response.xpath("//div[@class='left con-br']/text()").extract()
        if list is None or len(list) == 0:
            print '该链接无效'
            return
        table1s = response.xpath("//table[@class='default_tab stock-detail-tab']")
        table2s = response.xpath("//table[@class='default_tab tab-2']")
        for i in range(0, len(table1s)):
            table1 = table1s[i].xpath(".//tbody/tr")
            reason = None
            if i < len(list):
                reason = list[i]
                reason = reason.encode("utf8")
                reason = reason.split("：")[1]
            for one in table1:
                tmp = one.xpath("./td/text()").extract()
                if len(tmp) < 10:
                    break
                department = one.xpath("./td/div[@class='sc-name']/a/text()").extract()
                item = GetContentItem()
                item['tag'] = 1  # 1 买入
                if department is None or len(department) == 0:
                    item['department'] = None
                else:
                    item['department'] = department[0].encode("utf8")
                item['date'] = date
                item['stock_code'] = stock_code
                item['reason'] = reason
                item['serial_number'] = tmp[0]
                item['buy'] = tmp[5]
                item['buy_percent'] = tmp[6]
                item['sell'] = tmp[7]
                item['sell_percent'] = tmp[8]
                item['net'] = tmp[9]
                yield item
        for i in range(0, len(table2s)):
            table2 = table2s[i].xpath(".//tbody/tr")
            reason = None
            if i < len(list):
                reason = list[i]
                reason = reason.encode("utf8")
                reason = reason.split("：")[1]
            for one in table2:
                tmp = one.xpath("./td/text()").extract()
                if len(tmp) < 9:
                    break
                department = one.xpath("./td/div[@class='sc-name']/a/text()").extract()
                item = GetContentItem()
                item['tag'] = 2  # 2 卖出
                if (department is None or len(department) == 0):
                    item['department'] = None
                else:
                    item['department'] = department[0].encode("utf8")
                item['date'] = date
                item['stock_code'] = stock_code
                item['reason'] = reason
                item['serial_number'] = tmp[0]
                item['buy'] = tmp[4]
                item['buy_percent'] = tmp[5]
                item['sell'] = tmp[6]
                item['sell_percent'] = tmp[7]
                item['net'] = tmp[8]
                yield item

