import sys
import calendar
import re
from json import loads,dumps
import time
from bs4 import BeautifulSoup
import datetime
import pytz
import random
try:
    from urllib import FancyURLopener
except:
    from urllib.request import FancyURLopener
import frappe

from candlescan.candlescan_service import insert_symbol

class UrlOpener(FancyURLopener):
    version = 'w3m/0.5.3+git20180125'

def process():
    targets = ["earnings","splits","ipo","economic"]
    for target in targets:
        url = 'https://finance.yahoo.com/calendar/'+target
        urlopener = UrlOpener()
        # Try to open the URL up to 10 times sleeping random time if something goes wrong
        max_retry = 10
        data = None
        for i in range(0, max_retry):
           response = urlopener.open(url)
           if response.getcode() != 200:
               time.sleep(random.randrange(10, 20))
           else:
               response_content = response.read()
               soup = BeautifulSoup(response_content, "html.parser")
               re_script = soup.find("script", text=re.compile("root.App.main"))
               if re_script is not None:
                   script = re_script.text
                   # bs4 4.9.0 changed so text from scripts is no longer considered text
                   if not script:
                       script = re_script.string
                   data = loads(re.search("root.App.main\s+=\s+(\{.*\})", script).group(1))
                   response.close()
                   break
               else:
                   time.sleep(random.randrange(10, 20))
           if i == max_retry - 1:
               break

        if data:
            rows = data["context"]["dispatcher"]["stores"]["ScreenerResultsStore"]["results"]["rows"]
            if rows:
                #print(rows)
                for row in rows:
                    #print(row)
                    if 'ticker' in row:
                        ticker = row['ticker']
                        if ticker and not frappe.db.exists("Symbol",ticker):
                            companyshortname = row['companyshortname'] if 'companyshortname' in row else ''
                            exchange_short_name= row['exchange_short_name'] if 'exchange_short_name' in row else 'N/A'
                            print(ticker)
                            new_symbol = frappe.get_doc({
                                'doctype':'Symbol',
                                'symbol':ticker,
                                'company':companyshortname,
                                'exchange':exchange_short_name
                            })
                            insert_symbol(new_symbol)
                            
                json_rows = dumps(rows)
                frappe.db.set_value("Fundamentals",None,target,json_rows)
                time.sleep(3)
    frappe.db.commit()
