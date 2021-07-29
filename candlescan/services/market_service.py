from __future__ import unicode_literals
import frappe, json, random
from candlescan.utils.get_tickers import get_tickers as gt
import requests
from candlescan.utils.candlescan import insert_symbol
from candlescan.utils.yf import YahooFinancials as YF
import frappe, json
from frappe.utils import cstr
import requests
import sys
import calendar
import re
from json import loads,dumps
import time
from bs4 import BeautifulSoup
import datetime
import pytz
import random
import socketio
import asyncio
from candlescan.utils.socket_utils import get_user,validate_data,build_response,json_encoder

sio = socketio.AsyncClient(logger=True,json=json_encoder, engineio_logger=True,reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)

def start():
	asyncio.get_event_loop().run_until_complete(run())
	asyncio.get_event_loop().run_forever()

async def run():
	try:
		await sio.connect('http://localhost:9002',headers={"microservice":"market_service"})
		while(True):
			await asyncio.sleep(3)
			price = random.uniform(1,10)
			await sio.emit("transfer",build_response("price","MSON",{"symbol":"MSON","price":price}))
		
	except socketio.exceptions.ConnectionError as err:
		print("error",sio.sid,err)
		await sio.sleep(5)
		await run()

def init():
	frappe.connect()	

@sio.event
async def connect():
	init()
	print("I'm connected!")




try:
    from urllib import FancyURLopener
except:
    from urllib.request import FancyURLopener
import frappe

class UrlOpener(FancyURLopener):
    version = 'w3m/0.5.3+git20180125'

def process_calendar():
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


def process_tickers():
	#NYSE=True, NASDAQ=True, AMEX=True
	tickers = gt(NYSE=True, NASDAQ=False, AMEX=False)
	for ticker in tickers:
		ticker['symbol'] = ticker['symbol'].replace('^','p')
		ticker['name'] = (ticker['name'][:100] + '..') if len(ticker['name']) > 100 else ticker['name']
		exist = frappe.db.exists("Symbol",ticker['symbol'])
		if not exist:
			print(ticker)
			print(ticker['symbol'])
			symbol = frappe.get_doc({
				'doctype':'Symbol',
				'symbol':ticker['symbol'],
				'company':ticker['name'],
				'exchange':'NYSE'
			})
			insert_symbol(symbol)
			
	tickers = gt(NYSE=False, NASDAQ=True, AMEX=False)
	for ticker in tickers:
		ticker['symbol'] = ticker['symbol'].replace('^','p')	
		ticker['name'] = (ticker['name'][:100] + '..') if len(ticker['name']) > 100 else ticker['name']
		exist = frappe.db.exists("Symbol",ticker['symbol'])
		if not exist:
			print(ticker['symbol'])
			symbol = frappe.get_doc({
				'doctype':'Symbol',
				'symbol':ticker['symbol'],
				'company':ticker['name'],
				'exchange':'NASDAQ'
			})
			insert_symbol(symbol)
			
	tickers = gt(NYSE=False, NASDAQ=False, AMEX=True)
	for ticker in tickers:
		ticker['symbol'] = ticker['symbol'].replace('^','p')	
		ticker['name'] = (ticker['name'][:100] + '..') if len(ticker['name']) > 100 else ticker['name']
		exist = frappe.db.exists("Symbol",ticker['symbol'])
		if not exist:
			print(ticker['symbol'])
			symbol = frappe.get_doc({
				'doctype':'Symbol',
				'symbol':ticker['symbol'],
				'company':ticker['name'],
				'exchange':'AMEX'
			})
			insert_symbol(symbol)
			
	#https://api.iextrading.com/1.0/ref-data/symbols
	URL = "https://api.iextrading.com/1.0/ref-data/symbols"
	r = requests.get('https://api.iextrading.com/1.0/ref-data/symbols')
	data = r.json()
	for s in data:
		if not s['symbol']:
			continue
		if not frappe.db.exists("Symbol",s['symbol']):
			symbol = frappe.get_doc({
				'doctype':'Symbol',
				'symbol':s['symbol'],
				'company':s['name'] or 'N/A',
				'exchange':'N/A'
			})
			insert_symbol(symbol)

def process_fundamentals():
	settings =frappe.get_doc("Fundamentals")
	activate = settings.activate
	if not activate:
		return
	batch = settings.batch
	offset =  settings.offset
	count = frappe.db.count("Symbol")
	settings.offset = offset + batch
	if offset >= count:
		settings.offset = 0
		offset = 0
	symbols = frappe.db.sql(""" select name,exchange from `tabSymbol` LIMIT %s OFFSET %s """ % (batch,offset),as_dict=True)
	settings.save()
	global yf
	if batch == 1 and len(symbols) == 1:
		_symbol = symbols[0]
		yf = YF(_symbol.name)
	else:
		yf = YF([a['name'] for a in symbols])
	data = yf.get_key_statistics_data()
	summaries = yf.get_stock_profile_data()
	prices = yf.get_stock_price_data()
	sumdatas = yf.get_stock_summary_detail()
	for s in symbols:
		print("Fetching %s" % s.name)
		stats = data[s.name] if s.name in data else []
		summary = summaries[s.name] if s.name in summaries else []
		price = prices[s.name] if s.name in prices else []
		sumdata = sumdatas[s.name] if s.name in sumdatas else []
		if sumdata:
			clean_sumdata =  cstr(json.dumps(sumdata))
			frappe.db.set_value("Symbol",s.name,"stock_summary_detail",clean_sumdata,update_modified=False)
			
		if price:
			clean_price =  cstr(json.dumps(price))
			short_name = price['shortName'] if 'shortName' in price else ''
			exchange = price['exchangeName'] if 'exchangeName' in price else ''
			frappe.db.set_value("Symbol",s.name,"key_price_data",clean_price,update_modified=False)
			frappe.db.set_value("Symbol",s.name,"company",short_name,update_modified=False)
			frappe.db.set_value("Symbol",s.name,"exchange",exchange,update_modified=False)
			
		if stats:
			clean =  cstr(json.dumps(stats))
			#print(clean)
			frappe.db.set_value("Symbol",s.name,"key_statistics_data",clean,update_modified=False)
			
		if summary:
			website =  summary['website'] if 'website' in summary else ''
			industry = summary['industry'] if 'industry' in summary else ''
			sector = summary['sector']  if 'sector' in summary else ''
			company_summary = summary['longBusinessSummary']  if 'longBusinessSummary' in summary else ''
			clean_summary =  cstr(json.dumps(summary))
			frappe.db.set_value("Symbol",s.name,"key_summary_data",clean_summary,update_modified=False)
			frappe.db.set_value("Symbol",s.name,"summary",company_summary,update_modified=False)
			frappe.db.set_value("Symbol",s.name,"sector",sector,update_modified=False)
			frappe.db.set_value("Symbol",s.name,"industry_type",industry,update_modified=False)
			frappe.db.set_value("Symbol",s.name,"website",website,update_modified=False)
		
	frappe.db.commit()
