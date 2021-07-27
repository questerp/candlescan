from __future__ import unicode_literals
import frappe, json
from candlescan.utils.get_tickers import get_tickers as gt
import requests
from candlescan.candlescan_service import insert_symbol

def process():
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
