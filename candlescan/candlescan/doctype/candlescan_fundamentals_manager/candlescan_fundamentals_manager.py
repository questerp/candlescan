# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe, json
from frappe.model.document import Document
from candlescan.candlescan_yf import fetch_calendars,YahooFinancials as YF
from frappe.utils import cstr
from candlescan.get_tickers import get_tickers as gt
import requests

class CandlescanFundamentalsManager(Document):
	pass

def get_calendars():
	fetch_calendars()
	
def get_tickers():
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
			symbol.insert()
			
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
			symbol.insert()
			
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
			symbol.insert()
			
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
			symbol.insert()
	frappe.db.commit()

def process():
	settings =frappe.get_doc("Candlescan Fundamentals Manager")
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
	

	
