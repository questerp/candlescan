# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe, json
from frappe.model.document import Document
from candlescan.candlescan_yf import YahooFinancials as YF
from frappe.utils import cstr

class CandlescanFundamentalsManager(Document):
	pass


def process():
	settings =frappe.get_doc("Candlescan Fundamentals Manager")
	activate = settings.activate
	if not activate:
		return
	batch = settings.batch
	offset =  settings.offset
	count = frappe.db.count("Symbol")
	settings.offset = batch
	if offset >= count:
		settings.offset = 0
		offset = 0
	symbols = frappe.db.sql(""" select name,exchange from `tabSymbol` LIMIT %s OFFSET %s """ % (batch,offset),as_dict=True)
	settings.save()
	yf = YF([a['name'] for a in symbols])
	data = yf.get_key_statistics_data()
	summaries = yf.get_stock_profile_data()
	prices = yf.get_stock_price_data()
	for s in symbols:
		stats = data[s.name]
		summary = summaries[s.name]
		price = prices[s.name]
		if price:
			clean_price =  cstr(json.dumps(price))
			short_name = price['shortName']
			exchange = price['exchangeName']
			frappe.db.set_value("Symbol",s.name,"key_price_data",clean_price,update_modified=False)
			frappe.db.set_value("Symbol",s.name,"company",short_name,update_modified=False)
			frappe.db.set_value("Symbol",s.name,"exchange",exchange,update_modified=False)
			
		if stats:
			clean =  cstr(json.dumps(stats))
			#print(clean)
			frappe.db.set_value("Symbol",s.name,"key_statistics_data",clean,update_modified=False)
			
		if summary:
			website = summary['website']
			industry = summary['industry']
			sector = summary['sector']
			company_summary = summary['longBusinessSummary']
			clean_summary =  cstr(json.dumps(summary))
			frappe.db.set_value("Symbol",s.name,"key_summary_data",clean_summary,update_modified=False)
			frappe.db.set_value("Symbol",s.name,"summary",company_summary,update_modified=False)
			frappe.db.set_value("Symbol",s.name,"sector",sector,update_modified=False)
			frappe.db.set_value("Symbol",s.name,"industry_type",industry,update_modified=False)
			frappe.db.set_value("Symbol",s.name,"website",website,update_modified=False)
		
	frappe.db.commit()
	

	
