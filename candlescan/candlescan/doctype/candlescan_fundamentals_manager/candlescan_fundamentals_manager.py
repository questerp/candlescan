# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe.model.document import Document
from yahoofinancials import YahooFinancials as YF

class CandlescanFundamentalsManager(Document):
	pass


def process():
	settings =frappe.get_doc("Candlescan Fundamentals Manager")
	activate = settings.activate
	if not activate:
		return
	batch = frappe.db.get_value("Candlescan Fundamentals Manager","batch")
	offset =  frappe.db.get_value("Candlescan Fundamentals Manager","offset")
	count = frappe.db.count("Symbol")
	print(count)
	print(offset)
	if offset >= count:
		offset = 0
	symbols = frappe.db.sql(""" select name,exchange from `tabSymbol` LIMIT %s OFFSET %s """ % (batch,offset),as_dict=True)
	frappe.db.set_value("Candlescan Fundamentals Manager","offset",batch)
	yf = YF([a['name'] for a in symbols])
	data = yf.get_key_statistics_data()
	for s in symbols:
		stats = data[s.name]
		print(stats)
		frappe.db.set_value("Symbol","key_statistics_data",stats)
		
	frappe.db.commit()
	

	
