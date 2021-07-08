# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe, json
from frappe.model.document import Document
from yahoofinancials import YahooFinancials as YF
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
	print(count)
	print(offset)
	if offset >= count:
		offset = 0
	symbols = frappe.db.sql(""" select name,exchange from `tabSymbol` LIMIT %s OFFSET %s """ % (batch,offset),as_dict=True)
	settings.offset = batch
	settings.save()
	yf = YF([a['name'] for a in symbols])
	data = yf.get_key_statistics_data()
	for s in symbols:
		stats = data[s.name]
		
		if stats:
			clean =  cstr(json.dumps(stats))
			print(clean)
			frappe.db.set_value("Symbol","key_statistics_data",clean)
		
	frappe.db.commit()
	

	
