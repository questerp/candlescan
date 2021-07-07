# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe, random
from frappe.model.document import Document
from frappe.realtime import get_redis_server

class CandlescanExtrasManager(Document):
	pass

@frappe.whitelist()
def process_extras():
	extras_manager = frappe.get_doc("Candlescan Extras Manager")
	batch_size = extras_manager.batch_size
	offset = 0
	count = frappe.db.count("Symbol")
	pages = int(count / batch_size) + 1
	for page in range(0,pages,1):
		offset = offset * page
		symbols = frappe.db.sql("""SELECT name FROM `tabSymbol` LIMIT %s OFFSET %s """ % (batch_size,offset),as_dict=True)
		for symbol in symbols:
			volume = random.randrange(2000000, 5000000)
			frappe.db.set_value("Symbol",symbol.name,"volume",volume)
			
		frappe.db.commit()
		
		# API CALL to get data
