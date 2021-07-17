# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe.model.document import Document
import sqlvalidator

class StockFilter(Document):
	def validate(self):
		sql = self.validate_script()
		final = """ SELECT name from tabSymbol where {cond} """.format(cond= sql)
		frappe.msgprint(final)
		
		sql_query = sqlvalidator.parse(final)
		if not sql_query.is_valid():
		    frappe.throw(sql_query.errors)
		
	def validate_script(self):
		if not self.script:
			frappe.throw("Script is required")
		
		or_blocks = self.script.split(' OR ')
		sql =""
		and_sqls = []
		for or_block in or_blocks:
			and_blocks = or_block.splitlines()
			and_sql = " AND ".join(and_blocks)
			and_sql = "( %s )" % and_sql
			and_sqls.append(and_sql)
			
		if and_sqls:
			if len(and_sqls) > 1:
				sql = " OR ".join(and_sqls)
			else:
				sql = and_sqls
				
		return sql
		
		
