# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe.model.document import Document

class StockFilter(Document):
	def validate(self):
		sql = self.validate_script()
		final = """ SELECT name from tabSymbol where %s """ % sql
		frappe.msgprint(final)
		try:
			frappe.db.sql("""explain %s""" % final)
		except Exception:
			frappe.throw("Errors in the script, please check syntax")

		
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
			if len(and_sqls) > 0:
				sql = " OR ".join(and_sqls)
			else:
				sql = and_sqls
				
		return sql
		
		
