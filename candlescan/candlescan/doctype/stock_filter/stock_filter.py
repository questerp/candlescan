# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe, json
from frappe.model.document import Document

class StockFilter(Document):
	def validate(self):
		if not self.columns:
			frappe.throw("Please select at least one column for the filter")
		sql = self.validate_script()
		columns = json.loads(self.columns)
		fields = ",".join([a['field'] for a in columns])
		final = """ SELECT symbol,%s from tabSymbol where %s """ % (fields,sql)
		#frappe.msgprint(final)
		try:
			frappe.db.sql("""explain %s""" % final)
		except Exception as e:
			missing_columns = frappe.db.is_missing_column(e)
			if missing_columns:
				frappe.throw(e.args[1].replace("in 'where clause'","in script"))
			frappe.throw("Errors in the script, please check syntax")
		self.sql_script = json.dumps(final)

		
	def validate_script(self):
		if not self.script:
			frappe.throw("Script is required")
		self.script = self.script.replace('(','').replace(')','').replace('[','').replace(']','')
		script = json.loads(self.script)
		conds = script.splitlines()
		sql =""
		or_sql = []
		and_sqls = []
		for cond in conds:
			if not cond:
				continue
			if cond == 'OR':
				or_sql.append(and_sqls)
				and_sqls = []
			else:
				and_sql = "( %s )" % cond
				and_sqls.append(and_sql)
		or_sql.append(and_sqls)
		finalsql = []
		for s in or_sql:
			finalsql.append( "(%s)" % (' AND '.join(s)))
		query = ' OR '.join(finalsql)
		return query
		
		
