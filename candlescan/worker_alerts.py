# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe, json
from frappe.realtime import get_redis_server
import time

def process():
	redis = get_redis_server()
	while(True):
		alerts = frappe.db.sql(""" select name,user,symbol,filters_script,notify_by_email,enabled,triggered from `tabPrice Alert` where enabled=1 and triggered=0 limit 100""",as_dict=True)
		if not alerts:
			return
		for alert in alerts:
			if not alert.filters_script:
				continue
			filters_script = alert.filters_script
			filters = json.loads(filters_script)
			sql_filter = convert_filters_script(filters)
			symbol = alert.symbol
			if sql_filter and symbol:
				exists = frappe.db.sql(""" select name from tabSymbol where symbol == '{symbol}' and {filter}  """.format(symbol=symbol,filter=sql_filter),as_dict=True)
				if exists:
					socket_id = frappe.db.get_value("Customer",alert.user,"socket_id")
					if socket_id:
						doc = frappe.get_doc("Price Alert",alert.name)
						doc.triggered = True
						doc.save()
						redis.publish("candlescan_single",frappe.as_json({"socket_id":socket_id,"data":'%s alert is triggered' % alert.symbol}))
		time.sleep(3)

def convert_filters_script(filters):
	if not filters:
		return ''
	sql = ""
	cond = []
	for filter in filters:
		operator = convert_operator(filter['operator'])
		field = filter['field']
		value = filter['value']
		value_max = filter['value_max']
		if operator and value and operator != 'BETWEEN':
			sc = "'%s' %s %s" % (field,operator,value)
			cond.append(sc)
		elif operator and value and operator == 'BETWEEN' and value_max:
			sc = "'%s' %s %s AND %s" % (field,operator,value,value_max)
			cond.append(sc)
	if cond:
		sql = " and ".join(cond)
	print("sql %s" % sql)
	return sql
		
def convert_operator(operator):
	if not operator:
		return ""
	return ">" if operator == "Above" else "<" if operator == "Below" else "BETWEEN" if operator == "Between" else ""
		
