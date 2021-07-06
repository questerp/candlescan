# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe.model.document import Document
from frappe.realtime import get_redis_server

class CandlescanAlertManager(Document):
	pass


@frappe.whitelist()        
def proccess_alerts:
	redis = get_redis_server()
	alerts = frappe.db.sql(""" select user,symbol,filters_script from `tabPrice Alert` where enabled=1 and triggered=0 """,as_dict=True)
	if not alerts:
		return
	for alert in alerts:
		price = frappe.db.get_value("Symbol",alert.symbol,"price")
		socket_id = frappe.db.get_value("Customer",alert.user,"socket_id")
		# decode filter script here (filter_script -> sql condition)
		if price and socket_id:
			redis.publish("candlescan_single",frappe.as_json({"socket_id":socket_id,"msg":'%s price is above $_X_' % alert.symbol}))
		
