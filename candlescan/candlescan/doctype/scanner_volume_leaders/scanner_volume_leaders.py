# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe, random
import time
from frappe.utils.background_jobs import enqueue_doc
from frappe.model.document import Document
from frappe.realtime import get_redis_server
from candlescan.candlescan_service import broadcast


class ScannerVolumeLeaders(Document):
	pass

# feed_type : ["list","socket","once"]
#

def get_config():
	return {
		"feed_type":"list",
		"can_reorder":False
	}

def signature():
	return [
	{"field":"symbol","header":"Symbol","align":"left","value_type":"string"},
	
	]

def start(scanner_id):        
	redis = get_redis_server()
	interval = 5
	symbols = frappe.db.sql("""select name from tabSymbol limit 30""",as_dict=True)
	#doc = frappe.get_doc("Premarket Scanner")	
	while(True):
		active = frappe.db.get_value("Scanner Volume Leaders","active")
		#frappe.local.cache = {}
		#stop = frappe.cache().hget(scanner_id,"stop",shared=True)
		if not active:
			break
		
		resultdata = []
		for i in symbols:
			resultdata.append(  {"symbol":i.name})
		broadcast("Scanner Volume Leaders",scanner_id,interval,resultdata)
		#redis.publish("candlescan_all",frappe.as_json({"scanner_id":scanner_id,"data":resultdata}))
		#redis.publish("candlesocket",frappe.as_json({"scanner_id":"alerts","data":{"symbol":"AAPL","price":152}}))
