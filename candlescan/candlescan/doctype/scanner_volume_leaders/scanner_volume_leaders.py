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
	{"field":"volume","header":"Volume","align":"left","value_type":"number"}
	]

def start(scanner_id):        
	redis = get_redis_server()
	settings = frappe.get_doc("Scanner Volume Leaders")
	interval = settings.interval
	max_rows = settings.max_rows
	#doc = frappe.get_doc("Premarket Scanner")	
	while(True):
		symbols = frappe.db.sql("""select symbol,volume from tabSymbol where volume>200000 order by volume desc limit %s""" % max_rows,as_dict=True)
		active = frappe.db.get_value("Scanner Volume Leaders",None,"active")
		#frappe.local.cache = {}
		#stop = frappe.cache().hget(scanner_id,"stop",shared=True)
		if not active:
			break
		broadcast("Scanner Volume Leaders",scanner_id,interval,symbols)
