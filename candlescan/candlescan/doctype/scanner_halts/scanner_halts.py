# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe.model.document import Document
import time
from frappe.utils.background_jobs import enqueue_doc
from frappe.model.document import Document
from frappe.realtime import get_redis_server
import feedparser
from dateutil import parser
from datetime import timedelta

class ScannerHalts(Document):
	pass


def get_config():
	return {
		"feed_type":"list",
		"can_reorder":False
	}

def signature():
	return [
	{"field":"symbol","header":"Symbol","align":"left","value_type":"string"},
	{"field":"date","header":"Date","align":"left","value_type":"string"},
	{"field":"time","header":"Time","align":"left","value_type":"string"},
	{"field":"company","header":"Company","align":"left","value_type":"string"},
	{"field":"exchange","header":"Exchange","align":"left","value_type":"string"},
	{"field":"code","header":"Code","align":"left","value_type":"string"},
	{"field":"resumption","header":"Resumption","align":"left","value_type":"string"},
	]

def start(scanner_id):        
	URL = "http://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
	redis = get_redis_server()
	
	while(True):
		active = frappe.db.get_value("Scanner Halts","active")
		if not active:
			break;
		data = feedparser.parse(URL)
		if not data:
			time.sleep(10)
		entries = data.entries
		resultdata = []
		for entry in entries:
			halt = {}
			halt['symbol'] = entry.ndaq_issuesymbol
			halt['hdate'] = entry.ndaq_haltdate
			halt['htime'] = entry.ndaq_halttime
			halt['company'] = entry.ndaq_issuename
			halt['exchange'] = entry.ndaq_market
			halt['hcode'] = entry.ndaq_reasoncode
			if halt['htime']:
				res = parser.parse(halt['htime']) + timedelta(minutes=5)
				halt['resumption'] = res
				
			resultdata.append(halt)
		if resultdata:
			redis.publish("candlescan_all",frappe.as_json({"scanner_id":scanner_id,"data":resultdata}))
		time.sleep(30)
		#redis.publish("candlesocket",frappe.as_json({"scanner_id":"alerts","data":{"symbol":"AAPL","price":152}}))
