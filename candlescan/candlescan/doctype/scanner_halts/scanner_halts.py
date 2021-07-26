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
from candlescan.candlescan_service import broadcast
from candlescan.socket_utils import get_user,validate_data,build_response,json_encoder
import socketio
import asyncio

class ScannerHalts(Document):
	pass



sio = socketio.Client(logger=True,json=json_encoder, engineio_logger=True,reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)
sio.connect('http://localhost:9002',headers={"microservice":"scanner_halts"})


def get_config():
	return {
		"feed_type":"list",
		"can_reorder":False
	}

def signature():
	return [
	{"field":"symbol","header":"Symbol","align":"left","value_type":"string"},
	{"field":"status","header":"Status","align":"left","value_type":"select","doctype":"Halt status"},
	{"field":"hdate","header":"Date","align":"left","value_type":"string"},
	{"field":"htime","header":"Halt Time","align":"left","value_type":"string"},
	{"field":"hcode","header":"Code","align":"left","value_type":"string"},
	{"field":"resumption_date","header":"Resumption Date","align":"left","value_type":"string"},
	{"field":"resumption_time","header":"Resumption Time","align":"left","value_type":"string"},
	]

def start(scanner_id):        
	URL = "http://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
	redis = get_redis_server()
	interval = 30
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
			halt['status'] = "Halted" if not entry.ndaq_resumptiontradetime else "Resumed"
			halt['hdate'] = entry.ndaq_haltdate
			halt['htime'] = entry.ndaq_halttime
			halt['resumption_date'] = entry.ndaq_resumptiondate
			halt['resumption_time'] = entry.ndaq_resumptiontradetime
			
			halt['hcode'] = entry.ndaq_reasoncode
			if halt['htime'] and not halt['resumption_time']:
				res = parser.parse(halt['htime']) + timedelta(minutes=5)
				halt['resumption_time'] = res.strftime("%H:%M:%S")
				
			resultdata.append(halt)
		if resultdata:
			sio.emit("transfer",build_response("halts","halts",resultdata))
		time.sleep(30)
			#broadcast("Scanner Halts",scanner_id,interval,resultdata)
			#redis.publish("candlescan_all",frappe.as_json({"scanner_id":scanner_id,"data":resultdata}))
		#time.sleep(30)
		#redis.publish("candlesocket",frappe.as_json({"scanner_id":"alerts","data":{"symbol":"AAPL","price":152}}))
