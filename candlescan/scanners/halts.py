# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
import time
from frappe.realtime import get_redis_server
import feedparser
from dateutil import parser
from datetime import timedelta
from candlescan.utils.socket_utils import get_user,validate_data,build_response,json_encoder
from candlescan.utils.candlescan import save_scanner_state
import socketio
import asyncio


sio = socketio.Client(logger=True,json=json_encoder, engineio_logger=True,reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)

def connect():
	try:
		sio.connect('http://localhost:9002',headers={"microservice":"scanner_halts"})
	except socketio.exceptions.ConnectionError as err:
		print("error",sio.sid,err)
		sio.sleep(5)
		connect()

def start():    
	connect()
	URL = "http://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
	redis = get_redis_server()
	interval = 30
	while(True):
		#active = frappe.db.get_value("Scanner Halts","active")
		#if not active:
		#	break;
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
			save_scanner_state("halts",resultdata)
			sio.emit("transfer",build_response("halts","halts",resultdata))
		time.sleep(interval)
			#broadcast("Scanner Halts",scanner_id,interval,resultdata)
			#redis.publish("candlescan_all",frappe.as_json({"scanner_id":scanner_id,"data":resultdata}))
		#time.sleep(30)
		#redis.publish("candlesocket",frappe.as_json({"scanner_id":"alerts","data":{"symbol":"AAPL","price":152}}))
