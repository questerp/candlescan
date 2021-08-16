
# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe, json
from frappe.realtime import get_redis_server
import time
from frappe.cache_manager import clear_doctype_cache
from candlescan.utils.socket_utils import get_user,validate_data,build_response,json_encoder,keep_alive
from frappe.utils import cstr
import socketio
import asyncio



sio = socketio.AsyncClient(logger=True,json=json_encoder, engineio_logger=True,reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)
def start():
	asyncio.get_event_loop().run_until_complete(run())
	asyncio.get_event_loop().run_forever()

async def run():
	try:
		await sio.connect('http://localhost:9002',headers={"microservice":"alerts_service"})
		await process()
	except socketio.exceptions.ConnectionError as err:
		print("error",sio.sid,err)
		await sio.sleep(5)
		await run()

	
		


@sio.event
async def connect():
	print("I'm connected!")
	
async def process():
	try:
		await _process()
	except Exception as e:
		print(e)
		await sio.sleep(1)
		await process()
		
async def _process():
	#redis = get_redis_server()
	while(True):
		#clear_doctype_cache("Price Alert")
		frappe.db.commit()
		frappe.db.sql("select 'KEEP_ALIVE'")
		time.sleep(5)
		frappe.local.db.commit()
		sessions = frappe.db.sql(""" select token,user from `tabWeb Session`""",as_dict=True)
		for session in sessions:
			alerts = frappe.db.sql(""" select name,user,symbol,filters_script,notify_by_email,enabled,triggered from `tabPrice Alert` where enabled=1 and triggered=0 and user='%s'  """ % session.user,as_dict=True)
			if not alerts:
				print("No alerts")
				continue
			for alert in alerts:
				if not alert.filters_script:
					continue
				filters_script = alert.filters_script
				filters = json.loads(filters_script)
				sql_filter = convert_filters_script(filters)
				symbol = alert.symbol
				if sql_filter and symbol:
					scr = """ select name from tabSymbol where symbol = '{symbol}' and {filter}  """.format(symbol=symbol,filter=sql_filter)
					print("scr %s" % scr)
					exists = frappe.db.sql(scr,as_dict=True)
					print("exists %s" % exists)
					if exists:
						socket_id = get_redis_server().hget("sockets",session.user)
						if socket_id:
							socket_id = cstr(socket_id)
							print("socket_id %s" % socket_id)
							
							frappe.db.set_value("Price Alert",alert.name,"triggered",1)
							#doc.triggered = True
							#doc.save()
							frappe.db.commit()
							msg = '%s alert is triggered' % alert.symbol
							await sio.emit("transfer",build_response("alerts",socket_id,msg))
							#redis.publish("candlescan_single",frappe.as_json({"socket_id":socket_id,"data":'%s alert is triggered' % alert.symbol}))


def convert_filters_script(filters):
	if not filters:
		return ''
	sql = ""
	cond = []
	for filter in filters:
		operator = convert_operator(filter['operator'])
		field = filter['column']['field']
		value = filter['value']
		value_max = filter['value_max']
		if operator and value and operator != 'BETWEEN':
			sc = "%s %s %s" % (field,operator,value)
			cond.append(sc)
		elif operator and value and operator == 'BETWEEN' and value_max:
			sc = "%s %s %s AND %s" % (field,operator,value,value_max)
			cond.append(sc)
	if cond:
		sql = " and ".join(cond)
	print("sql %s" % sql)
	return sql
		
def convert_operator(operator):
	if not operator:
		return ""
	return ">" if operator == "Above" else "<" if operator == "Below" else "BETWEEN" if operator == "Between" else ""
		
