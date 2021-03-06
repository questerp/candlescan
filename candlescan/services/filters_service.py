import frappe,json
from frappe.realtime import get_redis_server
from candlescan.api import handle
from candlescan.utils.socket_utils import get_user,validate_data,build_response,json_encoder,keep_alive
from frappe.utils import cstr,getdate, get_time, today,now_datetime
import socketio
import asyncio
from candlescan.utils.candlescan import get_yahoo_prices as get_prices
from datetime import datetime as dt


sio = socketio.AsyncClient(logger=True,json=json_encoder, engineio_logger=True,reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)

def start():
	asyncio.get_event_loop().run_until_complete(run())
	asyncio.get_event_loop().run_forever()

async def run():
	try:
		await sio.connect('http://localhost:9002',headers={"microservice":"filters_service"})
		await keep_alive()
	except socketio.exceptions.ConnectionError as err:
		print("error",sio.sid,err)
		await sio.sleep(5)
		await run()


		
@sio.event
async def connect_error(message):
	print("connect_error")
	print(message)

@sio.event
async def connect():
	print("I'm connected!")

@sio.event
async def disconnect():
	print("I'm disconnected!")


    
@sio.event
async def run_stock_filter(message):
	name = message.get('data')
	if not name:
		return
	#frappe.db.commit()
	
	data = run_filter(name)
	await sio.emit("transfer",build_response(message,data,"run_stock_filter"))

	#filter = frappe.db.sql("select sql_script,limit_results,name,sort_field from `tabStock Filter` where name='%s' limit 1" % name,as_dict=True)
	#if filter:
	#	filter = filter[0]
	#else:
	#	return

def run_filter(name):
	if not name:
		return
	start = dt.now()

	frappe.db.commit()
	filter = frappe.db.sql("""select * from `tabStock Filter` where name='%s'"""%name,as_dict=1)
	if filter:
		filter = filter[0]
	else:
		return
	#print( filter)
	
	data = []
	if filter.sql_script:
		sql = json.loads(filter.sql_script)
		sort = "ASC" if filter.sort_mode == "Ascending" else "DESC"
		if sql:
			try:
				data = frappe.db.sql("""%s order by %s %s limit %s""" % (sql,filter.sort_field,sort,filter.limit_results or 1),as_dict=True)
			except Exception as e:
				print(e)
				print(sql)

	end = dt.now()
	print("done",end-start)
	return data