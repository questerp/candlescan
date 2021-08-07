
from __future__ import unicode_literals
import frappe
from frappe.utils import cstr
import socketio
import asyncio
from frappe.realtime import get_redis_server
from candlescan.utils.socket_utils import get_user,validate_data,build_response,json_encoder



sio = socketio.AsyncClient(logger=True,json=json_encoder, engineio_logger=True,reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)
def start():
	asyncio.get_event_loop().run_until_complete(run())
	asyncio.get_event_loop().run_forever()

async def run():
	try:
		await sio.connect('http://localhost:9002',headers={"microservice":"ta_service"})
		await sio.wait()
	except socketio.exceptions.ConnectionError as err:
		print("error",sio.sid,err)
		await sio.sleep(5)
		await run()

	
		

	
def init():
	if not frappe.local.db:
		frappe.connect()	

@sio.event
async def connect():
	init()
	print("I'm connected!")
