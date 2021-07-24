import frappe,json
from frappe.realtime import get_redis_server
from candlescan.candlescan_api import handle
from candlescan.platform import get_platform_data
from frappe.utils import cstr
import socketio
import asyncio


sio = socketio.AsyncClient(reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)


async def run():
	try:
		await sio.connect('http://localhost:9002',headers={"microservice":"broadcaster"})
		await sio.emit("join", "broadcaster")
		await sio.wait()
	except socketio.exceptions.ConnectionError as err:
		await sio.sleep(5)
		await run()


@sio.event
async def from_client(data):
	pass
