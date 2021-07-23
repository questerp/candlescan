import frappe,json
from frappe.realtime import get_redis_server
from candlescan.candlescan_api import handle
from candlescan.platform import get_platform_data
from frappe.utils import cstr
import socketio

sio = socketio.Client(reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)

		
def run():
	try:
		sio.connect('http://localhost:9002',auth={"microservice":"broadcaster"})
		sio.emit("join", "broadcaster")
		sio.wait()
	except socketio.exceptions.ConnectionError as err:
		sio.sleep(5)
		run()

	
@sio.event
def from_client(data):
	pass
	
	

def broadcast(sid,data):
	pass

