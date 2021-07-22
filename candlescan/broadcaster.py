import frappe,json
from frappe.realtime import get_redis_server
from candlescan.candlescan_api import handle
from candlescan.platform import get_platform_data

def handle(sid,data):
	action = data['action']
	user =  get_redis_server().hget("sockets",sid)
	if not user:
		return handle(False,"Connection lost")
	#user = json.loads(user)
	# return as handle always
	if action == "get_platform_data":
		return get_platform_data(user)


def ressource(data):
	pass
