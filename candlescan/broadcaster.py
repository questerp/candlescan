import frappe
from frappe.realtime import get_redis_server
from candlescan.candlescan_api import handle


def handle(sid,data):
	action = data['action']
	user = get_redis_server().hget("sockets",sid)
	if not user:
		return handle(False,"Connection lost")
	# return as handle always
	if action == "get_platform_data":
		return candlescan.paltform.get_platform_data(user)


def ressource(data):
	pass
