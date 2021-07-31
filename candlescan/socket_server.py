import socketio
from aiohttp import web
import asyncio
import frappe, json
from candlescan.api import validate_token
from frappe.realtime import get_redis_server
from frappe.utils import cstr
from candlescan.utils.socket_utils import decode_cookies,validate_data,json_encoder


sio = socketio.AsyncServer(logger=True,json=json_encoder, engineio_logger=True,async_mode='aiohttp',cors_allowed_origins="*")
app = web.Application()
sio.attach(app)


events_map = {
	"get_platform_data":"data_service",
	"ressource":"data_service",
	"get_last_result":"data_service",
	"get_extra_data":"data_service",
	"get_select_values":"data_service",
	"get_history_result":"data_service",
	"set_default_layout":"data_service",
	"run_stock_filter":"filters_service",
	"get_symbol_info":"market_service",
	"lookup":"data_service",
	"get_symbol_prices":"market_service",
	"get_calendar":"market_service",
	"get_filings":"market_service",
}

@sio.event
async def transfer(sid, data):
	if not data or not validate_data(data,["event","data"]):
		await sio.emit('transfer', 'Invalide data format', room=sid)
		return
	data['source_sid'] = sid
	event = data['event']
	to = None
	if 'to' in data:
		to = data['to']
	else:
		to = events_map.get(event)
	if not to:
		sio.emit("errors", "Destination not found", room=sid)
		return
	print("sending",event,"to",to)
	await sio.emit(event, data, room=to)


	
@sio.event	
async def join(sid, room):
	sio.enter_room(sid, room)

@sio.event	
async def join_all(sid, rooms):
	if not rooms:
		return
	for room in rooms:
		sio.enter_room(sid, room)	

@sio.event
async def connect(sid, environ):
	#print("environ",environ)
	frappe.connect()
	microservice=None
	is_microservice = 'HTTP_MICROSERVICE' in environ
	if is_microservice:
		microservice = environ.get("HTTP_MICROSERVICE")
	validated = True
	if not microservice:
		raw_cookies = environ.get("HTTP_COOKIE")
		cookies = decode_cookies(raw_cookies)
		validated =cookies and ( microservice or validate_auth(cookies))
	if validated:
		if not microservice:
			user = cookies.get('user_name')
			if user:
				user =  cstr(user)
				get_redis_server().hset("sockets",user,sid)
				get_redis_server().hset("sockets",sid,user)
		else:
			sio.enter_room(sid,microservice)
		await sio.emit('auth', 'Connected', room=sid)
	else:
		return False


	
def validate_auth(cookies):
	if not cookies:
		return False

	user_name = cookies.get("user_name")
	user_key = cookies.get("user_key")
	user_token = cookies.get("user_token")
	
	if not (user_name and user_key and user_token) or not validate_token(user_key,user_token):
		return False
	return True
		
	
@sio.event
def disconnect(sid):
	user = get_redis_server().hget("sockets",sid)
	if user:
		get_redis_server().hdel("sockets",user)
		get_redis_server().hdel("sockets",sid)

def run_app():
	print("Starting socket at 9002")
	frappe.connect()
	web.run_app(app, port=9002)
	

		
