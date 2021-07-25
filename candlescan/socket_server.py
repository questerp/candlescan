import socketio
from aiohttp import web
import asyncio
import frappe, json
from candlescan.candlescan_api import validate_token
from frappe.realtime import get_redis_server
from frappe.utils import cstr
from candlescan.socket_utils import decode_cookies,validate_data

sio = socketio.AsyncServer(logger=True, engineio_logger=True,async_mode='aiohttp',cors_allowed_origins="*")
app = web.Application()
sio.attach(app)


events_map = {
	"get_platform_data":"platform"
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
	await sio.emit(event, data, room=to)

@sio.event
async def send_to_client(sid, response):
	#frappe.throw("send_to_client")
	to=response['to']
	event = response['event']
	data=response['data']
	await sio.emit(event, data, room=to)
	
	
@sio.event	
async def join(sid, room):
	await sio.enter_room(sid, room)


@sio.event
async def connect(sid, environ):
	microservice = 'microservice' in environ
	print(environ)
	validated = True
	if not microservice:
		raw_cookies = environ.get("HTTP_COOKIE")
		cookies = decode_cookies(raw_cookies)
		validated =cookies and ( microservice or validate_auth(cookies))
	print("validated",validated)
	if validated:
		if not microservice:
			user = cookies.get('user_name')
			if user:
				get_redis_server().hset("sockets",user,sid)
				get_redis_server().hset("sockets",sid,user)
		else:
			sio.enter_room(sid, environ['microservice'])
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
	web.run_app(app, port=9002)	
	
def run_microservices():
	from candlescan.platform import run as run_platform
	from candlescan.broadcaster import run as run_broadcaster

	loop = asyncio.get_event_loop()
	trun_platform = loop.create_task(run_platform())
	trun_broadcaster = loop.create_task(run_broadcaster())

	asyncio.get_event_loop().run_until_complete(asyncio.gather(
	trun_platform,
	trun_broadcaster,
	return_exceptions=False,
	))
	
	asyncio.get_event_loop().run_forever()
		
