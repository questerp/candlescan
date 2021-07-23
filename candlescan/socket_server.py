import socketio
import uvicorn
import asyncio
import frappe, json
from candlescan.candlescan_api import validate_token
from candlescan.broadcaster import dispatch
from frappe.realtime import get_redis_server

mgr = socketio.AsyncRedisManager(frappe.conf.redis_socketio or "redis://localhost:12311")
sio = socketio.AsyncServer(async_mode='asgi',client_manager=mgr)
app = socketio.ASGIApp(sio)

events_map = {
	"get_platform_data":"platform"
}

@sio.event
async def transfer(sid, data):
	if not data or not validate_data(data):
		await sio.emit('from_server', 'Invalide data format', room=sid)
		return
	data['from'] = sid
	event = data['event']
	to = None
	if 'to' in data:
		to = data['to']
	else:
		to = events_map.get(event)
	await sio.emit(event, data, room=to)


@sio.event	
async def join(sid, room):
	await sio.enter_room(sid, room, namespace=None)¶


@sio.event
async def connect(sid, environ, auth):
	microservice = 'microservice' in auth
	validated =microservice or True# validate_auth(auth)
	if validated:
		if not microservice:
			user = auth['user']
			get_redis_server().hset("sockets",user,sid)
			get_redis_server().hset("sockets",sid,user)
		else:
			await sio.enter_room(sid, auth['microservice'])
		await sio.emit('auth', 'Connected', room=sid)
	else:
		return False

def validate_data(data):
	return 'action' in data and 'data' in data
	
def validate_auth(auth):
	if not auth or ('user' not in auth) or ('user_key' not in auth) or ('token' not in auth) or not validate_token(auth['user_key'],auth['token']):
		return False
	return True
		
	
@sio.event
def disconnect(sid):
	user = get_redis_server().hget("sockets",sid)
	get_redis_server().hdel("sockets",user)
	get_redis_server().hdel("sockets",sid)
		
def run():
	print("Starting socket at 9002")
	uvicorn.run(app, host='0.0.0.0', port=9002)
