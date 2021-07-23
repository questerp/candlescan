import socketio
import uvicorn
import asyncio
import frappe, json
from candlescan.candlescan_api import validate_token
from candlescan.broadcaster import dispatch
from frappe.realtime import get_redis_server


sio = socketio.AsyncServer(async_mode='asgi')
app = socketio.ASGIApp(sio)


@sio.event
async def to_server(sid, data):
	if not data or not validate_data(data):
		await sio.emit('from_server', 'Invalide data format', room=sid)
		return
	response = await dispatch(sid,data)
	await sio.emit('from_server', response, room=sid)

@sio.event	
async def to_client(sid, data):
	if not data or not validate_data(data):
		frappe.throw('Invalide data format')
	await sio.emit('from_server', data, room=sid)
	

@sio.event
async def connect(sid, environ, auth):
	validated = True# validate_auth(auth)
	if validated:
		user = auth['user']
		get_redis_server().hset("sockets",user,sid)
		get_redis_server().hset("sockets",sid,user)
		await sio.emit('candlescan', 'Connected', room=sid)
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
