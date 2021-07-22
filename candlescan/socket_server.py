import socketio
import uvicorn
import asyncio
import frappe
from candlescan.candlescan_api import validate_token

redis_server = None
redis_addr = "redis://localhost:12000"
#mgr = socketio.RedisManager(redis_addr)

sio = socketio.AsyncServer(async_mode='asgi')
app = socketio.ASGIApp(sio)

def get_redis_server():
	"""returns redis_socketio connection."""
	global redis_server
	print("get redis")
	if not redis_server:
		from redis import Redis
		redis_server = Redis.from_url(redis_addr)
	return redis_server

@sio.event
async def my_message(sid, data):
	print('message ', data)
	return "OK", 123

@sio.event
async def connect(sid, environ, auth):
	validated = True# validate_auth(auth)
	if validated:
		user = auth['user']
		get_redis_server().hset("sockets",user,sid)	
		await sio.emit('candlescan', 'Connected', room=sid)
	else:
		return False
		
def validate_auth(auth):
	if not auth or ('user' not in auth) or ('user_key' not in auth) or ('token' not in auth) or not validate_token(auth['user_key'],auth['token']):
		return False
	return True
		
	
@sio.event
def disconnect(sid):
	print('disconnect ', sid)

		
def run():
	#start_server = websockets.serve(handler,"0.0.0.0",  9002)
	print("Starting socket at 9002")
	uvicorn.run(app, host='0.0.0.0', port=9002)

	#c = get_redis_server()
	#print(c)
	#asyncio.get_event_loop().run_until_complete(start_server )
	#asyncio.get_event_loop().run_forever()

