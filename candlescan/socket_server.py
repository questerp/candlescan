import socketio
import uvicorn
import asyncio
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
	if not auth or ('user' not in auth) or ('user_key' not in auth) or ('token' not in auth):
		raise ConnectionRefusedError('Missing header infos, authentication failed')
	if not validate_token(auth['user_key'],auth['token']):
		raise ConnectionRefusedError('Invalide token, authentication failed')
	get_redis_server().hset("sockets",user,sid)	
	await sio.emit('candlescan', 'Connected', room=sid)

@sio.event
def disconnect(sid):
	print('disconnect ', sid)

		
if __name__ == '__main__':
	#start_server = websockets.serve(handler,"0.0.0.0",  9002)
	print("Starting socket at 9002")
	uvicorn.run(app, host='0.0.0.0', port=9002)

	#c = get_redis_server()
	#print(c)
	#asyncio.get_event_loop().run_until_complete(start_server )
	#asyncio.get_event_loop().run_forever()

