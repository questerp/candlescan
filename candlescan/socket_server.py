import socketio


redis_server = None
redis_addr = "redis://localhost:12000"
mgr = socketio.RedisManager(redis_addr)
sio = socketio.Server(client_manager=mgr)
	
def get_redis_server():
	"""returns redis_socketio connection."""
	global redis_server
	print("get redis")
	if not redis_server:
		from redis import Redis
		redis_server = Redis.from_url(redis_addr)
	return redis_server

@sio.event
def connect(sid, environ, auth):
	print('connect ', sid)
	return "Hello"

@sio.event
def disconnect(sid):
	print('disconnect ', sid)

		
if __name__ == '__main__':
	
	#start_server = websockets.serve(handler,"0.0.0.0",  9002)
	print("Starting socket at 9002")	
	#c = get_redis_server()
	#print(c)
	#asyncio.get_event_loop().run_until_complete(start_server )
	#asyncio.get_event_loop().run_forever()

