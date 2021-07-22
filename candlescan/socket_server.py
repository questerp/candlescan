import asyncio
import json
import logging
import websockets
import queue


redis_server = None
redis_addr = "redis://localhost:13000"

def get_redis_server():
	"""returns redis_socketio connection."""
	global redis_server
	print("get redis")
	if not redis_server:
		from redis import Redis
		redis_server = Redis.from_url(redis_addr)
	return redis_server


async def respond(user,data):
	try:
		if data:
			socket = redis_server.hget(user,"socket")
			if socket:
				print("socket ",socket)
				await socket.send(json.dumps(data))
	except:
		print("error in respond")
		
async def handler(websocket, path):
	try:
		print("Starting handler")
		if not redis_server.hexists("sockets",websocket):
			redis_server.hset("sockets",websocket,"socket")
			
		print("Got connection to redis")
		async for msg in websocket:
			print(msg)
			await websocket.send("Hello!")
			#redis_server.publish("socket",msg)
			#response_queue.put({"subscriber":websocket,"data":"data from response_queue"})
				
	except Exception as e:
		print(e)
	
	print("Done")
		
if __name__ == '__main__':
	start_server = websockets.serve(handler,"0.0.0.0",  9002)
	print("Starting socket at 9002")	
	c = get_redis_server()
	print(c)
	asyncio.get_event_loop().run_until_complete(start_server )
	asyncio.get_event_loop().run_forever()

