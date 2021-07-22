import asyncio
import json
import logging
import websockets
import queue

redis_server = None
redis_addr = "redis://localhost:12311"

async def get_redis_server():
	"""returns redis_socketio connection."""
	global redis_server
	if not redis_server:
		from redis import Redis
		redis_server = Redis.from_url(redis_addr)
	return redis_server


async def respond(user,data):
	try:
		if data:
			conn = get_redis_server()
			socket = conn.hget(user,"socket")
			if socket:
				print("socket ",socket)
				await socket.send(json.dumps(data))
	except:
		print("error in respond")
	
async def handler(websocket, path):
	try:
		conn = get_redis_server()
		if not conn.hexists("sockets",websocket):
			conn.hset("sockets",websocket,"socket")
		print("Starting handler")
		async for msg in websocket:
			print(msg)
			await websocket.send("Hello!")
			redis.publish("socket",msg)
			#response_queue.put({"subscriber":websocket,"data":"data from response_queue"})
				
	except Exception as e:
		print(e)
	
	print("Done")
		
if __name__ == '__main__':
	start_server = websockets.serve(handler,"0.0.0.0",  9002)
	print("Starting socket at 9002")	
	asyncio.get_event_loop().run_until_complete(start_server, return_exceptions=False)
	asyncio.get_event_loop().run_forever()

