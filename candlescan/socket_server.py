import asyncio
import json
import logging
import websockets
import queue
from frappe.realtime import get_redis_server

conn = get_redis_server()

async def respond(user,data):
	try:
		if data:
			socket = conn.hget(user,"socket")
			if socket:
				print("socket ",socket)
				await socket.send(json.dumps(data))
	except:
		print("error in respond")
	
async def handler(websocket, path):
	try:
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
