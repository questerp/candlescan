import asyncio
import json
import logging
import websockets
import queue

	
async def handler(websocket, path):
	try:
		print("Starting handler")
		#if not redis_server.hexists("sockets",websocket):
		#	redis_server.hset("sockets",websocket,"socket")
			
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
	#c = get_redis_server()
	#print(c)
	asyncio.get_event_loop().run_until_complete(start_server, return_exceptions=False)
	asyncio.get_event_loop().run_forever()

