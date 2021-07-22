import asyncio
import json
import logging
import websockets
import queue

USERS = set()


subscribers = {}
q_mapping = {}
register_queue = queue.Queue()
response_queue = queue.Queue()

async def respond():
	while 1:
		try:
			if response_queue.empty():
				await asyncio.sleep(0.05)
				continue
			response = response_queue.get()
			await response["subscriber"].send(json.dumps(response["response"]))
		except:
			print("error in respond")
	
async def handler(websocket, path):
	try:
		print("Starting handler")
		async for msg in websocket:
			print(msg)
			if websocket not in subscribers.keys():
				print("new socket %s" % websocket)
				subscribers[websocket] = []
				
			websocket.send("Hello!")
			response_queue.put("data from response_queue")
				
	except Exception as e:
		print(e)
	
	print("Done")
		
if __name__ == '__main__':
	start_server = websockets.serve(handler,"0.0.0.0",  9002)
	print("Starting socket at 9002")
	asyncio.get_event_loop().run_until_complete(asyncio.gather(
		start_server,
		respond(),
		return_exceptions=False))
	asyncio.get_event_loop().run_forever()
