import frappe,json
from frappe.realtime import get_redis_server
from candlescan.api import handle
from candlescan.utils.socket_utils import get_user,validate_data,build_response,json_encoder,keep_alive
from frappe.utils import cstr,getdate, get_time, today,now_datetime
import socketio
import asyncio
from candlescan.utils.candlescan import get_yahoo_prices as get_prices
import FinNews as fn
from frappe.utils import get_datetime,now_datetime
from datetime import timedelta


sio = socketio.AsyncClient(logger=True,json=json_encoder, engineio_logger=True,reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)

def start():
	asyncio.get_event_loop().run_until_complete(run())
	asyncio.get_event_loop().run_forever()

async def run():
	try:
		await sio.connect('http://localhost:9002',headers={"microservice":"news_service"})
		await keep_alive()
	except socketio.exceptions.ConnectionError as err:
		print("error",sio.sid,err)
		await sio.sleep(5)
		await run()
		
@sio.event
async def get_news(message):
	sid = message.get("source_sid")
	symbol = message.get("data")
	if not symbol:
		return
	symbol = symbol.upper()
	news = frappe.db.sql(""" select creation,date,title, symbol, source from tabNews where symbol='%s'""" %  (symbol),as_dict=True)
	if not news:
		news = fetch_news(symbol)
	elif news[0].creation < (now_datetime() - timedelta(minutes=5)):
		news = fetch_news(symbol)
		

	await sio.emit("transfer",build_response("get_news",sid,news))

def fetch_news(symbol):
	mytopic = "$%s" % symbol
	feedSeekingAlpha = fn.SeekingAlpha(topics=[mytopic])
	feedYahoo = fn.Yahoo(topics=[mytopic])
	#feedNasdaq = fn.Nasdaq(topics=[mytopic])
	newsSeekingAlpha = feedSeekingAlpha.get_news()
	newsYahoo = feedYahoo.get_news()
	#newsNasdaq = feedNasdaq.get_news()
	news = []
	news.extend(newsSeekingAlpha)
	news.extend(newsYahoo)
	result = []
	#.extend(newsNasdaq)
	frappe.db.sql(""" delete from tabNews where symbol='%s' """ % symbol )
	frappe.db.commit()
	
	for n in news:
		if n and n.get("published"):
			op = frappe.get_doc({"doctype":"News"})
			op.title = n.get("title")
			op.date = n.get("published")
			op.source = n.get("link")
			#op.content = n.get("summary")
			op.symbol = symbol
			data = op.insert()
			result.append(data)
	frappe.db.commit()
	return result

	
	
		
@sio.event
async def connect_error(message):
	print("connect_error")
	print(message)

@sio.event
async def connect():
	print("I'm connected!")

@sio.event
async def disconnect():
	print("I'm disconnected!")
  
  
  
