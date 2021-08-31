
import frappe, json
from frappe.utils import cstr
from frappe.realtime import get_redis_server
from urllib.parse import unquote
from frappe.utils.response import json_handler
from candlescan.utils.shared_memory_obj import response_queue 
import time
import asyncio


class SocketEncoder(json.JSONEncoder):
	def default(self, object_):
		return json_handler(object_)
	
class CustomSocketJsonHandler(object):
	@staticmethod
	def dumps(*args, **kwargs):
		#if 'cls' not in kwargs:
		kwargs['cls'] = SocketEncoder
		return json.dumps(*args, **kwargs)

	@staticmethod
	def loads(*args, **kwargs):
		return json.loads(*args, **kwargs)
	
def queue_data(event,room,data):
	#print("keys",get_redis_server().keys())
	if event and room and data:
		data = build_response(event,data,room)
		sc = json.dumps(data,default=str)
		#response_queue.put(sc)
		get_redis_server().lpush("queue",sc)
		#print("queue",get_redis_server().lpop("queue"))

json_encoder = CustomSocketJsonHandler()

async def keep_alive():
	while(1):
		frappe.db.sql("select 'KEEP_ALIVE'")
		await asyncio.sleep(60)
		
def get_user(sid):
	user = get_redis_server().hget("sockets",sid)
	if user:
		user  = cstr(user)
	return user

def validate_data(data, fields):
	return all([field in data for field in fields])

def build_response(message,data,event):#(event,to,data):
	source = ""
	call_id = event
	if isinstance(message,str):
		source = message
	else:
		source = message.get("source_sid")
		call_id = message.get("call_id") or event

	return {
		"event":call_id,
		"to":source,
		"data":data
	}

def decode_cookies(raw_cookie):
	cookies = {}
	if not raw_cookie:
		return cookies
	txtcookies = raw_cookie.split(';') 
	for t in txtcookies:
		#print("t",t)
		key,val = t.split('=')
		#print("key",key)
		#print("val",val)
		
		if key and val:
			cookies[cstr(key).replace(' ','')] = unquote(cstr(val))
	return cookies
