
import frappe, json
from frappe.utils import cstr
from frappe.realtime import get_redis_server
from urllib.parse import unquote
from frappe.utils.response import json_handler
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
	if event and room and data:
		data = build_response(event,room,data)
		sc = json.dumps(data)
		get_redis_server().lpush("queue",sc)

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

def build_response(event,to,data):
	return {
		"event":event,
		"to":to,
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
