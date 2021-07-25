import frappe, json
from frappe.utils import cstr
from frappe.realtime import get_redis_server
from urllib.parse import unquote
from frappe.utils.response import json_handler


json_encoder = CustomSocketJsonHandler()

def get_user(sid):
	user = get_redis_server().hget("sockets",sid)
	if user:
		user  = cstr(user)
	return user

def validate_data(data, fields):
	return all([field in data for field in fields])

def build_response(event,to,data):
	#{"event":"ressource","to":source_sid,"data":"Not connected"}
	#if data:
	#	data = json.dumps(data, default=json_handler, separators=(',',':'))
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

class CustomSocketJsonHandler():
	def dumps(self,data):
		data = json.dumps(data, default=json_handler, separators=(',',':'))
		return data
		
	def loads(self,data):
		if data:
			data = json.loads(data)
		return data
		
