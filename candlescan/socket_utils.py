from frappe.utils import cstr


def get_user(sid):
	return get_redis_server().hget("sockets",sid)

def validate_data(data, fields):
	return all([field in data for field in fields])

def build_response(event,to,data):
	#{"event":"ressource","to":source_sid,"data":"Not connected"}
	return {
		"event":event,
		"to":to,
		"data":data
	}

def decode_cookies(raw_cookie):
	cookies = {}
	txtcookies = raw_cookie.split(';') 
	for t in txtcookies:
		#print("t",t)
		key,val = t.split('=')
		#print("key",key)
		#print("val",val)
		
		if key and val:
			cookies[cstr(key).replace(' ','')] = cstr(val)
	return cookies
