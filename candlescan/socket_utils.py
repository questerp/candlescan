

def get_user(sid):
	return get_redis_server().hget("sockets",sid)

def validate_data(data, fields):
	return all([field in data for field in fields])

def build_response(sid,data,msg):
	pass

