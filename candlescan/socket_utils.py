

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

