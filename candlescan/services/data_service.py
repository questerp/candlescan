import frappe,json
from frappe.realtime import get_redis_server
from candlescan.api import handle
from candlescan.utils.socket_utils import get_user,validate_data,build_response,json_encoder,keep_alive
from frappe.utils import cstr,getdate, get_time, today,now_datetime
import socketio
import asyncio
from candlescan.utils.candlescan import get_yahoo_prices as get_prices
import time
import threading
from threading import Lock
from candlescan.utils.shared_memory_obj import response_queue 

lock = Lock()

public_ressources = ["Scanner"]

sio = socketio.AsyncClient(logger=True,json=json_encoder, engineio_logger=True,reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)

def start():
	asyncio.get_event_loop().run_until_complete(run())
	asyncio.get_event_loop().run_forever()

async def run():
	try:
		
		await sio.connect('http://localhost:9002',headers={"microservice":"data_service"})
		with lock:
			get_redis_server()
			threading.Thread(target=handle_queue).start()	
		await keep_alive()
	except socketio.exceptions.ConnectionError as err:
		print("error",sio.sid,err)
		await sio.sleep(5)
		await run()

def handle_queue():
	try:
		#from redis import Redis
		#redis = Redis.from_url(redis_socketio or "redis://localhost:12311")
		#redis = get_redis_server()
		while(1):
			#if response_queue.empty():
			data =  get_redis_server().lpop("queue")#response_queue.get() # 
			if data:
				print("data",data)
				try:
					data = cstr(data)
					resp = json.loads(data)
					asyncio.get_event_loop().create_task(sio.emit("transfer",resp))
					
				except Exception as ex:
					print(ex)
			else:
				print("data",get_redis_server().keys(),data)
				time.sleep(1)
				
	except Exception as ex:
		#raise
		time.sleep(1)
		handle_queue()
		
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


@sio.event
async def subscribe_symbol(message):
	source = message.get("source_sid")
	symbol = message.get("data")
	if not symbol:
		return
	active = frappe.db.get_value("Symbol",symbol,"active")
	if active:
		#sio.enter_room(source, symbol)
		get_redis_server().sadd("symbols",symbol)
	
@sio.event
async def lookup(message):
	symbol = message.get('data')
	sid = message.get('source_sid')
	if not symbol or len(symbol) > 9 or len(symbol) < 1:
		return
	symbols = frappe.db.sql("""select symbol from tabSymbol where name LIKE  %(symbol)s limit 10 """ ,{"symbol":'%%%s%%' % symbol},as_dict=True)
	await sio.emit("transfer",build_response("lookup",sid,symbols))
	
	
@sio.event
async def get_history_result(message):
	data = message.get('data')
	source_sid = message.get('source_sid')
	if not source_sid:
		return
	scanner_id = data.get('scanner_id')
	date = data.get('date')
	frappe.db.commit()
	results = get_history(scanner_id,date)
	await sio.emit("transfer",build_response("get_history_result",source_sid,results[0]))
	

@sio.event
async def set_default_layout(message):
	data = message.get('data')
	if not data:
		return
	source_sid = message.get('source_sid')
	user = get_user(source_sid)
	if user and data:
		frappe.db.set_value("Customer",user,"default_layout",data)
		frappe.db.commit()
		await sio.emit("transfer",build_response("set_default_layout",source_sid,"Default layout changed"))    
		
@sio.event
async def get_last_result(message):
	scanner_id = message.get('data')
	source_sid = message.get('source_sid')
	if not source_sid:
		return
	
	if not scanner_id:
		return
	frappe.db.commit()
	results = get_history(scanner_id,now_datetime())
	#results = frappe.db.sql("""select state,date from `tabScanner Result` where scanner='%s' order by date desc limit 1""" % scanner_id,as_dict=True)
	if results and len(results):
		await sio.emit("transfer",build_response("get_last_result",source_sid,results[0]))
		

def get_history(scanner,date):
	if scanner and date:
		return frappe.db.sql("""select state,date from `tabScanner Result` where scanner='%s' and date<='%s' order by date desc limit 1""" % (scanner,date),as_dict=True)
	
@sio.event
async def ressource(message):
	try:
		data = message.get('data')
		#print("this is ressource",data)

		validated = validate_data(data,["doctype","method"])
		if not validated:
			return
		print("validated",validated)
		source_sid = message.get('source_sid')
		if not source_sid:
			return
		if not (validated or source_sid):
			await sio.emit("transfer",build_response("ressource",source_sid,"Invalid data format"))
			return

		user = get_user(source_sid)
		doctype = data.get("doctype")
		name = data.get("name")
		method = data.get("method")
		document = data.get("doc")
		if not name and document:
			print("doc",document)
			name = document.get("name")

		if method == "save":
			if name:
				doc = frappe.get_doc(doctype, name)
				doc.update(document)
				modified =  frappe.db.sql("""select modified from `tab{0}` where name = %s for update""".format(doctype), name, as_dict=True)
				if modified:
					modified = cstr(modified[0].modified)
					doc.modified = modified
				response = doc.save().as_dict()
				
				if response:
					await sio.emit("transfer",build_response("ressource",source_sid,{"method":method,"doctype":doctype,"data":response}))
					frappe.db.commit()
					#frappe.clear_cache(doctype=doctype)
					#await sio.emit("send_to_client",build_response("ressource",source_sid,response))

			else:
				document.update({"doctype": doctype})
				response = frappe.get_doc(document).insert()
				if response:
					await sio.emit("transfer",build_response("ressource",source_sid,{"method":method,"doctype":doctype,"data":response}))
					frappe.db.commit()
					#frappe.clear_cache(doctype=doctype)
					#await sio.emit("send_to_client",build_response("ressource",source_sid,response))

		if method == "delete" and name:
			frappe.delete_doc(doctype, name, ignore_missing=True)
			frappe.db.commit()
			await sio.emit("transfer",build_response("ressource",source_sid,{"method":method,"doctype":doctype,"data":"Deleted"}))
			#await sio.emit("send_to_client",build_response("ressource",source_sid,"Deleted"))


		if method == "list":
			frappe.db.commit()
			response = []
			if doctype == "Scanner":
				response = frappe.db.sql(""" select * from `tabCandlescan scanner` """,as_dict=True)

			elif doctype == "Extras":
				extras = frappe.db.get_single_value('Candlescan Settings', 'extras')
				response = []
				if extras:
					extras = extras.splitlines()
				for ex in extras:
					name, label, value_type,extra_doctype = ex.split(':')
					response.append({"field":name,"header":label,"value_type":value_type,"doctype":extra_doctype,"signature":False})

			else:
				response = frappe.db.sql(""" select * from `tab%s` where user='%s'""" % (doctype,user),as_dict=True)

			await sio.emit("transfer",build_response("ressource",source_sid,{"method":method,"doctype":doctype,"data":response}))
			#await sio.emit("send_to_client",build_response("ressource",source_sid,response))
	except Exception as exc:
		print("ERROR---------------------")
		print(exc)
		await sio.emit("transfer",build_response("errors",source_sid,"Operation failed! %s" % exc))
			
			

			       
@sio.event
async def get_platform_data(data):
	source = data['source_sid']
	user = get_redis_server().hget("sockets",source)
	if source and not user:
		await sio.emit("transfer",build_response("get_platform_data",source,"Not connected"))
		#await sio.emit("send_to_client",{"event":"get_platform_data","to":source,"data":"Not connected"})
		#await sio.emit("transfer",{"event":"get_platform_data","to":source,"data":"Not connected"})
		return
	user = cstr(user)
	alerts = frappe.db.sql(""" select name,user,creation, enabled, filters_script, symbol, triggered, notify_by_email from `tabPrice Alert` where user='%s'""" % (user),as_dict=True)
	extras = frappe.db.get_single_value('Candlescan Settings', 'extras')
	scanners = frappe.db.sql(""" select default_config,title,description,active,scanner_id,scanner,method from `tabCandlescan scanner` """,as_dict=True)
	customScanners = frappe.db.sql(""" select title,scanner,name,user,config,target from `tabCustom Scanner` where user='%s' """ % (user),as_dict=True)
	layouts = frappe.db.sql(""" select title,name,config  from `tabLayout` where user='%s' """ % (user),as_dict=True)
	watchlists = frappe.db.sql(""" select name,watchlist,symbols from `tabWatchlist` where user='%s' """ % (user),as_dict=True)
	filters = frappe.db.sql(""" select sound,notify_all,name,filters,refresh,columns,title,script,sort_field,sort_mode from `tabStock Filter` where user='%s' """ % (user),as_dict=True)
	fExtras = []
	if extras:
		extras = extras.splitlines()
	for ex in extras:
		name, label, value_type,doctype = ex.split(':')
		fExtras.append({"field":name,"header":label,"value_type":value_type,"doctype":doctype})
	for scanner in scanners:
		signautre_method = "%s.signature" % scanner.method
		config_method = "%s.get_config" % scanner.method
		signature = frappe.call(signautre_method, **frappe.form_dict)
		config = frappe.call(config_method, **frappe.form_dict)
		scanner['signature'] = signature
		scanner['config'] = config

	res = handle(True,"Success",{"filters":filters,"layouts":layouts,"scanners":scanners,"extras":fExtras,"alerts":alerts,"customScanners":customScanners,"watchlists":watchlists})
	#await sio.emit("transfer",{"event":"get_platform_data","to":source,"data":res})
	#await sio.emit("send_to_client",{"event":"get_platform_data","to":source,"data":res})
	await sio.emit("transfer",build_response("get_platform_data",source,res))
	

async def get_symbol_info(message):
	symbol = message.get("data")
	source = message.get('source_sid')
	
	if not (symbol and source):
		return

	# return data fields
	fields =  ' ,'.join(["name","stock_summary_detail","key_statistics_data","key_price_data","key_summary_data","website","summary","industry_type","company","country","floating_shares","sector","exchange"])
	data = frappe.db.sql(""" select {0} from tabSymbol where symbol='{1}' limit 1 """.format(fields,symbol),as_dict=True)
	if(data and len(data)>0):
		await sio.emit("transfer",build_response("get_symbol_info",source,data))


@sio.event
async def get_extra_data(message):
	source = message['source_sid']
	data = message.get("data")
	symbols = data.get("symbols")
	fields = data.get("fields")
	
	if not (symbols or fields):
		return
	sql_fields =  ' ,'.join(fields)
	sql_symbols =  ', '.join(['%s']*len(symbols))
	sql = """select symbol,{0} from tabSymbol where name in ({1})""".format(sql_fields,sql_symbols)
	frappe.db.commit()
	result = frappe.db.sql(sql,tuple(symbols),as_dict=True)
	#print("result",result)
	
	await sio.emit("transfer",build_response("get_extra_data",source,result))


async def get_last_broadcast(message):
	scanner = message.get("data")
	source = message.get('source_sid')
	
	if not (symbol and scanner):
		return
	frappe.db.commit()
	raw_state = frappe.db.get_value(scanner,None,"state")
	if raw_state:
		data = json.loads(raw_state)
		await sio.emit("transfer",build_response("get_last_broadcast",source,data))

def check_symbol(user,symbol):
    #logged_in()
    if not (user or symbol):
        return handle(False,"User is required")
    exists =  frappe.db.exists("Symbol", symbol)
    return handle(True,"Success",{"exists":exists})
        
@frappe.whitelist()        
def delete_custom_scanner(user,name):
    logged_in()
    if not (user or name):
        return handle(False,"User is required")
    frappe.delete_doc('Custom Scanner', name)
    return handle(True,"Success")
    

@frappe.whitelist()        
def get_historical(user,doctype,date,feed_type):
    logged_in()
    if not (user or doctype or date):
        return handle(False,"Data missing")
    lmt = 1 if feed_type == "list" else 20
        
    values = frappe.db.sql(""" select creation,data from `tabVersion` where ref_doctype='%s' and creation<='%s' order by creation DESC limit %s""" % (doctype,date,lmt),as_dict=True)
    if values:
        values = values[0]
        odata = json.loads(values.data)
        state = json.loads(odata['changed'][0][1])
        if state:
            resp = {"version":values.creation,"data":state}
            return handle(True,"Success",resp)
    return handle(True,"Success")
    
@sio.event
async def get_select_values(message):
	doctype = message.get("data")
	source = message.get("source_sid")
	if not doctype:
		return
	values = frappe.db.sql(""" select name from `tab%s` limit 100""" % doctype,as_dict=True)
	values = [a['name'] for a in values]
	print("values",values)
	await sio.emit("transfer",build_response("get_select_values",source,values))
    



@frappe.whitelist()        
def activate_alert(user,name):
    logged_in()
    if not (user or name):
        return handle(False,"Missing data")
    frappe.db.set_value('Price Alert', name, "triggered", 0)
    return handle(True,"Success")

@frappe.whitelist()        
def toggle_alert(user,name,enabled):
    logged_in()
    if not (user or name):
        return handle(False,"Missing data")
    frappe.db.set_value('Price Alert', name, "enabled", enabled)
    return handle(True,"Success")


@frappe.whitelist()        
def get_calendar(target):
    logged_in()
    if not target:
        return handle(False,"Missing data")
    calendar = frappe.db.get_value("Fundamentals",None,target)
    return handle(True,"Success",calendar)
        


