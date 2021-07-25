import frappe,json
from frappe.realtime import get_redis_server
from candlescan.candlescan_api import handle
from candlescan.socket_utils import get_user,validate_data,build_response
from frappe.utils import cstr
import socketio
import asyncio

sio = socketio.AsyncClient(logger=True, engineio_logger=True,reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)

		
async def run():
	try:
		await sio.connect('http://localhost:9002',headers={"microservice":"platform"})
		await sio.sleep(2)
		await sio.emit("join", "platform")
		await sio.wait()
	except socketio.exceptions.ConnectionError as err:
		print("error",sio.sid,err)
		#await sio.sleep(5)
		#await run()

@sio.event
async def connect_error(message):
    print('Connection was rejected due to ' + message)

@sio.event
async def connect():
    print("I'm connected!")

@sio.event
async def disconnect():
    print("I'm disconnected!")

@sio.event
async def ressource(data):
	validated = validate_data(data,["source_id","doctype","method"])
	source_sid = data.get('source_sid')
	if not (validated or source_sid):
		await sio.emit("send_to_client",build_response("ressource",source_sid,"Invalid data format"))
		return
			       
	user = get_user(source_sid)
	doctype = data.get("doctype")
	name = data.get("name")
	method = data.get("method")
	document = data.get("doc")
	
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
				await sio.emit("send_to_client",build_response("ressource",source_sid,response))

		else:
			document.update({"doctype": doctype})
			response = frappe.get_doc(data).insert()
			if response:
				await sio.emit("send_to_client",build_response("ressource",source_sid,response))

	if method == "delete" and name:
		frappe.delete_doc(doctype, name, ignore_missing=False)
		await sio.emit("send_to_client",build_response("ressource",source_sid,"Deleted"))


	if method == "list":
		response = frappe.get_all(doctype,filters={"user":user},fields=["*"])
		await sio.emit("send_to_client",build_response("ressource",source_sid,response))

			       
@sio.event
async def get_platform_data(data):
	source = data['source_sid']
	user = get_redis_server().hget("sockets",source)
	if source and not user:
		await sio.emit("send_to_client",{"event":"get_platform_data","to":source,"data":"Not connected"})
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
	await sio.emit("send_to_client",{"event":"get_platform_data","to":source,"data":res})

@frappe.whitelist()
def get_symbol_info(symbol):
    logged_in()
    if not symbol:
        return handle(False,"Data missing")
    
    # return data fields
    fields =  ' ,'.join(["name","stock_summary_detail","key_statistics_data","key_price_data","key_summary_data","website","summary","industry_type","company","country","floating_shares","sector","exchange"])
    data = frappe.db.sql(""" select {0} from tabSymbol where symbol='{1}' limit 1 """.format(fields,symbol),as_dict=True)
    if(data and len(data)>0):
        return handle(True,"Success",data[0])
    return handle(False,"Symbol not found")


@frappe.whitelist()
def get_extra_data(symbols,fields):
    logged_in()
    if not (symbols or fields):
        return handle(False,"Data missing")

    sql_fields =  ' ,'.join(fields)
    sql_symbols =  ', '.join(['%s']*len(symbols))
    sql = """select name,{0} from tabSymbol where name in ({1})""".format(sql_fields,sql_symbols)
    result = frappe.db.sql(sql,tuple(symbols),as_dict=True)
    return handle(True,"Success",result)


@frappe.whitelist()        
def last_broadcast(user,scanner):
    logged_in()
    if not (user or scanner):
        return handle(False,"User is required")
    #doctype =  frappe.db.get_value("Candlescan scanner", {"scanner_id":scanner_id},"scanner")
    return get_last_broadcast(scanner)
       
@frappe.whitelist()        
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
    
@frappe.whitelist()        
def get_select_values(doctype):
    logged_in()
    if not doctype:
        return handle(Flase,"Data required")
    values = frappe.db.sql(""" select name from `tab%s` limit 100""" % doctype,as_dict=True)
    values = [a['name'] for a in values]
    return handle(True,"Success",values)


@frappe.whitelist()        
def delete_watchlist(user,watchlist_id):
    logged_in()
    if not (user or watchlist_id):
        return handle(Flase,"User is required")
    frappe.delete_doc('Watchlist', watchlist_id)
    return handle(True,"Success")
  

@frappe.whitelist()        
def delete_layout(user,name):
    logged_in()
    if not (user or name):
        return handle(Flase,"User is required")
    frappe.delete_doc('Layout', name)
    return handle(True,"Success")
    
@frappe.whitelist()        
def save_layout(user,title,config,name=None):
    logged_in()
    if not (user or title):
        return handle(Flase,"User is required")
    if not name:
        w = frappe.get_doc({
            'doctype':'Layout',
            'title':title,
            'config':config,
            'user':user,
        })
        w = w.insert()
        return handle(True,"Success",w)
        
    else:
        frappe.db.set_value("Layout",name,"config",config)
        frappe.db.set_value("Layout",name,"title",title)
        w = frappe.get_doc("Layout",name)
        return handle(True,"Success",w)
        
@frappe.whitelist()        
def save_watchlist(user,watchlist,symbols='',name=None):
    logged_in()
    if not (user or watchlist):
        return handle(Flase,"User is required")
    if not name:
        w = frappe.get_doc({
            'doctype':'Watchlist',
            'watchlist':watchlist,
            'user':user,
        })
        w = w.insert()
        return handle(True,"Success",w)
        
    else:
        frappe.db.set_value("Watchlist",name,"watchlist",watchlist)
        frappe.db.set_value("Watchlist",name,"symbols",symbols)
        w = frappe.get_doc("Watchlist",name)
        return handle(True,"Success",w)
    
    
@frappe.whitelist()        
def save_customer_scanner(user,config,scanner,title,target,name=None):
    logged_in()
    if not (user or config or title or target or scanner):
        return handle(Flase,"User is required")
    scanner_obj = None
    if name:
        scanner_obj = frappe.get_doc("Custom Scanner",name)
    else:
        scanner_obj = frappe.get_doc({
            'doctype':"Custom Scanner",
            'user': user,
            'target':target
        })
        
    scanner_obj.title = title
    scanner_obj.config = config
    scanner_obj.scanner = scanner
    
    if name:
        scanner_obj = scanner_obj.save()
    else:
        scanner_obj = scanner_obj.insert()
        
    return handle(True,"Success",scanner_obj)
        
        
@frappe.whitelist()        
def update_socket(user,socket_id):
    logged_in()
    if not (user or socket_id):
        return handle(Flase,"User is required")
    frappe.db.set_value("Customer",user,"socket_id",socket_id)
    return handle(True,"Success")

@frappe.whitelist()        
def get_alerts(user):
    logged_in()
    if not user:
        return handle(Flase,"User is required")
    alerts = frappe.db.sql(""" select name,user,creation, enabled, filters_script, symbol, triggered, notify_by_email from `tabPrice Alert` where user='%s'""" % (user),as_dict=True)
    return handle(True,"Success",alerts)
    


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
def delete_alert(user,name):
    logged_in()
    if not (user or name):
        return handle(False,"Missing data")
    frappe.delete_doc('Price Alert', name)
    return handle(True,"Success")

       
@frappe.whitelist()        
def get_calendar(target):
    logged_in()
    if not target:
        return handle(False,"Missing data")
    calendar = frappe.db.get_value("Fundamentals",None,target)
    return handle(True,"Success",calendar)
        

@frappe.whitelist()     
def delete_stock_filter(user,name):
    logged_in()
    if not (user or name):
        return handle(False,"Missing data")
    frappe.delete_doc('Stock Filter', name)
    return handle(True,"Success")

@frappe.whitelist()     
def save_stock_filter(user,title,sort_field,sort_mode,script,name=None):
    logged_in()
    if not (user or title or sort_field or sort_mode or script):
        return handle(False,"Missing data")
    script = json.dumps(script)
    if not name:
        filter = frappe.get_doc({
                    'doctype':'Stock Filter',
                    'user': user,
                    'title': title,
                    'sort_field': sort_field,
                    'sort_mode': sort_mode,
                    'script': script
                })
        filter = filter.insert()
        return handle(True,"Success",filter)
    else:
        f = frappe.get_doc("Stock Filter",name)
        if f:
            f.title = title
            f.sort_field=sort_field
            f.sort_mode=sort_mode
            f.script=script
            f = f.save()
            return handle(True,"Success",f)
        else:
            return handle(False,"Failed! filter doesn't exist")
            
            

@frappe.whitelist()     
def delete_filter(user,filter):
    logged_in()
    if not (user or filter):
        return handle(False,"Missing data")        
    
    

@frappe.whitelist()     
def set_default_layout(user,layout):
    logged_in()
    if not (user or layout):
        return handle(False,"Missing data")   
    frappe.db.set_value("Customer",user,"default_layout",layout)
    return handle(True,"Success")
    

            
@frappe.whitelist()     
def get_symbol_prices(user,symbol,period_type, period, frequency_type, frequency):
    #logged_in()
    if not (user or symbol or period_type or period or frequency_type or frequency):
        frappe.throw("Missing data")         
        
    data = get_prices(symbol,period_type, period, frequency_type, frequency)
    return handle(True,"Success",data)
    
    
@frappe.whitelist()     
def get_notifications(user):
    logged_in()
    if not user:
        frappe.throw("Missing data")         
    notifs = frappe.get_all("User Notification", fields=['user','message','creation'],filters={"user":user})
    return handle(True,"Success",notifs)
    
@frappe.whitelist()     
def run_stock_filter(user,name):
    logged_in()
    if not (user or name):
        frappe.throw("Missing data")
    filter = frappe.get_doc("Stock Filter",name)
    if filter.sql_script:
        sql = json.loads(filter.sql_script)
        sort = "ASC" if filter.sort_mode == "Ascending" else "DESC"
        if sql:
            data = frappe.db.sql("""%s order by %s %s limit 100""" % (sql,filter.sort_field,sort),as_dict=True)
            return handle(True,"Success",data)
    frappe.throw("Can't execute filter, contact support")

#@frappe.whitelist()        
#def add_alert(user,symbol,filters,notify):
#    logged_in()
#    if not (user or symbol):
#        return handle(False,"No user found")
#    
#    #res = []
#    #for f in filters:
#    #    r = {f['field']:[f['operator'],f['value']]}
#    #    res.append(r)
#    #fs = json.dumps(res)
#    alert = frappe.get_doc({
#        'doctype': 'Price Alert',
#        'user': user,
#        'triggered':False,
#        'enabled':True,
#        'symbol':symbol,
#        'filters_script':filters,
#        'notify_by_email':notify
#    })
#    c = alert.insert()
#    return handle(True,"Success",c)
