import frappe,json
from frappe.realtime import get_redis_server
from candlescan.candlescan_api import handle
from frappe.utils import cstr
import asyncio
import socketio

sio = socketio.AsyncClient(reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)

	
async def run():
	#try:
		await sio.connect('http://localhost:9002',auth={"microservice":"platform"})
		await sio.emit("join", "platform")
		await sio.wait()
	#except socketio.exceptions.ConnectionError as err:
	#	await sio.sleep(5)
	#	run()

@sio.event
async def get_platform_data(data):
	source = data['from']
	user = get_redis_server().hget(source)
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

