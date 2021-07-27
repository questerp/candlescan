from __future__ import unicode_literals
import frappe, random, json
from frappe.model.document import Document
from frappe.realtime import get_redis_server
from frappe.utils.background_jobs import enqueue,get_redis_conn,get_jobs,enqueue_doc,execute_job
from rq.job import Job
import time
from frappe.utils import cstr
from rq.registry import StartedJobRegistry
from rq import Connection, Queue, Worker
from candlescan import handle
import asyncio
import threading
import candlescan.utils.yahoo_finance_api2 as yf
from frappe.utils import cint,now_datetime



def clear_user_notifications():
    frappe.db.sql("""delete from `tabUser Notification` where user IS NOT NULL """)
    frappe.db.commit()


def save_scanner_state(data):
    if data:
        state = json.dumps(data)
        result = frappe.get_doc({
            "doctype":"Scanner Result,
            "date": now_datetime(),
            "state":state
            }).insert()
    
def insert_symbol(symbol):
    symbol.flags.ignore_links = True
    symbol.flags.ignore_validate = True
    symbol.flags.ignore_permissions  = True
    symbol.flags.ignore_mandatory = True
    symbol.insert()
    frappe.db.commit()
	

def generate_customer_codes(customer,method):
    if not customer:
        return
    if not customer.confirm:
        customer.confirm = random.randrange(1000,99999)
    if not customer.referral:
        customer.referral = frappe.generate_hash(length=10)
    if not customer.user_key:
        customer.user_key = frappe.generate_hash(length=15)
    
    customer.save()	

	
def get_yahoo_prices(symbol,period_type, period, frequency_type, frequency):
	if not (symbol or period_type or period or frequency_type or frequency):
		return
	
	share = yf.Share(symbol)
	period = cint(period)
	frequency = cint(frequency)
	data = share.get_historical(period_type,period,frequency_type,frequency,"dict")
	data = [a for a in data if a['low'] and a['low']>0]
	return data
