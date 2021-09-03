from __future__ import unicode_literals
import frappe, random, json
from frappe.model.document import Document
from frappe.realtime import get_redis_server
from frappe.utils.background_jobs import enqueue,get_redis_conn,get_jobs,enqueue_doc,execute_job
from rq.job import Job
import time
from frappe.utils import cstr,add_days, get_datetime
from rq.registry import StartedJobRegistry
from rq import Connection, Queue, Worker
from candlescan import handle
import asyncio
import threading
import candlescan.utils.yahoo_finance_api2 as yf
from frappe.utils import cint,now_datetime
from datetime import timedelta,datetime as dt
from pytz import timezone	
_timezone = timezone("America/New_York")

active_symbols = []

def get_active_symbols(reload=False):
    global active_symbols
    if reload or not active_symbols:
        s = frappe.db.sql(""" select symbol from tabIndicators  order by M_VOLUME desc""",as_list=True)
        active_symbols = [a[0] for a in s]
        print("top 1m volume",active_symbols[0])
    return active_symbols

def clear_active_symbols():
    global active_symbols
    #active_symbols = []
    get_active_symbols(reload=True)


def to_candle(data,symbol=None):
    # if not symbol and not data.get("s"):
    #     frappe.throw("Symbol is required")

    # if not data.get('t'):
    #     frappe.throw("Time is required")

    return {
        'ticker':  data.get("s")  or symbol,
       # 'time': data.get("t"),
        'timestamp': data.get("t"),
        'open': float(data.get("o") or 0),
        'close': float(data.get("c") or 0),
        'high': float(data.get("h") or 0),
        'low': float(data.get("l") or 0),
        'volume': data.get("v") or 0,
        'trades': data.get("n") or 0
    }


def clear_user_notifications():
    frappe.db.sql("""delete from `tabUser Notification` where user IS NOT NULL """)
    frappe.db.commit()


def save_scanner_state(scanner_id,data):
    if data and scanner_id:
        state = json.dumps(data)
        result = frappe.get_doc({
            "doctype":"Scanner Result",
            "scanner":scanner_id,
            "date": now_datetime(),
            "state":state
            }).insert()
        frappe.db.commit()
    
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
