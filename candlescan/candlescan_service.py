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


def clear_user_notifications():
    frappe.db.sql("""delete from `tabUser Notification` where user IS NOT NULL """)
    frappe.db.commit()
    
def start_microservices():
    asyncio.get_event_loop().run_until_complete(_start_microservices())
    
async def _start_microservices():
    from candlescan.platform import run as run_platform
    from candlescan.broadcaster import run as run_broadcaster
    runs = await asyncio.gather(
        run_platform(),
        run_broadcaster()
    )
    asyncio.get_event_loop().run_until_complete(runs)

    
def insert_symbol(symbol):
    symbol.flags.ignore_links = True
    symbol.flags.ignore_validate = True
    symbol.flags.ignore_permissions  = True
    symbol.flags.ignore_mandatory = True
    symbol.insert()
    frappe.db.commit()

def get_last_broadcast(doctype):
    if not doctype:
        return handle(False,'No doctype')
    raw_state = frappe.db.get_value(doctype,None,"state")
    if raw_state:
        data = json.loads(raw_state)
        #parsed_data = frappe.as_json({"scanner_id":scanner_id,"data":data})
        if data:
            return handle(True,'Success',data)
    return handle(True,'No history for %s' % doctype)

def broadcast(doctype,scanner_id,interval,data):
    redis = get_redis_server()
    parsed_data = frappe.as_json({"scanner_id":scanner_id,"data":data})
    if doctype and data:
        doc = frappe.get_doc(doctype)
        doc.state=parsed_data
        doc.save(ignore_permissions=1)
        #frappe.db.set_value(doctype,None,"state",parsed_data,update_modified=False)
        frappe.db.commit()
        
    redis.publish("candlescan_all",parsed_data)
    if interval and interval > 0:
        time.sleep(interval)

def after_signup(customer,method):
    if not customer:
        return
    if not customer.confirm:
        customer.confirm = random.randrange(1000,99999)
    if not customer.referral:
        customer.referral = frappe.generate_hash(length=10)
    if not customer.user_key:
        customer.user_key = frappe.generate_hash(length=15)
    
    customer.save()
        
def start_workers(queue):
    #scanners = frappe.db.sql(""" select scanner_id,method from `tabCandlescan scanner` """,as_dict=True)
    with frappe.init_site():
        redis_connection = get_redis_conn()
    with Connection(redis_connection):
        logging_level = "INFO"
        print("Starting worker %s" % queue)
        Worker([queue], name=queue).work(logging_level = logging_level)

@frappe.whitelist()
def start_services():
    from candlescan.worker_technicals import process as worker_technicals
    from candlescan.worker_alerts import process as worker_alerts
    
    redis_connection = get_redis_conn()
    kwargs = {
        'connection': redis_connection,
        'async': True,
    }
    q = Queue("worker_alerts", **kwargs)
    queue_args = {
        "site": frappe.local.site,
        "user": None,
        "method": "candlescan.worker_alerts.process",
        "event": None,
        "job_name": "worker_alerts",
        "is_async": True,
        "kwargs": {}
    }
    q.enqueue_call(execute_job, timeout=-1,	kwargs=queue_args)
    
    q = Queue("worker_technicals", **kwargs)
    queue_args = {
        "site": frappe.local.site,
        "user": None,
        "method": "candlescan.worker_technicals.process",
        "event": None,
        "job_name": "worker_technicals",
        "is_async": True,
        "kwargs": {}
    }
    q.enqueue_call(execute_job, timeout=-1,	kwargs=queue_args)
    

@frappe.whitelist()
def start_scanners():
    redis_connection = get_redis_conn()
    scanners = frappe.db.sql(""" select name,scanner_id,scanner,method from `tabCandlescan scanner` """,as_dict=True)
    for s in scanners:
        method = "%s.start" % s.method
        kwargs = {
            'connection': redis_connection,
            'async': True,
            'scanner_id':s.scanner_id
        }
        q = Queue(s.scanner_id, **kwargs)
        queue_args = {
            "site": frappe.local.site,
            "user": None,
            "method": method,
            "event": None,
            "job_name": cstr(method),
            "is_async": True,
            "kwargs": {
                 'scanner_id':s.scanner_id
            }
        }
        q.enqueue_call(execute_job, timeout=-1,	kwargs=queue_args)
           
