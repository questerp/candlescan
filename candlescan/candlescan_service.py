from __future__ import unicode_literals
import frappe, random
from frappe.model.document import Document
from frappe.realtime import get_redis_server
from frappe.utils.background_jobs import enqueue,get_redis_conn,get_jobs,enqueue_doc,execute_job
from rq.job import Job
import time
from frappe.utils import cstr
from rq.registry import StartedJobRegistry
from rq import Connection, Queue, Worker
from candlescan.candlescan_api import handle

def get_last_broadcast(doctype,scanner_id):
    if not (doctype or scanner_id):
        return
    raw_state = frappe.db.get_value(doctype,"state")
    if raw_state:
        data = json.loads(raw_state)
        parsed_data = frappe.as_json({"scanner_id":scanner_id,"data":data})
        return handle(True,'Success',parsed_data)
    return handle(True,'Success')

def broadcast(doctype,scanner_id,interval,data):
    redis = get_redis_server()
    parsed_data = frappe.as_json({"scanner_id":scanner_id,"data":data})
    if doctype:
        frappe.db.set_value(doctype,"state",parsed_data,update_modified=False)
    
    redis.publish("candlescan_all",parsed_data))
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
    from candlescan.candlescan.doctype.candlescan_extras_manager.candlescan_extras_manager import process_extras
    from candlescan.candlescan.doctype.candlescan_alert_manager.candlescan_alert_manager import process_alerts
    redis_connection = get_redis_conn()
    kwargs = {
        'connection': redis_connection,
        'async': True,
    }
    q = Queue("process_extras", **kwargs)
    queue_args = {
        "site": frappe.local.site,
        "user": None,
        "method": process_extras,
        "event": None,
        "job_name": cstr(process_extras),
        "is_async": True,
        "kwargs": {}
    }
    q.enqueue_call(execute_job, timeout=-1,	kwargs=queue_args)
    
    q = Queue("process_alerts", **kwargs)
    queue_args = {
        "site": frappe.local.site,
        "user": None,
        "method": process_alerts,
        "event": None,
        "job_name": cstr(process_alerts),
        "is_async": True,
        "kwargs": {}
    }
    q.enqueue_call(execute_job, timeout=-1,	kwargs=queue_args)
    

@frappe.whitelist()
def start_scanners():
    redis_connection = get_redis_conn()
    scanners = frappe.db.sql(""" select name,active,scanner_id,scanner,method from `tabCandlescan scanner` """,as_dict=True)
    for s in scanners:
        if s.active:
            q = Queue(s.scanner_id, connection=redis_connection)
            workers = Worker.all(queue=q)
            if q.job_ids:
                return
            if workers:
                worker = workers[0]
                if worker and (worker.state == "started" or worker.state == "busy"):
                    return
                    
            method = "%s.start" % s.method
            frappe.cache().hset(s.scanner_id,"stop",0,shared=True)
            #workers = Worker.all(connection=redis_connection)
            #if not workers or (method not in [w.name for w in workers]):
            #    with Connection(redis_connection):
            #       logging_level = "INFO"
            #        print("Starting worker %s" % method)
            #       Worker([s.scanner_id], name=s.scanner_id).work(logging_level = logging_level)

            kwargs = {
                'connection': redis_connection,
                'async': True,
                'scanner_id':s.scanner_id
            }
            #print("Starting Queue for the worker")
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
            #q = enqueue(method,queue="default", timeout=60000, job_name=s.scanner_id,scanner_id=s.scanner_id)
        else:
            q = Queue(s.scanner_id, connection=redis_connection)
            q.empty()
            q.delete(delete_jobs=True)
            frappe.cache().hset(s.scanner_id,"stop",1,shared=True)
