from __future__ import unicode_literals
import frappe, random
from frappe.model.document import Document
from frappe.realtime import get_redis_server
from frappe.utils.background_jobs import enqueue,get_redis_conn,get_jobs,enqueue_doc,execute_job
from rq.job import Job
from frappe.utils import cstr
from rq.registry import StartedJobRegistry
from rq import Connection, Queue, Worker

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
    redis_connection = get_redis_conn()
    with Connection(redis_connection):
        logging_level = "INFO"
        print("Starting worker %s" % method)
        Worker([queue], name=queue).work(logging_level = logging_level)

@frappe.whitelist()
def start_scanners():
    redis_connection = get_redis_conn()
    scanners = frappe.db.sql(""" select name,active,scanner_id,scanner,method from `tabCandlescan scanner` """,as_dict=True)
    for s in scanners:
        if s.active:
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
                'async': True
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
                "kwargs": {}
            }
            q.enqueue_call(execute_job, timeout=60000,	kwargs=queue_args)
            #q = enqueue(method,queue="default", timeout=60000, job_name=s.scanner_id,scanner_id=s.scanner_id)
        else:
            frappe.cache().hset(s.scanner_id,"stop",1,shared=True)
