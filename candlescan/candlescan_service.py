from __future__ import unicode_literals
import frappe, random
from frappe.model.document import Document
from frappe.realtime import get_redis_server
from frappe.utils.background_jobs import enqueue,get_redis_conn,get_jobs,enqueue_doc
from rq.job import Job
from rq.registry import StartedJobRegistry

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
        

@frappe.whitelist()
def start_scanners():
    scanners = frappe.db.sql(""" select name,active,scanner_id,scanner,method from `tabCandlescan scanner` """,as_dict=True)
    for s in scanners:
        if s.active:
            method = "%s.start" % s.method
            frappe.cache().hset(s.scanner_id,"stop",0,shared=True)
            q = enqueue(method,queue=s.scanner_id, timeout=60000, job_name=s.scanner_id,scanner_id=s.scanner_id)
        else:
            frappe.cache().hset(s.scanner_id,"stop",1,shared=True)
