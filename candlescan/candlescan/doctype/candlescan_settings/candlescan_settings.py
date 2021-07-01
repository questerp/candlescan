# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe.model.document import Document
from frappe.realtime import get_redis_server
from frappe.utils.background_jobs import enqueue,get_redis_conn,get_jobs,enqueue_doc
from rq.job import Job
from rq.registry import StartedJobRegistry


class CandlescanSettings(Document):
    pass

@frappe.whitelist()
def start_scanners():
    scanners = frappe.db.sql(""" select name,active,job_id,scanner,method from `tabCandlescan scanner` """,as_dict=True)
    result = {}
    for s in scanners:
        cache_id = 'stop_%s' % s.job_id
        result["cache_id"] = cache_id
        result[s.scanner] = s.active
        if s.active:
            print("Starting %s" % s.scanner)
            frappe.cache().hset(s.scanner,"stop",0,shared=True)
            #enqueue_doc(s.scanner, name=s.scanner, method="start", queue='long')
            q = enqueue(s.method,queue='default', job_name=s.scanner)
            #id = q.get_id()
            #frappe.db.sql("""UPDATE `tabCandlescan scanner` set job_id='%s' where name='%s'""" % (id,s.name))
        else:
            print("Stopping %s" % s.scanner)
            frappe.cache().hset(s.scanner,"stop",1,shared=True)
            
    result["cache"] = frappe.cache().hget(s.scanner,"stop",shared=True)
    return result

                
