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
    for s in scanners:
        if s.active:
            frappe.cache().set_value('stop_%s' % s.job_id,0)
            #enqueue_doc(s.scanner, name=s.scanner, method="start", queue='long')
            q = enqueue(s.method, queue='long', job_name=s.job_id)
            #id = q.get_id()
            #frappe.db.sql("""UPDATE `tabCandlescan scanner` set job_id='%s' where name='%s'""" % (id,s.name))
        else:
            frappe.cache().set_value('stop_%s' % s.job_id, 1)

                
