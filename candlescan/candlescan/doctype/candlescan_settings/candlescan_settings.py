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
    redis = get_redis_conn()
    registry = StartedJobRegistry('long', connection=redis)
    running_job_ids = registry.get_job_ids()  # Jobs which are exactly running. 


    scanners = frappe.db.sql(""" select name,active,job_id,scanner,method from `tabCandlescan scanner` """,as_dict=True)
    for s in scanners:
        if s.job_id:
            if s.job_id in running_job_ids:
                job = Job.fetch(s.job_id, connection=redis)
                job.cancel()
                job.delete()

        if s.active:
            #enqueue_doc(s.scanner, name=s.scanner, method="start", queue='long')
            q = enqueue(s.method, queue='long', job_name=s.job_id)
            id = q.get_id()
            frappe.db.sql("""UPDATE `tabCandlescan scanner` set job_id='%s' where name='%s'""" % (id,s.name))

                
