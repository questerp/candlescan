# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe.model.document import Document
from frappe.realtime import get_redis_server
from frappe.utils.background_jobs import enqueue,get_redis_conn,get_jobs,enqueue_doc
from rq.job import Job


class CandlescanSettings(Document):
    def on_update(self):
        self.start_scanners()

    def start_scanners(self):
        redis = get_redis_conn()
        #registry = StartedJobRegistry('long', connection=redis)
        #running_job_ids = registry.get_job_ids()  # Jobs which are exactly running. 

        #for job in running_job_ids:
        #    job = Job.fetch(job, connection=redis)
        #    print("job: %s" % job)
        #    print('Status: %s' % job.get_status())
            
            
        scanners = frappe.db.sql(""" select name,active,job_id,scanner,method from `tabCandlescan scanner` """,as_dict=True)
        for s in scanners:
            print("scanner: %s" % s.scanner)
            print("active: %s" % s.job_id)
            if s.job_id:
                job = Job.fetch(s.job_id, connection=redis)
                print('Stopping job, Status: %s' % job.get_status())
                job.cancel()
                job.delete()
                
            if s.active:
                print("Starting")
                #enqueue_doc(s.scanner, name=s.scanner, method="start", queue='long')
                q = enqueue(s.method, queue='long', job_name=s.job_id)
                id = q.get_id()
                print(id)
                frappe.db.sql("""UPDATE set job_id='%s' from `tabCandlescan scanner` where name='%s'""" % (id,s.name))

                
