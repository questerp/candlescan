# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe.model.document import Document
from frappe.realtime import get_redis_server
from frappe.utils.background_jobs import enqueue,get_redis_conn,get_jobs,enqueue_doc


class CandlescanSettings(Document):
    def on_update(self):
        self.start_scanners()

    def start_scanners(self):
        frappe.msgprint("start_scanners")
        redis = get_redis_conn()
        jobs = get_jobs(queue="long")
        for job in jobs:
            print("job: %s" % job)
            
        scanners = frappe.db.sql(""" select name,active,job_id,scanner,method from `tabCandlescan scanner` """,as_dict=True)
        for s in scanners:
            print("scanner: %s" % s.scanner)
            print("active: %s" % s.active)
            print(s.job_id)
            if s.active:
                print("Starting")
                #enqueue_doc(s.scanner, name=s.scanner, method="start", queue='long')
                q = enqueue(s.method, queue='long', job_name=s.job_id, job_id=s.job_id)
                print(q)
                
