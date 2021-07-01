# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe.model.document import Document
from frappe.realtime import get_redis_server
from frappe.utils.background_jobs import enqueue,get_redis_conn,get_jobs


class CandlescanSettings(Document):
    def on_update(self):
        self.start_scanners()

    def start_scanners(self):
        frappe.msgprint("start_scanners")
        redis = get_redis_conn()
        jobs = get_jobs()
        for job in jobs:
            print(job)
            frappe.msgprint(job)
            
        scanners = frappe.db.sql(""" select name,active,job_id,scanner from `tabCandlescan scanner` """,as_dict=True)
        for s in scanners:
            scanner = frappe.get_doc(s.scanner)
            print(s.job_id)
            if s.active:
                enqueue(scanner.start, queue='background', job_name=scanner.job_id, job_id=scanner.job_id)
