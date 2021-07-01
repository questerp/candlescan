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
    scanners = frappe.db.sql(""" select name,active,scanner_id,scanner,method from `tabCandlescan scanner` """,as_dict=True)
    for s in scanners:
        if s.active:
            frappe.cache().hset(s.scanner_id,"stop",0,shared=True)
            q = enqueue(s.method,queue='default', job_name=s.scanner_id,scanner_id=s.scanner_id)
        else:
            frappe.cache().hset(s.scanner_id,"stop",1,shared=True)
