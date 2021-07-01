# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe.model.document import Document
from frappe.realtime import get_redis_server
from frappe.utils.background_jobs import enqueue,get_redis_conn
from rq.command import send_stop_job_command


class CandlescanSettings(Document):
    def on_update(self):
        self.start_scanners()

    def start_scanners(self):
        if self.scanners:
            redis = get_redis_conn()
            for s in self.scanners:
                scanner = frappe.get_doc(s.scanner)
                print(scanner.job_id)
                if not scanner.job_id:
                    continue
                send_stop_job_command(redis, scanner.job_id)
                if scanner.active:
                    enqueue(scanner.start, queue='background', job_name=scanner.job_id, job_id=scanner.job_id)
