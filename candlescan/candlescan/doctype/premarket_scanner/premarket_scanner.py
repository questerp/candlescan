# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
import time
from frappe.utils.background_jobs import enqueue_doc
from frappe.model.document import Document
from frappe.realtime import get_redis_server

class PremarketScanner(Document):
    def on_update(self):
        #if self.active:
            #enqueue_doc(self.doctype,self.name,"start_scanner",is_async=False)
            #frappe.msgprint(job.id)
        frappe.db.set_value("Premarket Scanner",None,"active",self.active)
        enqueue_doc(self.doctype,self.name,"start_scanner",is_async=False)
    
    def start_scanner(self):        
        if self.active:
            redis = get_redis_server()
            val = 1
            active = 1
            while(True):
                if (val % 10) == 0:
                    active = frappe.db.get_value("Premarket Scanner",self.name,"active")
                if not active:
                    break
                val=val+1 
                time.sleep(self.update_ms/1000)
                redis.publish("candlesocket",frappe.as_json({"scanner":"premarket","title":self.public_name,"data":"%s %s"% (active,val)}))

