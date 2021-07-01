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
    pass

def start():        
    redis = get_redis_server()
    val = 1
    doc = frappe.get_doc("Premarket Scanner")
    while(True):
        val=val+1 
        time.sleep(2)
        print(val)
        redis.publish("candlesocket",frappe.as_json({"scanner":"premarket","title":doc.public_name,"data":" %s"% val}))

