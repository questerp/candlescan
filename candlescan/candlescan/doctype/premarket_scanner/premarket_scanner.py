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

def signature():
    return [
        {"symbol":"Symbol"},
        {"olume":"Volume"},
        {"rice":"Price"}
        ]

def start(scanner_id):        
    redis = get_redis_server()
    val = 1
    #doc = frappe.get_doc("Premarket Scanner")
    while(True):
        frappe.local.cache = {}
        stop = frappe.cache().hget(scanner_id,"stop",shared=True)
        if stop == 1:
            break
        val=val+1 
        time.sleep(2)
        redis.publish("candlesocket",frappe.as_json({"scanner_id":scanner_id,"data":[
                                                     {"symbol":"AAPL","price":5412.25,"volume":125158574},
                                                     {"symbol":"TSLA","price":43.2,"volume":342322332},
                                                     {"symbol":"AMD","price":11.31,"volume":232411432},
                                                     ]}))
