# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe, random
import time
from frappe.utils.background_jobs import enqueue_doc
from frappe.model.document import Document
from frappe.realtime import get_redis_server

class PremarketScanner(Document):
    pass

def signature():
    return [
        {"name":"symbol","title":"Symbol","align":"left"},
        {"name":"volume","title":"Volume","align":"left"},
        {"name":"price","title":"Price","align":"right"},
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
        rsymb = ''.join(random.choice('AZFQDFEZEF') for _ in range(3))
        rprice = random.randrange(2, 100)
        rvol = random.randrange(10000000, 90000000)
        redis.publish("candlesocket",frappe.as_json({"scanner_id":scanner_id,"data":[
                                                     {"symbol":rsymb,"price":rprice,"volume":rvol}
                                                     ]}))
