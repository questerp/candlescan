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
        {"name":"symbol","title":"Symbol","align":"left","value_type":"string"},
        {"name":"gap","title":"Gap %","align":"left","value_type":"number"},
        {"name":"price","title":"Price","align":"right","value_type":"number"},
        ]

def start(scanner_id):        
    redis = get_redis_server()
    val = 1
    symbols = frappe.db.sql("""select name from tabSymbol""",as_dict=True)
    #doc = frappe.get_doc("Premarket Scanner")
    while(True):
        frappe.local.cache = {}
        stop = frappe.cache().hget(scanner_id,"stop",shared=True)
        if stop == 1:
            break
        val=val+1 
        time.sleep(2)
        #rsymb = ''.join(random.choice('AZFQDFEZEF') for _ in range(3))
        rprice = random.randrange(2, 100)
        rvol = random.randrange(10, 99)
        redis.publish("candlesocket",frappe.as_json({"scanner_id":scanner_id,"data":[
                                                        {"symbol":symbols[0].name,"price":rprice,"gap":rvol},
                                                        {"symbol":symbols[1].name,"price":rprice,"gap":rvol}
                                                     ]}))
