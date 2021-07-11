# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe, random
import time
from frappe.utils.background_jobs import enqueue_doc
from frappe.model.document import Document
from frappe.realtime import get_redis_server
from candlescan.candlescan_service import broadcast

class PremarketScanner(Document):
    pass

# feed_type : ["list","socket","once"]

def get_config():
	return {
		"feed_type":"socket",
		"can_reorder":False
	}

def signature():
    return [
        {"field":"symbol","header":"Symbol","align":"left","value_type":"string"},
        {"field":"gap","header":"Gap %","align":"left","value_type":"number"}
        ]

def start(scanner_id):        
    redis = get_redis_server()
    interval = 2
    symbols = frappe.db.sql("""select name from tabSymbol where name LIKE %(txt)s   limit 200""",dict(txt='AA%'),as_dict=True)
    #doc = frappe.get_doc("Premarket Scanner")
    while(True):
        active = frappe.db.get_value("Premarket Scanner",None,"active")
        if not active:
            break
        #rsymb = ''.join(random.choice('AZFQDFEZEF') for _ in range(3))
        resuls = []
        for s in symbols:
            resuls.append({"symbol":s.name,"gap":0})
            broadcast("Premarket Scanner",scanner_id,interval,resuls)

