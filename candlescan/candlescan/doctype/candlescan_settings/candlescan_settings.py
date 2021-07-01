# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe.model.document import Document
from frappe.realtime import get_redis_server

class CandlescanSettings(Document):
    def on_update(self):
        redis = get_redis_server()
        redis.publish("candlesocket",frappe.as_json({'scanner':'premarket','data':'no data available yet'}))
        #frappe.emit_via_redis('candlesocket',{'message':'candlescan'},'candlescan')
        #print("Testingsocket")

@frappe.whitelist()
def get_scanners(user_id):
    if user_id:
        scanners = self.scanners
        return {"result":True,'data':scanners}
    return {"result":False}


