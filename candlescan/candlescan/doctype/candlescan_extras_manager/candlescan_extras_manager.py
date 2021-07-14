# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe, random
import time
from frappe.model.document import Document
from frappe.realtime import get_redis_server

class CandlescanExtrasManager(Document):
	pass

@frappe.whitelist()
def process_extras():
	pass
