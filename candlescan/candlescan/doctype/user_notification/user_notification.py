# -*- coding: utf-8 -*-
# Copyright (c) 2021, ovresko and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe.model.document import Document

class UserNotification(Document):
	def after_insert(self):
		frappe.db.sql("""DELETE from `tabUser Notification` where user='%s' and message='%s'""" % (self.user,self.message))
		frappe.db.commit()
