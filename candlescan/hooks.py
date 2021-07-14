# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from . import __version__ as app_version

app_name = "candlescan"
app_title = "CandleScan"
app_publisher = "ovresko"
app_description = "Market Scanners"
app_icon = "octicon octicon-file-directory"
app_color = "green"
app_email = "ovresko@gmail.com"
app_license = "MIT"

#boot_session = "candlescan.candlescan_service.start_workers"

# Includes in <head>
# ------------------

# include js, css files in header of desk.html
# app_include_css = "/assets/candlescan/css/candlescan.css"
# app_include_js = "/assets/candlescan/js/candlescan.js"

# include js, css files in header of web template
# web_include_css = "/assets/candlescan/css/candlescan.css"
# web_include_js = "/assets/candlescan/js/candlescan.js"

# include js in page
# page_js = {"page" : "public/js/file.js"}

# include js in doctype views
# doctype_js = {"doctype" : "public/js/doctype.js"}
# doctype_list_js = {"doctype" : "public/js/doctype_list.js"}
# doctype_tree_js = {"doctype" : "public/js/doctype_tree.js"}
# doctype_calendar_js = {"doctype" : "public/js/doctype_calendar.js"}

# Home Pages
# ----------

# application home page (will override Website Settings)
# home_page = "login"

# website user home page (by Role)
# role_home_page = {
#	"Role": "home_page"
# }

# Website user home page (by function)
# get_website_user_home_page = "candlescan.utils.get_home_page"

# Generators
# ----------

# automatically create page for each record of this doctype
# website_generators = ["Web Page"]

# Installation
# ------------

# before_install = "candlescan.install.before_install"
# after_install = "candlescan.install.after_install"

# Desk Notifications
# ------------------
# See frappe.core.notifications.get_notification_config

# notification_config = "candlescan.notifications.get_notification_config"

# Permissions
# -----------
# Permissions evaluated in scripted ways

# permission_query_conditions = {
# 	"Event": "frappe.desk.doctype.event.event.get_permission_query_conditions",
# }
#
# has_permission = {
# 	"Event": "frappe.desk.doctype.event.event.has_permission",
# }

# Document Events
# ---------------
# Hook on document methods and events

doc_events = {
 	"Customer": {
 		"after_insert": "candlescan.candlescan_service.after_signup",
	}
 }

# Scheduled Tasks
# ---------------

scheduler_events = {
 	"all": [
		
 	],
 	"daily": [
 	],
 	"hourly": [
		"candlescan.candlescan.doctype.fundamentals.fundamentals.process"
 	],
	"daily_long":[
		"candlescan.task_symbols.process",
		"candlescan.task_calendar.process"
	],
 	"weekly": [
 	],
 	"monthly": [
 	]
 }

# Testing
# -------

# before_tests = "candlescan.install.before_tests"

# Overriding Methods
# ------------------------------
#
# override_whitelisted_methods = {
# 	"frappe.desk.doctype.event.event.get_events": "candlescan.event.get_events"
# }
#
# each overriding function accepts a `data` argument;
# generated from the base implementation of the doctype dashboard,
# along with any modifications made in other Frappe apps
# override_doctype_dashboards = {
# 	"Task": "candlescan.task.get_dashboard_data"
# }

