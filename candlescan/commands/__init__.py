
from __future__ import unicode_literals, absolute_import, print_function
import click
import frappe
from frappe.commands import pass_context, get_site

def call_command(cmd, context):
	return click.Context(cmd, obj=context).forward(cmd)

@click.command('start-candlescan-workers')
def start_candlescan_workers():
	print("Starting Candlescan Workers")
	from candlescan.candlescan_service import start_workers
	start_workers()

commands = [
	start_candlescan_workers
]
