
from __future__ import unicode_literals, absolute_import, print_function
import click
import frappe
from frappe.commands import pass_context, get_site
from candlescan.candlescan_service import start_workers

def call_command(cmd, context):
	return click.Context(cmd, obj=context).forward(cmd)

@click.command('start-candlescan-workers')
def start_workers():
	print("Starting Candlescan Workers")
	start_workers()

commands = [
	start_workers
]
