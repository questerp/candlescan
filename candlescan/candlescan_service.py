import frappe


def after_signup(customer):
    if not customer:
        return
