import frappe, random


def after_signup(customer):
    if not customer:
        return
    if not customer.confirm:
        customer.confirm = random.randrange(1000,99999)
    if not customer.referral:
        customer.referral = frappe.generate_hash(length=10)
    if not customer.user_key:
        customer.user_key = frappe.generate_hash(length=15)
    
        
