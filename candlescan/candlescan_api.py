import frappe
from http import cookies
from urllib.parse import unquote, urlparse

def logged_in():
    cookie = cookies.BaseCookie()
    cookie_headers = frappe.request.headers.get('Cookie') 
    if cookie_headers:
        cookie.load(cookie_headers)
    user_name = unquote(cookie["user_name"].value)
    user_key = unquote(cookie["user_key"].value)

    if not (user_name or user_key):
        headd = " %s %s --- " %(user_name,user_key)
        #return handle(False,"Please login",{'header':frappe.request.headers})
        frappe.throw("NO DATA %s " % (headd))

    original = frappe.utils.password.get_decrypted_password('Customer',user_name,fieldname='user_key')
    if user_key != original:
        frappe.throw('Forbiden, Please login to continue.')


@frappe.whitelist()
def get_scanners(user):
    logged_in()
    if not user:
        return handle(Flase,"User is required")
    scanners = frappe.db.sql(""" select title,description,active,scanner_id,scanner,method from `tabCandlescan scanner` """,as_dict=True)
    for scanner in scanners:
        method = "%s.signature" % scanner.method
        signature = frappe.call(method, **frappe.form_dict)
        scanner['signature'] = signature
    return handle(True,"Success",scanners)

@frappe.whitelist(allow_guest=True)
def update_customer(name,customer_name,email):
    logged_in()
    if not (name or customer_name or email):
        return handle(False,"Missing data")
    frappe.db.set_value("Customer",name,"customer_name",customer_name)
    frappe.db.set_value("Customer",name,"email",email)
    frappe.db.commit()
    return handle(True,"Success",get_user(name,'name'))


@frappe.whitelist()
def confirm_email(customer,code):
    logged_in()
    if not (customer or code):
        return handle(False,"Check data")
    ocode = frappe.db.get_value("Customer",{"name":customer},"confirm")
    if code == ocode:
        frappe.db.set_value("Customer","email_is_confirmed",1)
        frappe.db.commit()
        return handle(True,"Email is confirmed")
    return handle(False,"Wrong confirmation code, please try again")


@frappe.whitelist(allow_guest=True)
def login_customer(usr,pwd):
    if not (usr or pwd):
        return {'result':False,'msg':'Missing email and/or password'}
    user = get_user(usr)
    #user = frappe.db.get_value('Customer',{'email':usr},['name','customer_type','email_is_confirmed','referral','user_key','email','customer_name','image'],as_dict=True)
    if not user:
        return handle(False,"Wrong password and/or email")
        #return {'result':False,'msg':'Wrong password and/or email'}
    password = frappe.utils.password.get_decrypted_password('Customer',user.name,fieldname='password')
    if password == pwd:
        return handle(True,"Logged in",user)
        #return {'result':True,'msg':'mock op success','data':user}

def get_user(name,target='email'):
    user = frappe.db.get_value('Customer',{target:name},['name','customer_type','email_is_confirmed','referral','user_key','email','customer_name','image'],as_dict=True)
    user_key = frappe.utils.password.get_decrypted_password('Customer',user.name,fieldname="user_key")
    user['user_key'] = user_key
    return user

@frappe.whitelist(allow_guest=True)
def signup_customer(customer_name,email,password):
    if not (customer_name or email or password):
        return handle(False,"Check data")
    customer = frappe.get_doc({
        'doctype':"Customer",
        'customer_name':customer_name,
        'email':email,
        'password':password
        })
    c = customer.insert(ignore_permissions=1)
    frappe.db.commit()
    customer = get_user(c.name,'name')
    return handle(True,"Success",customer)


def handle(result=False,msg='Call executed',data=None):
        return {'result':result,'msg':msg,'data':data}


