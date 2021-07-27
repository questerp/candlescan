import frappe, json
from http import cookies
from urllib.parse import unquote, urlparse
from frappe.utils import cstr
#from candlescan.candlescan_service import get_last_broadcast
from frappe.utils import getdate,today,cstr
from frappe.utils.data import nowdate, getdate, cint, add_days, date_diff, get_last_day, add_to_date, flt
from candlescan.utils.candlescan import get_yahoo_prices as get_prices

import requests

@frappe.whitelist()
def get_session():
    return handle(True,"Session",frappe.session)

def clear_sessions(user):
    sessions = frappe.db.sql(""" select name from  `tabWeb Session` where user='%s'""" % user,as_dict=True)
    for s in sessions:
        frappe.delete_doc("Web Session",s.name,force=True)

def set_session():
    if frappe.session['user'] == 'noreply@candlescan.com':
        frappe.session.user = 'Administrator'
    #dsession = frappe.db.sql("""select * from tabSessions where user='Administrator'""",as_dict=True)
    #if dsession:
    #    session=dsession[0]
    #    frappe.session = session

def set_token(user,user_key):
    clear_sessions(user)
    user_token = frappe.generate_hash("", 10)
    d = frappe.get_doc({
                    "doctype":"Web Session",
                    "user": user,
                    "token":user_token,
                    "user_key":user_key
                    })
    d.insert()
    frappe.db.commit()
    frappe.local.cookie_manager.set_cookie("user_token", user_token)
    return user_token

def validate_token(user_key,token):
    if token and user_key:
        web_session = frappe.db.sql(""" select token, user_key from `tabWeb Session` where token='%s'""" % token,as_dict=True)
        return (web_session and web_session[0].user_key == user_key)
    else:
        return false
            
def logged_in():
    cookie = cookies.BaseCookie()
    cookie_headers = frappe.request.headers.get('Cookie') 
    if cookie_headers:
        cookie.load(cookie_headers)
    user_name = unquote(cookie["user_name"].value)
    user_key = unquote(cookie["user_key"].value)
    user_token = unquote(cookie["user_token"].value)
    #if hasattr(cookie, 'user_token'):
    #    user_token = unquote(cookie["user_token"].value)

    if not (user_name or user_key or user_token):
        headd = " %s %s --- " %(user_name,user_key)
        #return handle(False,"Please login",{'header':frappe.request.headers})
        frappe.throw("NO DATA %s " % (headd))
    if user_token:
        if validate_token(user_key,user_token):
            set_session()
            return
        #web_session = frappe.db.sql(""" select token, user_key from `tabWeb Session` where token='%s'""" % user_token,as_dict=True)
        #if web_session and web_session[0].user_key == user_key:
        #    set_session()
        #    return
        else:
            frappe.throw('Forbiden, Please login to continue.')
    else:
        original = frappe.utils.password.get_decrypted_password('Customer',user_name,fieldname='user_key')
        if user_key != original:
            frappe.throw('Forbiden, Please login to continue.')
        set_token(user_name,user_key)
        set_session()
        
    
@frappe.whitelist()     
def send_support(user,message):
    logged_in()
    if not (user or message):
        return handle(False,"Missing data")
    issue = frappe.get_doc({
        'doctype':'Issue',
        'subject':'Platform message from %s' % user,
        'customer':user,
        'description':message
    })
    issue.insert()
    return handle(True,"Success")


@frappe.whitelist()     
def logout(user):
    logged_in()
    clear_sessions(user)
    #frappe.db.sql(""" delete from `tabWeb Session` where user='%s'""" % user,as_dict=True)        
    frappe.local.cookie_manager.delete_cookie(["user_token", "user_name", "user_key"])
    return handle(True,"Success",user)
 
@frappe.whitelist()        
def get_subscription_print(user,name):
    logged_in()
    if not (user or name):
        return handle(False,"Missing data")
    req = frappe.get_doc("Subscription",name)
    req.send_pdf = True
    req.save()
    frappe.db.set_value("Subscription",name,"send_pdf",False)
    #frappe.db.commit()
    return handle(True,"Success")
    
@frappe.whitelist()        
def get_subscription_status(user):
    logged_in()
    if not user:
        return handle(False,"Missing data")
    
    # result = {"status":"active/unpaid"}
    subs = frappe.db.get_all("Subscription",filters={'customer': user},fields=['*'])
    current = [sub.name for sub in subs if (getdate(nowdate()) >= getdate(sub.start) and getdate(nowdate()) <= getdate(sub.current_invoice_end ))]
    if not current:
        return handle(True,"Success",{
        "current":[] ,
        "payed":[] ,
        "start":'' ,
        "end":'',
        "active":False})
    payed = []
    for c in current:
        doc = frappe.get_doc("Subscription",c)
        if not doc.has_outstanding_invoice() and not doc.is_new_subscription():
            payed.append(doc)
    #payed = [a for a in subs if (a and (frappe.get_doc("Subscription",a.name).has_outstanding_invoice() == False) and (len(a.invoices or []) > 0))]
    #payed_names = []
    active = len(payed)>0
    start = None
    end = None
    if payed:
        start = payed[0].start
        end = payed[0].current_invoice_end
    payed_names = [a.name for a in payed]
    return handle(True,"Success",{
        "current":current ,
        "payed":payed_names ,
        "start":start ,
        "end":end,
        "active":active})



@frappe.whitelist()        
def delete_subscription(user,name):   
    logged_in()
    if not (user or name):
        return handle(False,"Missing data")
    doc = frappe.get_doc("Subscription",name)
    if not doc.has_outstanding_invoice() and not doc.is_new_subscription():
        return handle(True,"Can't delete active subscriptions")
    frappe.delete_doc('Subscription', name)
    return handle(True,"Success")

@frappe.whitelist()        
def new_subscription(user,date,plan,qty):        
    logged_in()
    if not (user or date or plan or qty):
        return handle(False,"Missing data")
    subs = []
    for p in range(qty):
        sub = frappe.get_doc({
            'doctype':'Subscription',
            'customer': user,
            'start':getdate(date),
            'cancel_at_period_end':True,
            'generate_invoice_at_period_start':True,
        })
        sub.days_until_due = 0
        sub.append('plans',	{'qty':1,'plan':plan})
        sub.save()
        sub.process()
        subs.append(sub)
    return handle(True,"Success",subs)


@frappe.whitelist()        
def get_subscription(user):
    logged_in()
    if not user:
        return handle(False,"Missing data")
    subs = frappe.db.get_all("Subscription",filters={'customer': user},fields=['name'])
    results =[]
    for sub in subs:
        doc = frappe.get_doc("Subscription",sub.name)
        invoiced = doc.is_new_subscription()
        not_paied = doc.has_outstanding_invoice()
        days_left = date_diff(today(), doc.current_invoice_end) if doc.current_invoice_end else 0
        data = doc.as_dict()
        data['invoiced'] = invoiced
        data['not_paied'] = not_paied
        data['days_left'] = days_left
        
        results.append(data)
    return handle(True,"Success",results)
                             
@frappe.whitelist()        
def get_plans():
    logged_in()
    settings = frappe.get_doc("Candlescan Settings")
    monthly = frappe.get_doc("Subscription Plan",settings.monthly_item)
    annual = frappe.get_doc("Subscription Plan",settings.annual_item)
    return handle(True,"Success",[monthly,annual])
    


#@frappe.whitelist()
#def get_scanners(user):
#    logged_in()
#    if not user:
#        return handle(Flase,"User is required")
#    scanners = frappe.db.sql(""" select title,description,active,scanner_id,scanner,method from `tabCandlescan scanner` """,as_dict=True)
#    extras = frappe.db.get_single_value('Candlescan Settings', 'extras')
#    
#    fExtras = []
#    if extras:
#        extras = extras.splitlines()
#        for ex in extras:
#            name, label, value_type = ex.split(':')
#            fExtras.append({"name":name,"label":label,"value_type":value_type})
#    for scanner in scanners:
#        signautre_method = "%s.signature" % scanner.method
#        config_method = "%s.get_config" % scanner.method
#        signature = frappe.call(signautre_method, **frappe.form_dict)
#        config = frappe.call(config_method, **frappe.form_dict)
#        scanner['signature'] = signature
#        scanner['config'] = config
#    return handle(True,"Success",{"scanners":scanners,"extras":fExtras})

@frappe.whitelist(allow_guest=True)
def update_customer(name,customer_name,email):
    logged_in()
    if not (name or customer_name or email):
        return handle(False,"Missing data")
    frappe.db.set_value("Customer",name,"customer_name",customer_name)
    frappe.db.set_value("Customer",name,"email",email)
    #frappe.db.commit()
    return handle(True,"Success",get_user(name,'name'))


@frappe.whitelist()
def confirm_email(customer,code):
    logged_in()
    if not (customer or code):
        return handle(False,"Check data")
    ocode = frappe.db.get_value("Customer",{"name":customer},"confirm")
    if code == ocode:
        frappe.db.set_value("Customer",customer,"email_is_confirmed",1)
        #frappe.db.commit()
        return handle(True,"Email is confirmed",True)
    return handle(True,"Wrong confirmation code, please try again",False)


@frappe.whitelist(allow_guest=True)
def login_customer(usr,pwd):
    if not (usr or pwd):
        return handle(True,"Missing password and/or email",[False])
    user = get_user(usr)
    #user = frappe.db.get_value('Customer',{'email':usr},['name','customer_type','email_is_confirmed','referral','user_key','email','customer_name','image'],as_dict=True)
    if not user:
        return handle(True,"Wrong password and/or email",[False])
        #return {'result':False,'msg':'Wrong password and/or email'}
    password = frappe.utils.password.get_decrypted_password('Customer',user.name,fieldname='password')
    if password == pwd:
        user_token = set_token(user.name,user.user_key)
        return handle(True,"Logged in",[True,user,user_token])
    return handle(True,"Incorrect email or password",False)

def get_user(name,target='email'):
    user = frappe.db.get_value('Customer',{target:name},['name','default_layout','customer_type','email_is_confirmed','referral','user_key','email','customer_name','image'],as_dict=True)
    if user:
        user_key = frappe.utils.password.get_decrypted_password('Customer',user.name,fieldname="user_key")
        user['user_key'] = user_key
        return user

@frappe.whitelist(allow_guest=True)
def signup_customer(customer_name,email,password):
    if not (customer_name or email or password):
        return handle(True,"Some fields are required",[False])
    customer = frappe.get_doc({
        'doctype':"Customer",
        'customer_name':customer_name,
        'email':email,
        'password':password
        })
    c = customer.insert(ignore_permissions=1)
    #frappe.db.commit()
    customer = get_user(c.name,'name')
    if customer:
        user_token = set_token(customer.name,customer.user_key)
        return handle(True,"Success",[True,customer,user_token])
    return handle(True,"Can't find any account linked to this email",[False])


def handle(result=False,msg='Call executed',data=None):
        return {'result':result,'msg':msg,'data':data}

    
