import frappe, json
from http import cookies
from urllib.parse import unquote, urlparse
from frappe.utils import cstr
from candlescan.candlescan_service import get_last_broadcast
from frappe.utils import getdate,today
from frappe.utils.data import nowdate, getdate, cint, add_days, date_diff, get_last_day, add_to_date, flt
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
        web_session = frappe.db.sql(""" select token, user_key from `tabWeb Session` where token='%s'""" % user_token,as_dict=True)
        if web_session and web_session[0].user_key == user_key:
            set_session()
            return
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
def delete_stock_filter(user,name):
    logged_in()
    if not (user or name):
        return handle(False,"Missing data")
    frappe.delete_doc('Stock Filter', name)
    return handle(True,"Success")

@frappe.whitelist()     
def save_stock_filter(user,title,sort_field,sort_mode,script,name=None):
    logged_in()
    if not (user or title or sort_field or sort_mode or script):
        return handle(False,"Missing data")
    if not name:
        filter = frappe.get_doc({
                    'doctype':'Stock Filter',
                    'user': user,
                    'title': title,
                    'sort_field': sort_field,
                    'sort_mode': sort_mode,
                    'script': script
                })
        filter = filter.insert()
        return handle(True,"Success",filter)
    else:
        f = frappe.get_doc("Stock Filter",name)
        if f:
            f.title = title
            f.sort_field=sort_field
            f.sort_mode=sort_mode
            f.script=script
            f = f.save()
            return handle(True,"Success",f)
        else:
            return handle(False,"Failed! filter doesn't exist")
            
            

@frappe.whitelist()     
def delete_filter(user,filter):
    logged_in()
    if not (user or filter):
        return handle(False,"Missing data")        
    
    

@frappe.whitelist()     
def set_default_layout(user,layout):
    logged_in()
    if not (user or layout):
        return handle(False,"Missing data")   
    frappe.db.set_value("Customer",user,"default_layout",layout)
    return handle(True,"Success")
    
@frappe.whitelist()     
def logout(user):
    logged_in()
    clear_sessions(user)
    #frappe.db.sql(""" delete from `tabWeb Session` where user='%s'""" % user,as_dict=True)        
    frappe.local.cookie_manager.delete_cookie(["user_token", "user_name", "user_key"])
    return handle(True,"Success",user)
        
@frappe.whitelist()        
def get_calendar(target):
    logged_in()
    if not target:
        return handle(False,"Missing data")
    calendar = frappe.db.get_value("Fundamentals",None,target)
    return handle(True,"Success",calendar)
        
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
    
@frappe.whitelist()        
def last_broadcast(user,scanner):
    logged_in()
    if not (user or scanner):
        return handle(False,"User is required")
    #doctype =  frappe.db.get_value("Candlescan scanner", {"scanner_id":scanner_id},"scanner")
    return get_last_broadcast(scanner)
       
@frappe.whitelist()        
def check_symbol(user,symbol):
    #logged_in()
    if not (user or symbol):
        return handle(False,"User is required")
    exists =  frappe.db.exists("Symbol", symbol)
    return handle(True,"Success",{"exists":exists})
        
@frappe.whitelist()        
def delete_custom_scanner(user,name):
    logged_in()
    if not (user or name):
        return handle(False,"User is required")
    frappe.delete_doc('Custom Scanner', name)
    return handle(True,"Success")
    

@frappe.whitelist()        
def get_historical(user,doctype,date,feed_type):
    logged_in()
    if not (user or doctype or date):
        return handle(False,"Data missing")
    lmt = 1 if feed_type == "list" else 20
        
    values = frappe.db.sql(""" select creation,data from `tabVersion` where ref_doctype='%s' and creation<='%s' order by creation DESC limit %s""" % (doctype,date,lmt),as_dict=True)
    if values:
        values = values[0]
        odata = json.loads(values.data)
        state = json.loads(odata['changed'][0][1])
        if state:
            resp = {"version":values.creation,"data":state}
            return handle(True,"Success",resp)
    return handle(True,"Success")
    
@frappe.whitelist()        
def get_select_values(doctype):
    logged_in()
    if not doctype:
        return handle(Flase,"Data required")
    values = frappe.db.sql(""" select name from `tab%s` limit 100""" % doctype,as_dict=True)
    values = [a['name'] for a in values]
    return handle(True,"Success",values)


@frappe.whitelist()        
def delete_watchlist(user,watchlist_id):
    logged_in()
    if not (user or watchlist_id):
        return handle(Flase,"User is required")
    frappe.delete_doc('Watchlist', watchlist_id)
    return handle(True,"Success")
  

@frappe.whitelist()        
def delete_layout(user,name):
    logged_in()
    if not (user or name):
        return handle(Flase,"User is required")
    frappe.delete_doc('Layout', name)
    return handle(True,"Success")
    
@frappe.whitelist()        
def save_layout(user,title,config,name=None):
    logged_in()
    if not (user or title):
        return handle(Flase,"User is required")
    if not name:
        w = frappe.get_doc({
            'doctype':'Layout',
            'title':title,
            'config':config,
            'user':user,
        })
        w = w.insert()
        return handle(True,"Success",w)
        
    else:
        frappe.db.set_value("Layout",name,"config",config)
        frappe.db.set_value("Layout",name,"title",title)
        w = frappe.get_doc("Layout",name)
        return handle(True,"Success",w)
        
@frappe.whitelist()        
def save_watchlist(user,watchlist,symbols='',name=None):
    logged_in()
    if not (user or watchlist):
        return handle(Flase,"User is required")
    if not name:
        w = frappe.get_doc({
            'doctype':'Watchlist',
            'watchlist':watchlist,
            'user':user,
        })
        w = w.insert()
        return handle(True,"Success",w)
        
    else:
        frappe.db.set_value("Watchlist",name,"watchlist",watchlist)
        frappe.db.set_value("Watchlist",name,"symbols",symbols)
        w = frappe.get_doc("Watchlist",name)
        return handle(True,"Success",w)
    
    
@frappe.whitelist()        
def save_customer_scanner(user,config,scanner,title,target,name=None):
    logged_in()
    if not (user or config or title or target or scanner):
        return handle(Flase,"User is required")
    scanner_obj = None
    if name:
        scanner_obj = frappe.get_doc("Custom Scanner",name)
    else:
        scanner_obj = frappe.get_doc({
            'doctype':"Custom Scanner",
            'user': user,
            'target':target
        })
        
    scanner_obj.title = title
    scanner_obj.config = config
    scanner_obj.scanner = scanner
    
    if name:
        scanner_obj = scanner_obj.save()
    else:
        scanner_obj = scanner_obj.insert()
        
    return handle(True,"Success",scanner_obj)
        
        
@frappe.whitelist()        
def update_socket(user,socket_id):
    logged_in()
    if not (user or socket_id):
        return handle(Flase,"User is required")
    frappe.db.set_value("Customer",user,"socket_id",socket_id)
    return handle(True,"Success")

@frappe.whitelist()        
def get_alerts(user):
    logged_in()
    if not user:
        return handle(Flase,"User is required")
    alerts = frappe.db.sql(""" select name,user,creation, enabled, filters_script, symbol, triggered, notify_by_email from `tabPrice Alert` where user='%s'""" % (user),as_dict=True)
    return handle(True,"Success",alerts)
    
@frappe.whitelist()        
def get_platform_data(user):    
    logged_in()
    if not user:
        return handle(Flase,"User is required")
    alerts = frappe.db.sql(""" select name,user,creation, enabled, filters_script, symbol, triggered, notify_by_email from `tabPrice Alert` where user='%s'""" % (user),as_dict=True)
    extras = frappe.db.get_single_value('Candlescan Settings', 'extras')
    scanners = frappe.db.sql(""" select title,description,active,scanner_id,scanner,method from `tabCandlescan scanner` """,as_dict=True)
    customScanners = frappe.db.sql(""" select title,scanner,name,user,config,target from `tabCustom Scanner` where user='%s' """ % (user),as_dict=True)
    layouts = frappe.db.sql(""" select title,name,config  from `tabLayout` where user='%s' """ % (user),as_dict=True)
    watchlists = frappe.db.sql(""" select name,watchlist,symbols from `tabWatchlist` where user='%s' """ % (user),as_dict=True)
    fExtras = []
    if extras:
        extras = extras.splitlines()
        for ex in extras:
            name, label, value_type,doctype = ex.split(':')
            fExtras.append({"field":name,"header":label,"value_type":value_type,"doctype":doctype})
    for scanner in scanners:
        signautre_method = "%s.signature" % scanner.method
        config_method = "%s.get_config" % scanner.method
        signature = frappe.call(signautre_method, **frappe.form_dict)
        config = frappe.call(config_method, **frappe.form_dict)
        scanner['signature'] = signature
        scanner['config'] = config

    return handle(True,"Success",{"layouts":layouts,"scanners":scanners,"extras":fExtras,"alerts":alerts,"customScanners":customScanners,"watchlists":watchlists})
            
#@frappe.whitelist()        
#def get_alerts(user):
#    logged_in()
#    if not user:
#        return handle(Flase,"User is required")
#    
#    alert_fields = frappe.db.get_single_value('Candlescan Settings', 'alert_fields')
#    fAlerts = []
#    if alert_fields:
#        alert_fields = alert_fields.splitlines()
#        for ex in alert_fields:
#            name, label, value_type = ex.split(':')
#            fAlerts.append({"name":name,"label":label,"value_type":value_type})
#    alerts = frappe.db.sql(""" select name,user,creation, enabled, filters_script, symbol, triggered, notify_by_email from `tabPrice Alert` where user='%s'""" % (user),as_dict=True)
#    return handle(True,"Success",{"alerts":alerts,"alert_fields":fAlerts})


@frappe.whitelist()        
def activate_alert(user,name):
    logged_in()
    if not (user or name):
        return handle(False,"Missing data")
    frappe.db.set_value('Price Alert', name, "triggered", 0)
    return handle(True,"Success")

@frappe.whitelist()        
def toggle_alert(user,name,enabled):
    logged_in()
    if not (user or name):
        return handle(False,"Missing data")
    frappe.db.set_value('Price Alert', name, "enabled", enabled)
    return handle(True,"Success")


@frappe.whitelist()        
def delete_alert(user,name):
    logged_in()
    if not (user or name):
        return handle(False,"Missing data")
    frappe.delete_doc('Price Alert', name)
    return handle(True,"Success")

@frappe.whitelist()        
def add_alert(user,symbol,filters,notify):
    logged_in()
    if not (user or symbol):
        return handle(False,"No user found")
    
    #res = []
    #for f in filters:
    #    r = {f['field']:[f['operator'],f['value']]}
    #    res.append(r)
    #fs = json.dumps(res)
    alert = frappe.get_doc({
        'doctype': 'Price Alert',
        'user': user,
        'triggered':False,
        'enabled':True,
        'symbol':symbol,
        'filters_script':filters,
        'notify_by_email':notify
    })
    c = alert.insert()
    return handle(True,"Success",c)

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

@frappe.whitelist()
def get_extra_data(symbols,fields):
    logged_in()
    if not (symbols or fields):
        return handle(False,"Data missing")

    sql_fields =  ' ,'.join(fields)
    sql_symbols =  ', '.join(['%s']*len(symbols))
    sql = """select name,{0} from tabSymbol where name in ({1})""".format(sql_fields,sql_symbols)
    result = frappe.db.sql(sql,tuple(symbols),as_dict=True)
    return handle(True,"Success",result)

@frappe.whitelist()
def get_symbol_info(symbol):
    logged_in()
    if not symbol:
        return handle(False,"Data missing")
    
    # return data fields
    fields =  ' ,'.join(["name","stock_summary_detail","key_statistics_data","key_price_data","key_summary_data","website","summary","industry_type","company","country","floating_shares","sector","exchange"])
    data = frappe.db.sql(""" select {0} from tabSymbol where symbol='{1}' limit 1 """.format(fields,symbol),as_dict=True)
    if(data and len(data)>0):
        return handle(True,"Success",data[0])
    return handle(False,"Symbol not found")
    
