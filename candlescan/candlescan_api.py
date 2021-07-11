import frappe, json
from http import cookies
from urllib.parse import unquote, urlparse
from frappe.utils import cstr
from candlescan.candlescan_service import get_last_broadcast

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
def last_broadcast(user,scanner_id):
    logged_in()
    if not (user or scanner_id):
        return handle(Flase,"User is required")
    doctype =  frappe.db.get_value("Candlescan scanner", {"scanner_id":scanner_id})
    return get_last_broadcast(doctype,scanner_id)
       
@frappe.whitelist()        
def check_symbol(user,symbol):
    #logged_in()
    if not (user or symbol):
        return handle(Flase,"User is required")
    exists =  frappe.db.exists("Symbol", symbol)
    return handle(True,"Success",{"exists":exists})
        
@frappe.whitelist()        
def delete_layout(user,name):
    logged_in()
    if not (user or name):
        return handle(Flase,"User is required")
    frappe.delete_doc('Candlescan Layout', name)
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
def save_layout(user,layout,scanner,layout_name,target,name=None):
    logged_in()
    if not (user or layout or layout_name or target or scanner):
        return handle(Flase,"User is required")
    layout_obj = None
    if name:
        layout_obj = frappe.get_doc("Candlescan Layout",name)
    else:
        layout_obj = frappe.get_doc({
            'doctype':"Candlescan Layout",
            'user': user,
            'target':target
        })
        
    layout_obj.layout_name = layout_name
    layout_obj.layout = layout
    layout_obj.scanner = scanner
    
    if name:
        layout_obj = layout_obj.save()
    else:
        layout_obj = layout_obj.insert()
        
    return handle(True,"Success",layout_obj)
        
        
@frappe.whitelist()        
def update_socket(user,socket_id):
    logged_in()
    if not (user or socket_id):
        return handle(Flase,"User is required")
    frappe.db.set_value("Customer",user,"socket_id",socket_id)
    return handle(True,"Success")
        
@frappe.whitelist()        
def get_platform_data(user):    
    logged_in()
    if not user:
        return handle(Flase,"User is required")
    alerts = frappe.db.sql(""" select name,user,creation, enabled, filters_script, symbol, triggered, notify_by_email from `tabPrice Alert` where user='%s'""" % (user),as_dict=True)
    extras = frappe.db.get_single_value('Candlescan Settings', 'extras')
    scanners = frappe.db.sql(""" select title,description,active,scanner_id,scanner,method from `tabCandlescan scanner` """,as_dict=True)
    layouts = frappe.db.sql(""" select layout_name,scanner,name,user,layout,target from `tabCandlescan Layout` where user='%s' """ % (user),as_dict=True)
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

    return handle(True,"Success",{"scanners":scanners,"extras":fExtras,"alerts":alerts,"layouts":layouts,"watchlists":watchlists})
            
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
    c = alert.insert(ignore_permissions=1)
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
    return handle(False,"Incorrect email or password")

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
    
