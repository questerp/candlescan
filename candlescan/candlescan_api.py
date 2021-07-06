import frappe, json
from http import cookies
from urllib.parse import unquote, urlparse
from frappe.utils import cstr

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
def get_alerts(user):
    logged_in()
    if not user:
        return handle(Flase,"User is required")
    
    alert_fields = frappe.db.get_single_value('Candlescan Settings', 'alert_fields')
    fAlerts = []
    if alert_fields:
        alert_fields = alert_fields.splitlines()
        for ex in alert_fields:
            name, label, value_type = ex.split(':')
            fAlerts.append({"name":name,"label":label,"value_type":value_type})
    alerts = frappe.db.sql(""" select name,user,creation, enabled, filters_script, symbol, triggered, notify_by_email from `tabPrice Alert` where user='%s'""" % (user),as_dict=True)
    return handle(True,"Success",{"alerts":alerts,"alert_fields":fAlerts})


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
    
    res = []
    for f in filters:
        r = {f['field']:[f['operator'],f['value']]}
        res.append(r)
    fs = json.dumps(res)
    alert = frappe.get_doc({
        'doctype': 'Price Alert',
        'user': user,
        'triggered':False,
        'enabled':True,
        'symbol':symbol,
        'filters_script':fs,
        'notify_by_email':notify
    })
    c = alert.insert(ignore_permissions=1)
    return handle(True,"Success",c)

@frappe.whitelist()
def get_scanners(user):
    logged_in()
    if not user:
        return handle(Flase,"User is required")
    scanners = frappe.db.sql(""" select title,description,active,scanner_id,scanner,method from `tabCandlescan scanner` """,as_dict=True)
    extras = frappe.db.get_single_value('Candlescan Settings', 'extras')
    
    fExtras = []
    if extras:
        extras = extras.splitlines()
        for ex in extras:
            name, label, value_type = ex.split(':')
            fExtras.append({"name":name,"label":label,"value_type":value_type})
    for scanner in scanners:
        signautre_method = "%s.signature" % scanner.method
        config_method = "%s.get_config" % scanner.method
        signature = frappe.call(signautre_method, **frappe.form_dict)
        config = frappe.call(config_method, **frappe.form_dict)
        scanner['signature'] = signature
        scanner['config'] = config
    return handle(True,"Success",{"scanners":scanners,"extras":fExtras})

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
    fields =  ' ,'.join(["symbol","company","country","floating_shares","sector","exchange"])
    data = frappe.db.sql(""" select {0} from tabSymbol where symbol='{1}' limit 1 """.format(fields,symbol),as_dict=True)
    if(data and len(data)>0):
        return handle(True,"Success",data[0])
    return handle(False,"Symbol not found")
    
