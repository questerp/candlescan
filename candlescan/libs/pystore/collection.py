
import os
import time
import shutil
# import dask.dataframe as dd
import multitasking

from . import utils
from .item import Item
from . import config
import pandas as pd
import apsw
# import threading
# lock = threading.Lock()
from candlescan.utils.candlescan import get_active_symbols
from datetime import datetime as dt 

class Collection(object):

    ITEM_FORMAT = "%Y%m%d"
    def __repr__(self):
        return "PyStore.collection <%s>" % self.collection

    def __init__(self, collection, datastore ):
        self.datastore = datastore
        self.collection = collection
        self.items = self.list_items()
        self.path =  self.get_item_path("data") 
        

    def get_item_path(self, item ):
        if not isinstance(item , str):
            item = item.strftime(self.ITEM_FORMAT)
        p = utils.make_path(self.datastore, self.collection, item)
        # if as_string:
        return  str(p)
        # return p 

    def create_table(self,item):
        path = self.get_item_path(item)
        #conn = apsw.Connection(path)
        conn = self.get_connection()
        symbols = get_active_symbols()
        with conn:
            cur = conn.cursor()
            sql  ="drop table if exists bars ;create table bars(s NOT NULL,t NOT NULL,o,c,h,l,v,PRIMARY KEY(s,t))"
            cur.execute(sql)
            sql  ="drop table if exists ta ;create table ta(s NOT NULL,sma20 ,PRIMARY KEY(s))"
            cur.execute(sql)
            sql  ="""create trigger if not exists sma20 after insert on bars
            begin
                update ta set sma20=((select sum(b.c) from bars as b where b.s=NEW.s limit 20)/20) where s=NEW.s;
            end;
            """
            cur.execute(sql)
            for s in symbols:
                cur.execute("INSERT INTO ta(s) VALUES(?)", s)


            
            cur.execute("create unique index on bars(t)")
            #conn.close()
    

    @multitasking.task
    def _list_items_threaded(self):
        self.items = self.list_items()

    def list_items(self):
        dirs = utils.subdirs(utils.make_path(self.datastore, self.collection))
        return set(dirs)

    def item(self, item,start,end,  filters=None, columns=None):
        #(self, item, path,start, filters=None, columns=None,):
        return Item(item, self.path,start,end, filters, columns )

    def index(self, item, last=False):
        return 0
      

    def delete_item(self, item, reload_items=False):
        shutil.rmtree(self.get_item_path(item))
        self.items.remove(item)
        if reload_items:
            self.items = self._list_items_threaded()
        return True

    def get_connection(self):
        #if not self.conn:
        conn = apsw.Connection(self.path)
        conn.setbusytimeout(5000)
        return conn

    def write(self,data ):
        if not data:
            return

        #s NOT NULL,t NOT NULL,o,c,h,l,v
        conn = self.get_connection()
       
        try:
            with conn:
                cur = conn.cursor()
                #cur.execute('BEGIN IMMEDIATE;')insert into foo values(:alpha, :beta, :gamma)", {'alpha': 1, 'beta': 2, 'gamma': 'three'})
                for item in data:
                    try:
                        cur.execute("INSERT or IGNORE INTO bars VALUES(?,?,?,?,?,?,?)", (item['s'],item['t'],item['o'],item['c'],item['h'],item['l'],item['v']))#or IGNORE
                    except apsw.ConstraintError as consterr:
                        print("ConstraintError",consterr)
                        print(item)

                #conn.close()
                # cur.execute('COMMIT;')
        except apsw.BusyError as err:
            print("BusyError",err)
            time.sleep(1)
            self.write(data)
        except apsw.SQLError as sqlerr:
            print("SQLError",sqlerr)
        

    def commit(self):
        conn = self.get_connection()
        cur.execute('COMMIT;')
        #conn.close()

  