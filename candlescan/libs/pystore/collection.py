
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
        
        with conn:
            cur = conn.cursor()
            sql  ="drop table if exists bars ;create table bars(s NOT NULL,t NOT NULL,o,c,h,l,v,PRIMARY KEY(s,t))"
            cur.execute(sql)
            self.create_ta_table(conn)


    def create_ta_table(self,conn=None,symbols=None):
        if conn is None:
            conn = self.get_connection()
        if symbols is None:
            symbols = get_active_symbols()
        with conn:
            cur = conn.cursor()
            sql  ="""drop table if exists ta ;
            create table 
            ta(
                s NOT NULL,
                sma50 ,
                sma20 ,
                sma15 ,
                sma10 ,
                sma9 ,
                sma8 ,
                sma7 ,
                sma6 ,
                sma5 ,
                sma4 ,
                price,
                atr7,
                atr14,
                atr20,
                cum_vol,
                total_tpv,
                vwap,
                PRIMARY KEY(s)
            )
            """
            cur.execute(sql)
            sql  ="""drop table if exists bars_tmp ;
            create table 
            bars_tmp(
                s,
                c,
                o,
                h,
                l,
                v
            )
            """
            cur.execute(sql)
            # view
            # sma15   =   select sum(c)/15 from (select c from bars_tmp limit 15),
            # sma10   =   select sum(c)/10 from (select c from bars_tmp limit 10),
            # sma9    =   select sum(c)/9 from (select c from bars_tmp limit 9),
            # sma8    =   select sum(c)/8 from (select c from bars_tmp limit 8),
            # sma7    =   select sum(c)/7 from (select c from bars_tmp limit 7),
            # sma6    =   select sum(c)/6 from (select c from bars_tmp limit 6),
            # sma5    =   select sum(c)/5 from (select c from bars_tmp limit 5),
            # sma4    =   select sum(c)/4 from (select c from bars_tmp limit 4),
            sql  ="""
            create trigger if not exists ta_trigger after insert on bars
                when (NEW.t >= (strftime('%s','now')-120))
                    begin
                        INSERT INTO bars_tmp(s,c ,o,h,l,v) select s,c,o,h,l,v from bars where s=NEW.s order by t desc limit 50 ;
                        update ta set 
                            sma50   =   (SELECT sum(c)/50 FROM (SELECT c FROM bars_tmp LIMIT 50)) ,
                            sma20   =   (SELECT sum(c)/20 FROM (SELECT c FROM bars_tmp LIMIT 20)) ,
                            sma15   =   (select sum(c)/15 from (select c from bars_tmp limit 15)),
                            sma10   =   (select sum(c)/10 from (select c from bars_tmp limit 10)),
                            sma9    =   (select sum(c)/9 from (select c from bars_tmp limit 9)),
                            sma8    =   (select sum(c)/8 from (select c from bars_tmp limit 8)),
                            sma7    =   (select sum(c)/7 from (select c from bars_tmp limit 7)),
                            sma6    =   (select sum(c)/6 from (select c from bars_tmp limit 6)),
                            sma5    =   (select sum(c)/5 from (select c from bars_tmp limit 5)),
                            sma4    =   (select sum(c)/4 from (select c from bars_tmp limit 4)),
                            cum_vol =   cum_vol + NEW.v,
                            price   =  (NEW.c + NEW.h + NEW.l) / 3,
                            total_tpv = NEW.v * price,
                            vwap    =   total_tpv / IFNULL(cum_vol,1)
                        where s=NEW.s;
                        DELETE FROM bars_tmp;
                    end;
            """
            cur.execute(sql)
            for s in symbols:
                cur.execute("INSERT INTO ta(s) VALUES(?)", (s,))
    

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

  