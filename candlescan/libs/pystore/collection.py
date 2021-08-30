
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
        conn = apsw.Connection(self.path)
        with conn:
            cur = conn.cursor()
            sql  ="drop table if exists bars ;create table bars(s NOT NULL,t NOT NULL,o,c,h,l,v,PRIMARY KEY(s,t))"
            cur.execute(sql)
            # cur.execute("create unique index on bars(t)")

    

    @multitasking.task
    def _list_items_threaded(self):
        self.items = self.list_items()

    def list_items(self):
        dirs = utils.subdirs(utils.make_path(self.datastore, self.collection))
        return set(dirs)

    def item(self, item,files,  filters=None, columns=None):
        return Item(item, self.datastore, self.collection,files, filters, columns )

    def index(self, item, last=False):
        return 0
      

    def delete_item(self, item, reload_items=False):
        shutil.rmtree(self.get_item_path(item))
        self.items.remove(item)
        if reload_items:
            self.items = self._list_items_threaded()
        return True


    def write(self,data ):
         
            
        #print(path)
        # if   append :
        #     if  utils.path_exists(path) :
        #         raise ValueError("""
        #             Item already exists. To overwrite, use `overwrite=True`.
        #             Otherwise, use `<collection>.append()`""")

        if not data:
            return

        if not isinstance(data,list):
            data = [data]

        values = [[a['t'],a['o'],a['c'],a['h'],a['l'],a['v']] for a in data]
        conn = apsw.Connection(self.path)
        with conn:
            cur = conn.cursor()
            cur.executemany("INSERT or IGNORE INTO bars(t,o,c,h,l,v) VALUES(?,?,?,?,?,?)", values)

        # with lock:
        #     data.to_hdf(
        #         path,
        #         "table",
        #         min_itemsize=min_itemsize,
        #         append=True,
        #         complib="zlib",
        #         complevel= 6,
        #         data_columns = True,
        #         format="table"
        #     )
        # wr.s3.to_parquet(
        #                 df=data,
        #                 path=path,
        #                 dataset=True,
        #                 mode ="append",
        #                 partition_cols =["s"],
        #                 index=False,
        #             )
                    
        # table = pa.Table.from_pandas(data,preserve_index=False)
        # pq.ParquetWriter(path, table.schema).write_table(table)            

        # if append:
        #     pq.ParquetWriter(path, table.schema).write_table(table)            
        # else:
        #     pq.write_table(table, path)


    # def append(self, item, data):
    #     self.write(
    #         item,
    #         data, 
    #         append=True,
    #         path = self._item_path(item,True) )


  