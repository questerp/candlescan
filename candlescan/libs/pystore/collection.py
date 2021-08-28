
import os
import time
import shutil
# import dask.dataframe as dd
import multitasking

from . import utils
from .item import Item
from . import config
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

class Collection(object):
    def __repr__(self):
        return "PyStore.collection <%s>" % self.collection

    def __init__(self, collection, datastore ):
        self.datastore = datastore
        self.collection = collection
        self.items = self.list_items()

    def _item_path(self, item, as_string=False):
        p = utils.make_path(self.datastore, self.collection, item)
        if as_string:
            return str(p)
        return p

    @multitasking.task
    def _list_items_threaded(self):
        self.items = self.list_items()

    def list_items(self):
        dirs = utils.subdirs(utils.make_path(self.datastore, self.collection))
        return set(dirs)

    def item(self, item,  filters=None, columns=None):
        return Item(item, self.datastore, self.collection, filters, columns )

    def index(self, item, last=False):
        return 0
      

    def delete_item(self, item, reload_items=False):
        shutil.rmtree(self._item_path(item))
        self.items.remove(item)
        if reload_items:
            self.items = self._list_items_threaded()
        return True


    def write(self,item,data,append = False,path=None):
        if path is None:
            path =  self._item_path(item,True) 
        #print(path)
        if not (overwrite or append):
            if  utils.path_exists(path) :
                raise ValueError("""
                    Item already exists. To overwrite, use `overwrite=True`.
                    Otherwise, use `<collection>.append()`""")

        if data.empty:
            return

        table = pa.Table.from_pandas(data)
        if append:
            pq.ParquetWriter(path, table.schema).write_table(table)            
        else:
            pq.write_table(table, 'example.parquet')


    def append(self, item, data):
        self.write(
            item,
            data, 
            append=True,
            path = self._item_path(item,True) )


  