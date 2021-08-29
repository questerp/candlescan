
import os
import time
import shutil
# import dask.dataframe as dd
import multitasking

from . import utils
from .item import Item
from . import config
import pandas as pd
# import pyarrow as pa
# import pyarrow.parquet as pq
import awswrangler as wr

from datetime import datetime as dt 

class Collection(object):

    ITEM_FORMAT = "%Y%m%d"

    def __repr__(self):
        return "PyStore.collection <%s>" % self.collection

    def __init__(self, collection, datastore ):
        self.datastore = datastore
        self.collection = collection
        self.items = self.list_items()

    def get_item_path(self, item, as_string=True):
        item = item.strftime(self.ITEM_FORMAT)
        p = utils.make_path(self.datastore, self.collection, item)
        if as_string:
            return str(p)
        return p+".parquet"

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
        shutil.rmtree(self.get_item_path(item))
        self.items.remove(item)
        if reload_items:
            self.items = self._list_items_threaded()
        return True


    def write(self,item,data,path=None):
        if path is None:
            path =  self.get_item_path(item,True) 
        #print(path)
        # if   append :
        #     if  utils.path_exists(path) :
        #         raise ValueError("""
        #             Item already exists. To overwrite, use `overwrite=True`.
        #             Otherwise, use `<collection>.append()`""")

        if data.empty:
            return
        wr.s3.to_parquet(
                        df=data,
                        path=path,
                        dataset=True,
                        mode ="append",
                        partition_cols =["s"],
                        index=False,
                    )
                    
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


  