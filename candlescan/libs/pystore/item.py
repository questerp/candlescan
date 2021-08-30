#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# PyStore: Flat-file datastore for timeseries data
# https://github.com/ranaroussi/pystore
#
# Copyright 2018-2020 Ran Aroussi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# import dask.dataframe as dd
import pandas as pd
# import pyarrow.parquet as pq
import os
from . import utils
import apsw


class Item(object):
    def __repr__(self):
        return "PyStore.item <%s/%s>" % (self.collection, self.item)

    def __init__(self, item, path,start, filters=None, columns=None,):
                 
        # self.engine = engine
        # self.datastore = datastore
        # self.collection = collection
        self.item = item
        self.filters = filters or ''
        self.columns = columns
        self.path = path
        self.start = start

        # self._path  =  utils.make_path(datastore, collection, "data") 
        # print("self._path",self._path)
        # if not self._path.exists():
        #     raise ValueError(
        #         "Item `%s` doesn't exist. "
        #         "Create it using collection.write(`%s`, data, ...)" % (
        #             item, item))
    def data(self):
        conn = apsw.Connection(self.path)
        conn.setbusytimeout(5000)
        data = []
        sql  = "select t,o,c,h,l,v from bars where s=? and t>=?" + self.filters
        with conn:
            try:
                rows=list( conn.cursor().execute(sql,[self.item,self.start]) )
                return rows
            except Exception as e:
                print("error item",e)
                return []


        # df = pd.DataFrame()
        # for path in self._paths:
        #     # if os.path.exists(path):
        #     with lock:
        #         with pd.HDFStore(path) as store:
        #             try:
        #                 filters = 's == ticker' + (' & '+self.filters  if self.filters else '')
        #                 data = store.select('table',where= filters, auto_close=True,columns=self.columns)
        #                 df = pd.concat([df, data], ignore_index=True) 
        #             except Exception as e:
        #                 print("error item",e)
            
        # wheres = [" 't==%s'"%self.item] + (self.filters or [])
        # print(wheres)
        # data = pd.read_hdf(self._path,key="table",where=wheres,columns=self.columns) # pq.read_pandas(self._path,filters=self.filters,columns=self.columns)
        # return df.drop_duplicates("t")
        #df = dataset.to_table(columns=columns).to_pandas()
        # self.metadata = utils.read_metadata(self._path)
        # print("self._path",self._path)
        # self.data = dd.read_parquet(
        #     self._path, engine=self.engine, filters=filters, columns=columns)

  
 
