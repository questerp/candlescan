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


def setwal(db):
    try:
        db.cursor().execute("pragma journal_mode=wal")
    except:
        pass

apsw.connection_hooks.append(setwal)

class Item(object):
    def __repr__(self):
        return "PyStore.item <%s/%s>" % (self.collection, self.item)

    def __init__(self, item, path,start=None,end=None, filters=None, columns=None,):
        self.item = item
        self.filters = filters or ''
        self.columns = columns
        self.path = path
        self.start = start
        self.end = end


    def snapshot(self,size,columns=None):
        sql = ""
        if columns:
            sql  = "select %s from bars where s=? order by t desc limit ?" % (",".join(columns))
        else:
            sql  = "select t,o,c,h,l,v from bars where s=? order by t desc limit ?"
        attrs =[self.item,size]
        data = []
        conn = apsw.Connection(self.path,flags = apsw.SQLITE_OPEN_READONLY)
        conn.setbusytimeout(2000)
        with conn:
            try:
                data=list( conn.cursor().execute(sql,attrs) )
            except Exception as e:
                print("error snapshot",e)
            finally:
                #conn.close()
                return data

    def data(self):
        if self.start is None:
            return []
        data = []
        attrs =[]
        if self.end:
            sql  = "select t,o,c,h,l,v from bars where s=? and t>=? and t<=?" + self.filters
            attrs = [self.item,self.start,self.end]
        else:
            sql  = "select t,o,c,h,l,v from bars where s=? and t>=?" + self.filters
            attrs = [self.item,self.start ]
        
        conn = apsw.Connection(self.path,flags = apsw.SQLITE_OPEN_READONLY)
        conn.setbusytimeout(2000)
        with conn:
            try:
                data=list( conn.cursor().execute(sql,attrs) )
            except Exception as e:
                print("error item",e)
            finally:
                #conn.close()
                return data

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

  
 
