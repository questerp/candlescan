from __future__ import unicode_literals
import frappe
import json
import random
from candlescan.utils.get_tickers import get_tickers as gt
import requests
from candlescan.utils.candlescan import insert_symbol
from candlescan.utils.yf import YahooFinancials as YF
import requests
import sys
import calendar
import re
from json import loads, dumps
import time
from bs4 import BeautifulSoup
import datetime
import pytz
import socketio
import asyncio
from candlescan.utils.socket_utils import get_user, validate_data, build_response, json_encoder, keep_alive
from candlescan.utils.candlescan import get_active_symbols, clear_active_symbols, get_yahoo_prices as get_prices
from secedgar.cik_lookup import get_cik_map
import feedparser
from alpaca_trade_api.rest import REST, TimeFrame
from frappe.utils import cstr, today, add_days, getdate, add_months
from frappe.realtime import get_redis_server
from alpaca_trade_api.common import URL
from candlescan.services.price_service import get_minute_bars

sio = socketio.AsyncClient(logger=True, json=json_encoder, engineio_logger=True, reconnection=True,
                           reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)
api = None


def start():
    asyncio.get_event_loop().run_until_complete(run())
    asyncio.get_event_loop().run_forever()


async def run():
    try:
        await sio.connect('http://localhost:9002', headers={"microservice": "market_service"})
        await keep_alive()
    except socketio.exceptions.ConnectionError as err:
        print("error", sio.sid, err)
        await sio.sleep(5)
        await run()


@sio.event
async def connect():
    print("I'm connected!")


@sio.event
async def get_filings(message):
    source = message.get("source_sid")
    symbol = message.get("data")
    if not symbol:
        return
    symbol = symbol.upper()
    frappe.db.commit()
    cik = frappe.db.get_value("Symbol", symbol, "cik")
    # print("cik",cik)

    if cik:
        url = "https://sec.report/CIK/%s.rss" % cik
        data = feedparser.parse(url)
        # print("data",data)
        if not data:
            return
        data = json.dumps(data)
        await sio.emit("transfer", build_response(message, data, "get_filings"))
    else:
        await sio.emit("transfer", build_response(message, False, "get_filings"))



@sio.event
async def get_symbol_prices(message):
    data = message.get("data")
    if not data:
        return []
    symbol = data.get("symbol")

    #frequency = data.get("frequency")
    start = data.get("start")
    end = data.get("end")
    timeframe = data.get("timeframe")

    if not (symbol or start):
        await sio.emit("transfer", build_response(message, "Invalid data for: %s" % symbol, "errors"))
        return

    data = get_minute_bars(symbol, timeframe, start, end)
    await sio.emit("transfer", build_response(message, data, "get_symbol_prices"))


@sio.event
async def get_symbol_info(message):
    symbol = message.get("data")
    source = message.get('source_sid')

    if not (symbol and source):
        return

    # return data fields
    fields = ' ,'.join(["name", "stock_summary_detail", "key_statistics_data", "key_price_data", "key_summary_data",
                       "website", "summary", "industry_type", "company", "country", "floating_shares", "sector", "exchange"])
    data = frappe.db.sql(""" select {0} from tabSymbol where symbol='{1}' limit 1 """.format(
        fields, symbol), as_dict=True)
    if data and len(data) > 0:
        await sio.emit("transfer", build_response(message, data, "get_symbol_info"))


def process_cik():
    ciks = get_cik_map()
    if ciks:
        tickers = ciks["ticker"]
        for sym in tickers:
            cik = tickers[sym]
            # print(sym,cik)
            if frappe.db.exists("Symbol", sym):
                frappe.db.set_value("Symbol", sym, "cik", cik)
        frappe.db.commit()
        print("Done CIK!")


def process_tickers():
    # NYSE=True, NASDAQ=True, AMEX=True
    api = REST(raw_data=True)
    assets = api.list_assets()
    cik = False
    for ticker in assets:
        #ticker['symbol'] = ticker['symbol'].replace('^','p')
        #ticker['name'] = (ticker['name'][:100] + '..') if len(ticker['name']) > 100 else ticker['name']
        symbol = ticker['symbol']
        
        exist = frappe.db.exists("Symbol", symbol)
        # print(ticker['symbol'],exist)
        existIndic = frappe.db.exists("Indicators", symbol)
        active = (ticker["status"] == 'active')
        if not exist:
            cik = True
            symbolobj = frappe.get_doc({
                'doctype': 'Symbol',
                'active': active,
                'symbol': symbol,
                'company': ticker['name'],
                'exchange': ticker['exchange'],
                'market_class': ticker['class']
            })
            insert_symbol(symbolobj)

        if not existIndic and active:
            indic = frappe.get_doc({
                'doctype': 'Indicators',
                'symbol': symbol
            }).insert()

		# update status
        status = frappe.db.get_value("Symbol",symbol,"active")
        if status != active or 'DELISTED' in symbol:
            frappe.db.set_value("Symbol",symbol,"active",active)
            status=False
    inactive = frappe.get_all("Symbol",filters=[("active","=",0)])
    for inc in inactive:
        frappe.db.delete("Indicators", {"name": inc['name']})
        print("delete",inc['name'])


    if cik:
        print("Processing CIK")
        process_cik()
    frappe.db.commit()
    clear_active_symbols()
    print("Done process_tickers!")
