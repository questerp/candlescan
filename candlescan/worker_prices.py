import candlescan.yahoo_finance_api2 as yf

def get_prices(symbol,period_type, period, frequency_type, frequency):
	if not (symbol or period_type or period or frequency_type or frequency):
		return
	
	share = yf.Share(symbol)
	data = share.get_historical(period_type,period,frequency_type,frequency,"dict")
	return data
