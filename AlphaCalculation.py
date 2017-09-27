import cx_Oracle
import pandas as pd
import numpy as np
import math


class AlphaCalculation(object):

	def __init__(self, start_date, end_date):
        self.conn_factor = cx_Oracle.connect('c##factor/sensegain@10.9.6.44/wind')
        self.cursor_factor = self.conn_factor.cursor()

        self.conn_wind = cx_Oracle.connect('c##sensegain/sensegain@10.9.6.44/wind')
        self.cursor_wind = self.conn_wind.cursor()
		
		fileds = 'WHERE TRADE_DT >=' + '\''+start_date+'\'' + ' and TRADE_DT < ' + '\''+end_date+'\''
		sqlstr = 'SELECT A.*, B.S_VAL_PE, B.S_VAL_PB_NEW, B.S_VAL_PE_TTM, B.S_DQ_FREETURNOVER, B.S_PQ_ADJHIGH_52W, B.S_PQ_ADJLOW_52W, \
		C.S_LI_INITIATIVEBUYRATE, C.S_LI_INITIATIVEBUYMONEY, C.S_LI_INITIATIVEBUYAMOUNT, C.S_LI_INITIATIVESELLRATE, C.S_LI_INITIATIVESELLMONEY, \
		C.S_LI_INITIATIVESELLAMOUNT, C.S_LI_LARGEBUYRATE, C.S_LI_LARGEBUYMONEY, C.S_LI_LARGEBUYAMOUNT, C.S_LI_LARGESELLRATE, C.S_LI_LARGESELLMONEY, \
		C.S_LI_LARGESELLAMOUNT \
		FROM( \
			SELECT S_INFO_WINDCODE, TRADE_DT, S_DQ_VOLUME, S_DQ_AMOUNT, S_DQ_ADJPRECLOSE, S_DQ_ADJOPEN, S_DQ_ADJHIGH, S_DQ_ADJLOW, S_DQ_ADJCLOSE, S_DQ_AVGPRICE \
			FROM AShareEODPrices ' + fileds + ' and S_DQ_VOLUME > 0' + \
		') A \
		LEFT OUTER JOIN( \
			SELECT S_INFO_WINDCODE, TRADE_DT, S_VAL_PE, S_VAL_PB_NEW, S_VAL_PE_TTM, S_DQ_FREETURNOVER, S_PQ_ADJHIGH_52W, S_PQ_ADJLOW_52W \
			FROM AShareEODDerivativeIndicator ' + fileds + \
		') B \
		ON A.S_INFO_WINDCODE = B.S_INFO_WINDCODE AND A.TRADE_DT = B.TRADE_DT \
		LEFT OUTER JOIN( \
			SELECT S_INFO_WINDCODE, TRADE_DT, S_LI_INITIATIVEBUYRATE, S_LI_INITIATIVEBUYMONEY, S_LI_INITIATIVEBUYAMOUNT, S_LI_INITIATIVESELLRATE, S_LI_INITIATIVESELLMONEY, \
			S_LI_INITIATIVESELLAMOUNT, S_LI_LARGEBUYRATE, S_LI_LARGEBUYMONEY, S_LI_LARGEBUYAMOUNT, S_LI_LARGESELLRATE, S_LI_LARGESELLMONEY, S_LI_LARGESELLAMOUNT, \
			FROM AShareL2Indicators ' + fileds + \
		') C \
		ON A.S_INFO_WINDCODE = C.S_INFO_WINDCODE AND A.TRADE_DT = C.TRADE_DT \
		'
        
		self.cursor_factor.execute(sqlstr)
        self.finaldata = self.cursor_factor.fetchall()
        # trading_days = self.get_trading_days()
        # index = list(set(data['TRADE_DT'].drop_duplicates().tolist()) & set(trading_days))
        # data = data[data['TRADE_DT'].isin(index)]
		# 数据抽取到retdata中
		self.finaldata['SORT_ID'] = self.finaldata[['TRADE_DT']].groupby(self.finaldata['S_INFO_WINDCODE']).rank()
		self.factors = self.finaldata[['S_INFO_WINDCODE', 'TRADE_DT']]
		self.stocks = np.unique(self.finaldata['S_INFO_WINDCODE'].values)
		self.inf = float("-inf")
		
		def write_Deviation(self, period):
		#背离因子计算，主要都量价背离、量幅背离、买卖背离、委托背离等
			delta2Factor=[]
			for stock in self.stocks:
				deltaStock=[]
				retdata = self.finaldata[self.finaldata['S_INFO_WINDCODE'] == stock].sort_values(by='SORT_ID').set_index('SORT_ID')
				
				deltavolumebase = pd.Series(np.zeros(len(retdata)))					#今昨天成交量增量
				deltaamountbase = pd.Series(np.zeros(len(retdata)))					#今昨天金额增量
				
				deltamainbuyvolume = pd.Series(np.zeros(len(retdata)))				#今昨天主买成交量增量
				deltamainbuyamount = pd.Series(np.zeros(len(retdata)))				#今昨天主买金额增量
				deltamainsalevolume = pd.Series(np.zeros(len(retdata)))				#今昨天主卖成交量增量
				deltamainsaleamount = pd.Series(np.zeros(len(retdata)))				#今昨天主卖金额增量
				deltamainbuyratio = pd.Series(np.zeros(len(retdata)))				#今昨天主买比率增量
				
				deltabigbuyvolume = pd.Series(np.zeros(len(retdata)))				#今昨天大买成交量增量
				deltabigbuyamount = pd.Series(np.zeros(len(retdata)))				#今昨天大买金额增量
				deltabigsalevolume = pd.Series(np.zeros(len(retdata)))				#今昨天大卖成交量增量
				deltabigsaleamount = pd.Series(np.zeros(len(retdata)))				#今昨天大卖金额增量
				deltabigbuyratio = pd.Series(np.zeros(len(retdata)))				#今昨天大买比率增量
				deltabigsaleratio = pd.Series(np.zeros(len(retdata)))				#今昨天大卖比率增量
				
				
				volumebase = pd.Series(np.zeros(len(retdata)))				#当日成交量
				amountbase = pd.Series(np.zeros(len(retdata)))				#当日成交金额
				
				mainbuyvolume = pd.Series(np.zeros(len(retdata)))			#当日主买总量
				mainbuyamount = pd.Series(np.zeros(len(retdata)))			#当日主买金额
				mainsalevolume = pd.Series(np.zeros(len(retdata)))			#当日主卖总量
				mainsaleamount = pd.Series(np.zeros(len(retdata)))			#当日主卖金额
				mainbuyratio = pd.Series(np.zeros(len(retdata)))			#当日主买比率
				
				bigbuyvolume = pd.Series(np.zeros(len(retdata)))			#当日大买总量
				bigbuyamount = pd.Series(np.zeros(len(retdata)))			#当日大买金额
				bigsalevolume = pd.Series(np.zeros(len(retdata)))			#当日大卖总量
				bigsaleamount = pd.Series(np.zeros(len(retdata)))			#当日大卖金额
				bigbuyratio = pd.Series(np.zeros(len(retdata)))				#当日大买比率
				bigsaleratio = pd.Series(np.zeros(len(retdata)))			#当日大卖比率
				
				
				openclosechg = pd.Series(np.zeros(len(retdata)))			#当日收益范围(相对于开盘价)
				precloseclosechg = pd.Series(np.zeros(len(retdata)))		#当日收益范围(相对于昨收盘)
				avgclosechg = pd.Series(np.zeros(len(retdata)))				#当日相对均价的收益
				
				lowhighchg = pd.Series(np.zeros(len(retdata)))				#当日最低到最高
				closehighchg = pd.Series(np.zeros(len(retdata)))			#当日收盘到最高
				
				
				
				
				
				for rak in range(len(retdata)):
					if rak > 0:
						deltavolumebase[rak] = math.log(retdata.loc[rak]['S_DQ_VOLUME']) - math.log(retdata.loc[rak-1]['S_DQ_VOLUME'])
						deltaamountbase[rak] = math.log(retdata.loc[rak]['S_DQ_AMOUNT']) - math.log(retdata.loc[rak-1]['S_DQ_AMOUNT'])
					else:
						deltavolumebase[rak] = self.inf
						deltaamountbase[rak] = self.inf
					
					volumebase[rak] = math.log(retdata.loc[rak]['S_DQ_VOLUME'])
					amountbase[rak] = math.log(retdata.loc[rak]['S_DQ_AMOUNT'])
					
					pricechg[rak] = (retdata.loc[rak]['S_DQ_ADJCLOSE'] - retdata.loc[rak]['S_DQ_ADJOPEN'])/retdata.loc[rak]['S_DQ_ADJOPEN']
					gainchg[rak] = (retdata.loc[rak]['S_DQ_ADJCLOSE'] - retdata.loc[rak]['S_DQ_ADJPRECLOSE'])/retdata.loc[rak]['S_DQ_ADJPRECLOSE']
					rangechg[rak] = (retdata.loc[rak]['S_DQ_ADJHIGH'] - retdata.loc[rak]['S_DQ_ADJLOW'])/retdata.loc[rak]['S_DQ_ADJLOW']
				
					deltaStock = [retdata.loc[rak]['S_INFO_WINDCODE'], retdata.loc[rak]['TRADE_DT']]
					#从第period+1个开始计算,计算delta的量价背离
					if rak + 1 > period:
						#差量价背离因子
						deltaVol = deltavolumebase[rak-period+1:rak+1].corr(pricechg[rak-period+1:rak+1])
					
			
					
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
			
		
		
		
