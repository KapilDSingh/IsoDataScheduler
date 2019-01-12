from oasisData import oasisData
import pandas as pd
import sqlite3
from pandas.io import sql
from pandas.lib import Timestamp



lmpOasis = oasisData()
lmpData = lmpOasis.fetch_oasis_data(dataType = 'LMP')
loadData = lmpOasis.fetch_oasis_data(dataType = 'LOAD')
print(lmpData)
print(loadData)

soup = lmpOasis.fetch_markets_operations_soup()
 
lmpDf = lmpData.reset_index()
loadDf = loadData.reset_index()

lmpOasis.saveDf(DataTbl='lmpTbl', Data= lmpDf)
 
#df1 = lmpOasis.getLmp_period(start = "2018-07-26 13:10:00.000000", end = "2018-07-26 13:20:00.000000", nodeId='PSEG')
df1 = lmpOasis.getLmp_latest( nodeId='PSEG',numIntervals=6)

print (df1)

