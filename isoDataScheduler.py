
from oasisData import oasisData
import sys
from distutils import sys
from sys import path
import pandas as pd
from pandas.io import sql
import schedule
import time


def main():
    def putIsoData(lmpOasis):
        lmpData = lmpOasis.fetch_oasis_data(dataType = 'LMP')
        loadData = lmpOasis.fetch_oasis_data(dataType = 'LOAD')
        #genmixData = get_generation()
        lmpDf = lmpData.reset_index()
        loadDf = loadData.reset_index()
        lmpOasis.saveDf(DataTbl='lmpTbl', Data= lmpDf)
        lmpOasis.saveDf(DataTbl='loadTbl', Data= loadDf)
        df = lmpOasis.getLmp_latest( nodeId='PSEG',numIntervals=6)
        #df2 = lmpOasis.getLmp_period()
        print(df)
        #print(df2)
        genDf = lmpOasis.get_generation()
        tradeDf = lmpOasis.get_trade();


        print(genDf)
        print (tradeDf)
        lmpOasis.saveDf(DataTbl='genTbl', Data= genDf)
        lmpOasis.saveDf(DataTbl='tradeTbl', Data= tradeDf)





 

    lmpOasis = oasisData()
    #putIsoData(lmpOasis)

    schedule.every(1).minutes.do(putIsoData , lmpOasis)
    while True:
        schedule.run_pending()
     #   time.sleep(10)

main()

