
from oasisData import oasisData
import sys
from distutils import sys
from sys import path
import pandas as pd
from pandas.io import sql
import schedule
import time


def main():
    def putIsoData(lmpOasis, prevHour, PrevHourlyAvgLMP):
        lmpData = lmpOasis.fetch_oasis_data(dataType = 'LMP')

        currentHour = lmpData.timestamp.dt.hour.PSEG
        currentHourlyLMPAvg=lmpData["Hourly Integrated LMP"].PSEG
        
        if (currentHour !=  prevHour) :
            prevHour = currentHour
            
            lmpData["Hourly Integrated LMP"].PSEG = PrevHourlyAvgLMP
            
            prevHourlyAvgLMP = currentHourlyLMPAvg
            
            
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
        return prevHourlyAvgLMP, prevHour;




 
    prevHour = -1;
    PrevHourlyAvgLMP =0;
    lmpOasis = oasisData()
    #putIsoData(lmpOasis)

   # schedule.every(1).minutes.do(putIsoData , lmpOasis)
    while True:
        prevHourlyAvgLMP, prevHour = putIsoData(lmpOasis, prevHour,PrevHourlyAvgLMP)
        time.sleep(180)

main()

