

import sys
from distutils import sys
from sys import path
import pandas as pd
from pandas.io import sql

import time
from DataMiner import DataMiner
from IsodataHelpers import IsodataHelpers
from meterData import MeterData
import matplotlib
from datetime import datetime
from pytz import timezone
from GridCPShaving import GridCPShaving
from datetime import datetime, timedelta
def main():
    def putIsoData(dataMiner, isoHelper):
        print(" Fetch LMP, InstLoad --- 1")
        dataMiner.fetch_LMP(1, isoHelper)
        dataMiner.fetch_InstantaneousLoad(1, 'ps',isoHelper)
        dataMiner.fetch_InstantaneousLoad(1, 'PJM RTO',isoHelper)
        print("Fetch GenFuel --- 2")
        dataMiner.fetch_GenFuel(11, isoHelper)
        print("Fetch Meter Data --- 3")
        #dataMiner.fetch_hourlyMeteredLoad(True, 'CurrentYear', True,isoHelper)
        #dataMiner.fetch_hourlyMeteredLoad(False, 'CurrentYear', True,isoHelper)
        meterData.fetchMeterData('550001081', 1, isoHelper)
        #dataMiner.fetch_7dayLoadForecast(True, isoHelper)
        print("Fetch Load Forecast --- 4")
        dataMiner.fetch_LoadForecast( 'ps', isoHelper,GCPShave)
        #dataMiner.fetch_7dayLoadForecast(False, isoHelper)
        dataMiner.fetch_LoadForecast('PJM RTO', isoHelper,GCPShave)
        print("Print LMPs --- 5")
        df = isoHelper.getLmp_latest(nodeId='PSEG',numIntervals=6)

      
        print(df)
   
        return
   
    dataMiner = DataMiner()
    isoHelper = IsodataHelpers()
    meterData = MeterData()
    GCPShave = GridCPShaving()

    isoHelper.emptyAllTbls()
    
    eastern = timezone('US/Eastern')

    oldestTimeStamp =  datetime.now() - timedelta (hours =-1)
    #meterData.fetchMeterData('550001081', 1000, isoHelper)
    currentDate =datetime.today();
    #startMeteredPeriod =  datetime(currentDate.year-1, currentDate.month, 1)
    #endMeteredPeriod =  datetime(currentDate.year, currentDate.month, 1)
   
    #dataMiner.fetch_hourlyMeteredLoad(True, startMeteredPeriod, endMeteredPeriod, False, isoHelper)
    #dataMiner.fetch_hourlyMeteredLoad(False, startMeteredPeriod, endMeteredPeriod, False, isoHelper)

    #meterData.genHist('9214411', isoHelper)
    #dataMiner.genPSEGLoadHist(isoHelper)
    #dataMiner.genRTOLoadHist(isoHelper)
   
    #rng.strftime('%B %d, %Y, %r')
    #i=1
    #dataMiner.fetch_YrHrlyEvalLoadForecast(oldestTimeStamp, 'ps', isoHelper)
    #dataMiner.fetch_YrHrlyEvalLoadForecast(oldestTimeStamp, 'RTO', isoHelper)

    
    #dataMiner.fetch_LMP(8640, isoHelper)
    #dataMiner.fetch_InstantaneousLoad(4320, 'ps',isoHelper)
    #dataMiner.fetch_InstantaneousLoad(4320, 'PJM RTO',isoHelper)

    #dataMiner.fetch_GenFuel(528, isoHelper)
        #GCPShave.findPeaks(oldestTimeStamp, 'ps', False, False, isoHelper)
    #GCPShave.findPeaks(oldestTimeStamp, 'PJM RTO', False, False, isoHelper)
    #GCPShave.findPeaks(oldestTimeStamp,  'ps', True, False, isoHelper)
    #GCPShave.findPeaks(oldestTimeStamp, 'PJM RTO', True, False, isoHelper)
    #GCPShave.checkPeaks('ps', True, isoHelper)
    #GCPShave.checkPeaks('PJM RTO', True, isoHelper)

    
    dataMiner.fetch_LoadForecast( 'ps', isoHelper, GCPShave)
    dataMiner.fetch_LoadForecast('PJM RTO', isoHelper, GCPShave)
    ##dataMiner.fetch_7dayLoadForecast(True, isoHelper)
    



    #oldestMergeTimeStamp =  datetime.now() - timedelta (days =30)

    #isoHelper.mergePSEGTimeSeries(oldestMergeTimeStamp)
    #isoHelper.mergeRTOTimeSeries(oldestMergeTimeStamp)
    #isoHelper.mergePSEGHrlySeries(oldestMergeTimeStamp)
    #isoHelper.mergeRTOHrlySeries(oldestMergeTimeStamp)



    while True:
        putIsoData(dataMiner,isoHelper)
        time.sleep(60)

main()

