

import sys
from distutils import sys
from sys import path
import pandas as pd
from pandas.io import sql
import schedule
import time
from DataMiner import DataMiner
from IsodataHelpers import IsodataHelpers
from meterData import MeterData
import matplotlib

def main():
    def putIsoData(dataMiner, isoHelper):

        dataMiner.fetch_LMP(1, isoHelper)
        dataMiner.fetch_InstantaneousLoad(1, 'ps',isoHelper)
        dataMiner.fetch_InstantaneousLoad(1, 'PJM RTO',isoHelper)

        dataMiner.fetch_GenFuel(11, isoHelper)
        dataMiner.fetch_hourlyMeteredLoad(True, 'CurrentYear', True,isoHelper)
        dataMiner.fetch_hourlyMeteredLoad(False, 'CurrentYear', True,isoHelper)
        meterData.fetchMeterData('550001081', 1, isoHelper)
        #dataMiner.fetch_7dayLoadForecast(True, isoHelper)
        dataMiner.fetch_LoadForecast(True, isoHelper)
        #dataMiner.fetch_7dayLoadForecast(False, isoHelper)
        dataMiner.fetch_LoadForecast(False, isoHelper)
        #startDateTime = pd.datetime(2019,7,1)
        #isoHelper.mergePSEGTimeSeries(startDateTime)
        #isoHelper.mergeRTOTimeSeries(startDateTime)

        df = isoHelper.getLmp_latest(nodeId='PSEG',numIntervals=6)

      
        print(df)
   

        
        return
   
    dataMiner = DataMiner()
    isoHelper = IsodataHelpers()
    meterData = MeterData()

    isoHelper.emptyAllTbls()

    meterData.fetchMeterData('550001081', 1000, isoHelper)

    dataMiner.fetch_hourlyMeteredLoad(True, 'LastYear', False, isoHelper)
    dataMiner.fetch_hourlyMeteredLoad(False, 'LastYear', False, isoHelper)

    #meterData.genHist('9214411', isoHelper)
    dataMiner.genPSEGLoadHist(isoHelper)
    dataMiner.genRTOLoadHist(isoHelper)
   
    #rng.strftime('%B %d, %Y, %r')
    i=1
    

    dataMiner.fetch_LMP(8640, isoHelper)
    dataMiner.fetch_InstantaneousLoad(8640, 'ps',isoHelper)
    dataMiner.fetch_InstantaneousLoad(8640, 'PJM RTO',isoHelper)

    dataMiner.fetch_GenFuel(528, isoHelper)

    
    dataMiner.fetch_LoadForecast(True, isoHelper)
    dataMiner.fetch_LoadForecast(False, isoHelper)
    dataMiner.fetch_7dayLoadForecast(True, isoHelper)
    
    startDateTime = pd.datetime(2020,1,1)
    isoHelper.mergePSEGTimeSeries(startDateTime)
    isoHelper.mergeRTOTimeSeries(startDateTime)

    while True:
        putIsoData(dataMiner,isoHelper)
        time.sleep(60)

main()

