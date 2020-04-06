

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

def main():
    def putIsoData(dataMiner, isoHelper):

        dataMiner.fetch_LMP(1, isoHelper)
        dataMiner.fetch_InstantaneousLoad(4, isoHelper)
        dataMiner.fetch_GenFuel(11, isoHelper)
        meterData.fetchMeterData('550001081', 1, isoHelper)
      
        df = isoHelper.getLmp_latest( nodeId='PSEG',numIntervals=6)
      
        print(df)
   

        
        return
   
    dataMiner = DataMiner()
    isoHelper = IsodataHelpers()
    meterData = MeterData()

    isoHelper.emptyAllTbls()

    dataMiner.fetch_LMP(1152, isoHelper)
    dataMiner.fetch_InstantaneousLoad(1152, isoHelper)
    dataMiner.fetch_GenFuel(528, isoHelper)
    meterData.fetchMeterData('550001081', 1000, isoHelper)

    while True:
        putIsoData(dataMiner,isoHelper)
        time.sleep(60)

main()

