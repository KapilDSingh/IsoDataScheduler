
from oasisData import oasisData
import sys
from distutils import sys
from sys import path
import pandas as pd
from pandas.io import sql
import schedule
import time
from DataMiner import DataMiner
from IsodataHelpers import IsodataHelpers


def main():
    def putIsoData(dataMiner, isoHelper):

        dataMiner.fetch_LMP(1, isoHelper)
        dataMiner.fetch_InstantaneousLoad(4, isoHelper)
        dataMiner.fetch_GenFuel(11, isoHelper)

        lmpOasis = oasisData()
        
      
        df = isoHelper.getLmp_latest( nodeId='PSEG',numIntervals=6)
      
        print(df)
   
        #genDf = lmpOasis.get_generation()
        
        return




 
   
    dataMiner = DataMiner()
    isoHelper = IsodataHelpers()

    isoHelper.emptyAllTbls()

    dataMiner.fetch_LMP(1152, isoHelper)
    dataMiner.fetch_InstantaneousLoad(5000, isoHelper)
    dataMiner.fetch_GenFuel(20, isoHelper)

    while True:
        putIsoData(dataMiner,isoHelper)
        time.sleep(180)

main()

