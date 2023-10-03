

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

from inverterHelper import regDataHelper as inverterDataHelper
from simple_pid import PID


def main():
    def putIsoData(dataMiner, isoHelper):
        print(" Fetch LMP, InstLoad --- 1")
        dataMiner.fetch_LMP(1, isoHelper)
        dataMiner.fetch_InstantaneousLoad(1, 'ps',isoHelper)
        dataMiner.fetch_InstantaneousLoad(1, 'PJM RTO',isoHelper)

        print("Fetch GenFuel --- 2")
        dataMiner.fetch_GenFuel(11, isoHelper)

        print("Fetch Load Forecast --- 4")

        psPeakOn = dataMiner.fetch_LoadForecast( 'ps', isoHelper,GCPShave)
        rtoPeakOn = dataMiner.fetch_LoadForecast('PJM RTO', isoHelper,GCPShave)

        print("Print LMPs --- 5")
        df = isoHelper.getLmp_latest(nodeId='PSEG',numIntervals=6)

        print(df)
        return psPeakOn, rtoPeakOn
   
   
    dataMiner = DataMiner()
    isoHelper = IsodataHelpers()
    meterData = MeterData()
    GCPShave = GridCPShaving()
    inverterHelper = inverterDataHelper()

    relayState =0

    isoHelper.emptyAllTbls()
    
    eastern = timezone('US/Eastern')

    meterData.fetchMeterData('550001081', 1000, isoHelper)

    #meterData.genHist('9214411', isoHelper)
    #dataMiner.genPSEGLoadHist(isoHelper)
    #dataMiner.genRTOLoadHist(isoHelper)
   
    modbusClient =  inverterHelper.connectInverter()

    valType, regValue =  inverterHelper.writeRegValue(modbusClient, 1001, 'int16', 1)
    valType, regValue =  inverterHelper.writeRegValue(modbusClient, 1024, 'int16',0)

    pidCharge = PID(1, 0.1, 0.05, setpoint=13)
    pidCharge.sample_time = 30  # Update every 30 second
    pidCharge.output_limits = (0, 12)

    pidDisCharge = PID(-.7,  -0.015, 0.05, setpoint=0.0)
    p, i, d = pidDisCharge.components
    pidDisCharge.sample_time = 30 # Update every 30 second
    pidDisCharge.output_limits = (0, 74)
    print ("Initial p= ", p, " i = ", i, " d = ",d)

    #inverterHelper.updateDBRegValues(modbusClient)

    while True:

        psPeakOn, rtoPeakOn = putIsoData(dataMiner,isoHelper)

        print (' Fetch Meter Data')
        timestamp, loadKW, State_Watts_Dir = meterData.fetchMeterData('550001081', 1, isoHelper)

        if (modbusClient != None):
            if (psPeakOn == False and rtoPeakOn == False):
                batteryVoltage, chargingCurrent =  inverterHelper.chargeBatteries(modbusClient, pidCharge)
            else:
                loadKW, newAmps= inverterHelper.peakShave(modbusClient, timestamp, loadKW, State_Watts_Dir , isoHelper, pidDisCharge)


            #Results = isoHelper.call_procedure("[ISPeakShavingON]", [])
            #if (len(Results) == 1):
            #    timestamp = Results[0][0]
            #    Results = isoHelper.call_procedure("[TurnPeakShavingOn] ?", timestamp)
            #    if (len(Results) == 1):
            #       relayState =1

            #Results = isoHelper.call_procedure("[ISPeakShavingOFF]", [])
            #if (len(Results) == 1):
            #    timestamp = Results[0][0]
            #    Results = isoHelper.call_procedure("[ChkShavingOff] ?", timestamp)
            #    if (len(Results) == 0):
            #        Results = isoHelper.call_procedure("[ChkLoadDecreasing] ?", timestamp)
            #        if (len(Results) == 1):
            #                relayState = 0
            #                Results = isoHelper.call_procedure("[UpdateShaveTimes] ?",timestamp)

            #        else:
            #            Results = isoHelper.call_procedure("[ChkOverTime] ?", timestamp)
            #            if (len(Results) ==1):
            #                    relayState = 0
            #                    Results = isoHelper.call_procedure("[UpdateShaveTimes] ?",timestamp)
              
        #isoHelper.SetRelayState(relayState)

        time.sleep(60)


    valType, regValue =  inverterHelper.writeRegValue(modbusClient, 1002, 'int16', 1)

main()

