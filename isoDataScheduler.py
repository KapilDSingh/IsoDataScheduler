

import sys
from distutils import sys
from sys import path
import pandas as pd
from pandas.io import sql

from DataMiner import DataMiner
from IsodataHelpers import IsodataHelpers
from PID import initChargePid, initDisChargePid
from graphics import graphics
from meterData import MeterData
import matplotlib
from datetime import datetime
from pytz import timezone
from GridCPShaving import GridCPShaving
from datetime import datetime, timedelta
import numpy as np
import matplotlib.pyplot as plt  
from simple_pid import PID as Pid
import time

from inverterHelper import regDataHelper as inverterDataHelper

import os.path


from enum import Enum
 
class States(Enum):
    CHARGING = 1
    DISCHARGING = 2
    INACTIVE = 3

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

        Graphics = graphics()
        InverterState = States.INACTIVE

        isoHelper.emptyAllTbls()
    
        eastern = timezone('US/Eastern')

        meterData.fetchMeterData('550001081', 1000, isoHelper)

        #meterData.genHist('9214411', isoHelper)
        #dataMiner.genPSEGLoadHist(isoHelper)
        #dataMiner.genRTOLoadHist(isoHelper)
   
        modbusClient =  inverterHelper.connectInverter()

        valType, regValue =  inverterHelper.writeRegValue(modbusClient, 1001, 'int16', 1)

        valType, regValue =  inverterHelper.writeRegValue(modbusClient, 1003, 'int16', 1)
        valType, regValue =  inverterHelper.writeRegValue(modbusClient, 1024, 'int16',0)

        #pidDisCharge = PID(-.7,  -0.015, 0.05, setpoint=0.0)
        #p, i, d = pidDisCharge.components
        #pidDisCharge.sample_time = 30 # Update every 30 second
        #pidDisCharge.output_limits = (0, 74)
        #print ("Initial p= ", p, " i = ", i, " d = ",d)

        P =1.4
        I =.6
        D = 0.2


        #inverterHelper.updateDBRegValues(modbusClient)

        Graphics.initPlot()

        while True:

            print ('Inverter State = ', InverterState.name )

            if (InverterState == States.INACTIVE):
                  pidCharge = initChargePid(inverterHelper, modbusClient)
                  pidDisCharge = initDisChargePid(inverterHelper, modbusClient)
                  Graphics.zeroModel()


            psPeakOn, rtoPeakOn = putIsoData(dataMiner,isoHelper)
            print (" psPeakOn = ",psPeakOn, "  rtoPeakOn =  ", rtoPeakOn)

            timestamp, RMS_Watts_Net, State_Watts_Dir = meterData.fetchMeterData('550001081', 1, isoHelper)

            if (RMS_Watts_Net != None):
             loadKW = RMS_Watts_Net / 1000

            if (modbusClient != None):

                currentTime = datetime.now()
                startChargeTime = currentTime.replace(hour =19, minute=00, second = 0, microsecond =0)
                endChargeTime = currentTime.replace (hour = 22, minute=0, second = 0, microsecond =0)

                if (psPeakOn == False and rtoPeakOn == False):
                    ### Set to TRUE TO CHARGE BATTERIES
                   if  (False): #((currentTime >=startChargeTime ) and (currentTime <= endChargeTime)):

                       if  (InverterState != States.CHARGING):
                            Graphics.StartTime = time.time()
                            InverterState = States.CHARGING
                            pidCharge.reset()
                            pidCharge = initChargePid(inverterHelper, modbusClient)   
                            Graphics.zeroModel()


                       batteryVoltage, chargingCurrent =  inverterHelper.chargeBatteries(modbusClient, pidCharge, Graphics)
                       
                   else:
                        InverterState = States.INACTIVE

                else:                    
                    if  (InverterState != States.DISCHARGING):
                            Graphics.StartTime = time.time()
                            InverterState = States.DISCHARGING
                            pidDisCharge.reset()
                            pidDisCharge = initDisChargePid(inverterHelper, modbusClient)    
                            Graphics.zeroModel()                            

                    loadKW, newAmps= inverterHelper.peakShave(modbusClient, loadKW, State_Watts_Dir , pidDisCharge, Graphics)

            if (psPeakOn == False and rtoPeakOn == False):
                 time.sleep(60)
            else:
                time.sleep(60)


        valType, regValue =  inverterHelper.writeRegValue(modbusClient, 1002, 'int16', 1)

main()

