

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
import numpy as np
import matplotlib.pyplot as plt  
import time


from inverterHelper import regDataHelper as inverterDataHelper

import PID as PID
import os.path

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
   
 
        def updatePlot(pid):
        
            x = pid.model[0,:]
            new_y =  pid.model[3,:]
            new_y2 =  pid.model[2,:]

                       
            #plt.subplot(2,1,1)
	        # updating data values
            pid.hlOP.set_xdata(x)
            pid.hlOP.set_ydata(new_y)

            minVal = min(new_y)
            maxVal = max(new_y)

            plt.ylim(minVal,maxVal)
            plt.xlim(0, max(x))

            pid.axOP.plot(x,new_y, 'b:',label= 'OP')

            pid.hlPV.set_xdata(x)
            pid.hlPV.set_ydata(new_y2)

            minVal = min(new_y2)
            maxVal = max(new_y2)
            plt.ylim(minVal,maxVal)

            pid.axPV.plot(x,new_y2, 'r--', label= 'PV')


	        # drawing updated values
            pid.pltFig.canvas.draw()

	        # This will run the GUI event
	        # loop until all UI events
	        # currently waiting have been processed
            pid.pltFig.canvas.flush_events()

            time.sleep(0.1)

        def initPlot(pid):

            # creating initial data values
            # of x and y
            x = np.linspace(0, 100, 100)
            y = np.sin(x)
            y2 = 10 * np.cos(x)

            # to run GUI event loop
            plt.ion()

            pid.pltFig = plt.figure( figsize=(10, 8))

            pid.axOP = plt.subplot(211)

            pid.hlOP, = pid.axOP.plot(x, y, 'b:',label= 'OP')

            plt.ylabel('Current (Amps)')
            plt.grid()

            plt.legend()

            pid.axPV = plt.subplot(212, sharex=pid.axOP)
 
 
            pid.hlPV, = pid.axPV.plot(x, y2, 'r--',label= 'PV')

            plt.ylabel('Net Power (KW)')
            plt.xlabel('Time (minutes)')
            plt.grid()
            plt.legend()


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

        pidCharge = PID.PID(3.5, 1.2, 0.05)
        pidCharge.SetPoint = 13.6
        pidCharge.setSampleTime(30)

        pidCharge.output_limits = (0, 12)
        pidCharge.ITermAccumulate = 0


        #pidDisCharge = PID(-.7,  -0.015, 0.05, setpoint=0.0)
        #p, i, d = pidDisCharge.components
        #pidDisCharge.sample_time = 30 # Update every 30 second
        #pidDisCharge.output_limits = (0, 74)
        #print ("Initial p= ", p, " i = ", i, " d = ",d)

        targetT = 0
        #P =1.7
        #I = .08
        #D = .05

        P =1.5
        I = .1
        D = .05

        P =1.8
        I = .3
        D = .05

        P =1.6
        I =.4
        D = .0

        P =1.4
        I =.6
        D = .20

        pidDisCharge = PID.PID(P, I, D)
        pidDisCharge.SetPoint = targetT
        pidDisCharge.setSampleTime(30)   

        #inverterHelper.updateDBRegValues(modbusClient)
        initPlot(pidDisCharge)

        while True:

            psPeakOn, rtoPeakOn = putIsoData(dataMiner,isoHelper)

            print (' Fetch Meter Data')

            timestamp, RMS_Watts_Net, State_Watts_Dir = meterData.fetchMeterData('550001081', 1, isoHelper)

            if (RMS_Watts_Net != None):
             loadKW = RMS_Watts_Net / 1000

            if (modbusClient != None):
                if (psPeakOn == False and rtoPeakOn == False):
                    batteryVoltage, chargingCurrent =  inverterHelper.chargeBatteries(modbusClient, pidCharge)
                else:
                    loadKW, newAmps= inverterHelper.peakShave(modbusClient, timestamp, loadKW, State_Watts_Dir , isoHelper, pidDisCharge)
                    updatePlot(pidDisCharge)

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

            if (psPeakOn == False and rtoPeakOn == False):
                 time.sleep(60)
            else:
                time.sleep(30)


        valType, regValue =  inverterHelper.writeRegValue(modbusClient, 1002, 'int16', 1)

main()

