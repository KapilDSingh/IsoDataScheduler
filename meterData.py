import urllib3
import urllib
from _datetime import datetime
from sqlalchemy.dialects.mssql import pyodbc
import json
from bs4 import BeautifulSoup
import pandas as pd
from dateutil.parser import parse
import pytz
from datetime import datetime, timedelta
import re
from sqlalchemy import create_engine
import sqlalchemy
import sys
import pyodbc
import requests

from pytz import timezone
import base64
import requests
from urllib.parse import urlencode, quote_plus,urlparse, parse_qsl
import pyperclip
import IsodataHelpers 
import matplotlib.pyplot as plt
import numpy as np
from pprint import pprint as pp

class MeterData(object):

    http = urllib3.PoolManager()


    
    def fetchMeterData(self, meter,  numReads, isoHelper):

        try:
            timestamp = None
            RMS_Watts_Net = None
            State_Watts_Dir = None

            numReads = str(numReads)     

            r = self.http.request('GET','https://io.ekmpush.com/readMeter?key=NjUyMzc0MjQ6Z3NaVmhEd20&meters='+ meter + '&ver=v4&fmt=json&cnt=' + numReads + \
                '&tz=America~New_York&fields=RMS_Watts_Tot~RMS_Watts_Ln_1~RMS_Watts_Ln_2~RMS_Watts_Ln_3~State_Watts_Dir&status=good ')

            jsonMeter = json.loads(r.data)

            jsonElectricData = jsonMeter['readMeter']['ReadSet'][0]['ReadData']

            meterDf = pd.DataFrame(jsonElectricData)

            meterDf.insert (1, 'timestamp',  pd.to_datetime(meterDf['Date'] + ' '+meterDf['Time']),allow_duplicates = False) 

            meterDf.insert(1, 'MeterID', meter, allow_duplicates = True)
     
            timezone = pytz.timezone("America/New_York")
      
            meterDf['timestamp'] = [pd.Timestamp.round(row, 'min') for row in  meterDf['timestamp']]
            meterDf['timestamp'] = [timezone.localize(row) for row in  meterDf['timestamp']]

            meterDf.drop(columns=['Good','Date', 'Time','Time_Stamp_UTC_ms'], inplace=True)

            meterDf.insert(len(meterDf.columns) - 1, 'RMS_Watts_Net' ,0 )
            meterDf = meterDf.astype({'RMS_Watts_Tot': float, 'RMS_Watts_Ln_1': float, 'RMS_Watts_Ln_2': float, 'RMS_Watts_Ln_3': float,'RMS_Watts_Net': float, 'State_Watts_Dir': int})

            oldestTimestamp =meterDf['timestamp'].min()
            newestTimeStamp = meterDf['timestamp'].max()

            isoHelper.clearTbl(oldestTimestamp, 'meterTbl')

            for index, row in meterDf.iterrows():

                timestamp = pd.to_datetime(row["timestamp"])

                RMS_Watts_Tot = row['RMS_Watts_Tot']

                State_Watts_Dir = row['State_Watts_Dir']

                Watts_Ln_1 = row['RMS_Watts_Ln_1']

                Watts_Ln_2 = row['RMS_Watts_Ln_2']

                Watts_Ln_3 = row['RMS_Watts_Ln_3']
                
                #Line1/Line2/Line3
                #Forward/Forward/Forward = 1
                #Forward/Forward/Reverse = 2
                #Forward/Reverse/Forward = 3
                #Reverse/Forward/Forward = 4
                #Forward/Reverse/Reverse = 5
                #Reverse/Forward/Reverse = 6
                #Reverse/Reverse/Forward = 7
                #Reverse/Reverse/Reverse = 8

                if (State_Watts_Dir == 1):
                    RMS_Watts_Net = Watts_Ln_1 + Watts_Ln_2 + Watts_Ln_3

                elif (State_Watts_Dir == 2):
                    RMS_Watts_Net = Watts_Ln_1 + Watts_Ln_2 - Watts_Ln_3

                elif (State_Watts_Dir == 3):
                    RMS_Watts_Net = Watts_Ln_1 - Watts_Ln_2 + Watts_Ln_3

                elif (State_Watts_Dir == 4):
                    RMS_Watts_Net = -Watts_Ln_1 + Watts_Ln_2 + Watts_Ln_3

                elif (State_Watts_Dir == 5):
                    RMS_Watts_Net = Watts_Ln_1 -  Watts_Ln_2 - Watts_Ln_3

                elif (State_Watts_Dir == 6):
                    RMS_Watts_Net = -Watts_Ln_1 + Watts_Ln_2 - Watts_Ln_3

                elif (State_Watts_Dir == 7):
                    RMS_Watts_Net = -Watts_Ln_1  -Watts_Ln_2 + Watts_Ln_3

                elif (State_Watts_Dir == 8):
                    RMS_Watts_Net = -Watts_Ln_1  -Watts_Ln_2  -Watts_Ln_3

                else:
                     RMS_Watts_Net =0

                meterDf.at[index, 'RMS_Watts_Net'] = RMS_Watts_Net

            print ("RMS_Watts_Tot = ", RMS_Watts_Tot, "  RMS_Watts_Net", RMS_Watts_Net,  "  State_Watts_Dir", State_Watts_Dir)


            isoHelper.saveDf('meterTbl', meterDf)

            meterDf.set_index("timestamp", inplace = True)

            if (len(meterDf.index) != 1):

                timestamp = None
                RMS_Watts_Net = None
                State_Watts_Dir = None

            self.get_current_hr_consumption(meter, meterDf, isoHelper)

        except Exception as e:
          print(e)
          print("Fetch Meter Data Unexpected error:",e)

        finally:
            return timestamp, RMS_Watts_Net, State_Watts_Dir


    

    def LoadCurveUtilityMeter(self, meter,  startMonth, endMonth, isoHelper, include):


        meterDf = isoHelper.getPSEGMeterData(meter, startMonth,endMonth,include)
        KWArr = meterDf['KW']
        KW=np.sort(KWArr)
        
        bins = np.arange(0,38,1)
        dist = np.histogram(KW,bins=bins, density=True)
        x=dist[0]
  
        xSum=np.sum(x)

        x=np.insert(x,0,0)
        y=dist[1]

        xCum = np.cumsum(x)
        
        flipxCum = 1-xCum
        flipxCum =list(map(lambda x : x if (x > 0.0000001) else 0, flipxCum));
                         
        Type=str(startMonth) + "-" + str(endMonth) + str(include);

        histDf = pd.DataFrame({'Type':Type, 'Percentile':flipxCum, 'KW':y})
        isoHelper.saveDf(DataTbl='ConsumptionHist', Data= histDf);
              
        return flipxCum, y

    def genHist(self, meter, isoHelper):
        engine = create_engine('mssql+pyodbc://Kapil:Acfjo12#@ISODSN');
        connection = engine.connect()

        result = connection.execute("delete from ConsumptionHist")  
        connection.close()
        flipxCum, y = self.LoadCurveUtilityMeter(meter, 6, 9, isoHelper, True);
        plt.scatter( flipxCum,y, marker='o',  color='skyblue', linewidth=1)
      
        flipxCum1, y1=self.LoadCurveUtilityMeter(meter, 6, 9, isoHelper, False);
        #plt.scatter( flipxCum1,y1, marker='o',  color='red', linewidth=1)
        flipxCum2, y2=self.LoadCurveUtilityMeter(meter, 1, 12, isoHelper, True);
        #plt.scatter( flipxCum2,y2, marker='o',  color='green', linewidth=1)
        flipxCum3, y3=self.LoadCurveUtilityMeter(meter, 1, 1, isoHelper, True);
        #plt.scatter( flipxCum3,y3, marker='o',  color='yellow', linewidth=1)
        #plt.show()
        self.LoadCurveUtilityMeter(meter, 2, 2, isoHelper, True);
        self.LoadCurveUtilityMeter(meter, 3, 3, isoHelper, True);
        self.LoadCurveUtilityMeter(meter, 4, 4, isoHelper, True);
        self.LoadCurveUtilityMeter(meter, 5, 5, isoHelper, True);
        self.LoadCurveUtilityMeter(meter, 6, 6, isoHelper, True);
        self.LoadCurveUtilityMeter(meter, 7, 7, isoHelper, True);
        self.LoadCurveUtilityMeter(meter, 8, 8, isoHelper, True);
        self.LoadCurveUtilityMeter(meter, 9, 9, isoHelper, True);
        self.LoadCurveUtilityMeter(meter, 10, 10, isoHelper, True);
        self.LoadCurveUtilityMeter(meter, 11, 11, isoHelper, True);
        self.LoadCurveUtilityMeter(meter, 12, 12, isoHelper, True);
        return


    def get_current_hr_consumption(self, meter, meterDf, isoHelper):

        df = None
  
        HrlyTbl = 'HrlyMeterTbl'
                    
        
        try:
            if (len(meterDf.index) == 1):

                timestamp = pd.to_datetime(meterDf.index[0])

                timestamp = timestamp.replace(minute=0, second = 0, microsecond =0)
                
                if (timestamp == pd.to_datetime(meterDf.index[0])):
                    timestamp -= timedelta(hours=1)                
            
                TimeStr = timestamp.strftime("%Y-%m-%dT%H:%M:%S")

                meterTblQuery = "SELECT  [MeterID]\
                                    ,[timestamp]\
                                    ,[RMS_Watts_Net]\
                                    ,[State_Watts_Dir]\
                                    FROM [ISODB].[dbo].[meterTbl] where (timestamp  >  CONVERT(DATETIME,'" + TimeStr + "')) and ( MeterID  = '" + meter + "')"
                 
                meterDf = pd.read_sql(meterTblQuery,isoHelper.engine)
                

                meterDf.reset_index(drop=True,inplace=True)
                meterDf.set_index('timestamp', inplace=True) 
            
            hrlyDataDf = meterDf.resample('H',label='right', closed='right').agg({"State_Watts_Dir":'size',"RMS_Watts_Net":'sum'})

            hrlyDataDf.reset_index(inplace=True)
            #print(hrlyDataDf)
            hrlyDataDf.insert(1, 'MeterID', meter, allow_duplicates = True)
           
            hrlyDataDf.rename(columns={"State_Watts_Dir": "NumReads",  "RMS_Watts_Net":"HrlyWatts"},inplace =True)

            oldestTimestamp =hrlyDataDf['timestamp'].min()
            isoHelper.clearTbl(oldestTimestamp, HrlyTbl)


            ret  = isoHelper.saveDf(DataTbl=HrlyTbl, Data= hrlyDataDf)


            if (ret==False):
                print("could not save MeterhrlyDataDf")

        except BaseException as e:
            print(e)
  
        finally:
            return

