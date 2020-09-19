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


class MeterData(object):

    http = urllib3.PoolManager()


    
    def fetchMeterData(self, meter,  numReads, isoHelper):

        try:
            numReads = str(numReads)     
            r = self.http.request('GET','https://io.ekmpush.com/readMeter?key=NjUyMzc0MjQ6Z3NaVmhEd20&meters='+ meter + '&ver=v4&fmt=json&cnt=' + numReads + '&tz=America~New_York&fields=RMS_Volts_Ln_1~RMS_Volts_Ln_2~RMS_Volts_Ln_3~RMS_Watts_Tot~Power_Factor_Ln_1~Power_Factor_Ln_2~Power_Factor_Ln_3&status=good ')
            jsonMeter = json.loads(r.data)
            jsonElectricData = jsonMeter['readMeter']['ReadSet'][0]['ReadData']
            meterDf = pd.DataFrame(jsonElectricData)
            meterDf.insert (1, 'timestamp',  pd.to_datetime(meterDf['Date'] + ' '+meterDf['Time']),allow_duplicates = False) 
            meterDf.insert(1, 'MeterID', meter, allow_duplicates = True)
      
            timezone = pytz.timezone("America/New_York")
      
            meterDf['timestamp'] = [pd.Timestamp.round(row, 'min') for row in  meterDf['timestamp']]
            meterDf['timestamp'] = [timezone.localize(row) for row in  meterDf['timestamp']]

            meterDf.drop(columns=['Good','Date', 'Time','Time_Stamp_UTC_ms'], inplace=True)

            meterDf = meterDf.astype({'RMS_Watts_Tot': float,  'RMS_Volts_Ln_1': float,  'RMS_Volts_Ln_2': float,  'RMS_Volts_Ln_3': float,  
           'Power_Factor_Ln_1': float, 'Power_Factor_Ln_2': float, 'Power_Factor_Ln_3': float})

            oldestTimestamp =meterDf['timestamp'].min()
            isoHelper.clearTbl(oldestTimestamp, 'meterTbl')

            isoHelper.saveDf(DataTbl='meterTbl', Data= meterDf);

            meterDf.set_index("timestamp", inplace = True)
            self.get_current_hr_consumption(meter, meterDf, isoHelper)

        except Exception as e:
          print(e)
          print("Fetch Meter Data Unexpected error:",e)
        finally:
            return


    

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
                                    ,[RMS_Watts_Tot]\
                                    ,[RMS_Volts_Ln_1]\
                                    ,[RMS_Volts_Ln_2]\
                                    ,[RMS_Volts_Ln_3]\
                                    ,[Power_Factor_Ln_1]\
                                    ,[Power_Factor_Ln_2]\
                                    ,[Power_Factor_Ln_3]\
                                    FROM [ISODB].[dbo].[meterTbl] where (timestamp  >  CONVERT(DATETIME,'" + TimeStr + "')) and ( MeterID  = '" + meter + "')"
                 
                meterDf = pd.read_sql(meterTblQuery,isoHelper.engine)
                

                isoHelper.engine.connect().close()

                meterDf.reset_index(drop=True,inplace=True)
                meterDf.set_index('timestamp', inplace=True) 
            
            meterDf.drop(columns=['RMS_Volts_Ln_2','RMS_Volts_Ln_3', 'Power_Factor_Ln_1','Power_Factor_Ln_2','Power_Factor_Ln_3'], inplace=True)

            hrlyDataDf = meterDf.resample('H',label='right', closed='right').agg({"RMS_Volts_Ln_1":'size',"RMS_Watts_Tot":'sum'})

            hrlyDataDf.reset_index(inplace=True)
            print(hrlyDataDf)
            hrlyDataDf.insert(1, 'MeterID', meter, allow_duplicates = True)
           
            hrlyDataDf.rename(columns={"RMS_Volts_Ln_1": "NumReads",  "RMS_Watts_Tot":"HrlyWatts"},inplace =True)

            oldestTimestamp =hrlyDataDf['timestamp'].min()
            isoHelper.clearTbl(oldestTimestamp, HrlyTbl)


            ret  = isoHelper.saveDf(DataTbl=HrlyTbl, Data= hrlyDataDf)


            if (ret==False):
                print("could not save MeterhrlyDataDf")

        except BaseException as e:
            print(e)
  
        finally:
            isoHelper.engine.connect().close()

