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



class IsodataHelpers(object):

   

    engine = create_engine('mssql+pyodbc://Kapil:Acfjo12#@ISODSN')

    def saveDf(self, DataTbl='lmpTbl', Data='df'):

        ret = True

        try:
            #Data.reset_index( inplace= True)
            Data.to_sql(DataTbl, self.engine, if_exists = 'append',index=False)

        except sqlalchemy.exc.IntegrityError as e:

          ret = False

        except BaseException as e:

          print(e)

          print("Save DF Unexpected error:", e)

        finally:
            
            return ret

    def clearTbl(self,timestamp, tblName, include = True):

        ret = True

        try:
            TimeStr = timestamp.strftime("%Y-%m-%dT%H:%M:%S")

            connection = self.engine.connect()
            if (include == True):
                result = connection.execute("delete from " + tblName + "  where timestamp  >= CONVERT(DATETIME,'" + TimeStr + "')")
            else:
                result = connection.execute("delete from " + tblName + "  where timestamp  > CONVERT(DATETIME,'" + TimeStr + "')")
        except BaseException as e:

            print(e)

            ret = False
            print("ClearTbl Unexpected error:", e)

        finally:
            self.engine.connect().close()
            return ret

    def emptyAllTbls(self):

        connection = self.engine.connect()

        #result = connection.execute("delete from lmpTbl")  
        #result = connection.execute("delete from loadTbl") 
        #result = connection.execute("delete from psInstLoadTbl") 
        #result = connection.execute("delete from psHrlyLoadTbl") 
        #result = connection.execute("delete from rtoHrlyLoadTbl") 
        #result = connection.execute("delete from genFuelTbl") 
        #result = connection.execute("delete from psMeteredLoad") 
        ##result = connection.execute("delete from meterTbl")
        #result = connection.execute("delete from pjmMeteredLoad")
        connection.close()
        return

    def getLmp_period(self, start="2018-07-26 13:10:00.000000", end="2018-07-26 13:20:00.000000", nodeId='PSEG'):
        
        df = None
        
        #self.engine
        #=create_self.engine('mssql+pymssql://KapilSingh:Acfjo12#@100.25.120.167\EC2AMAZ-I2S81GT:1433/ISODB')
        
        try:
            sql_query = 'select timestamp, node_id, [5 Minute Real Time LMP] from dbo.lmpTbl where node_id = %(nodeId)s and ( timestamp between %(start)s  and %(end)s )  order by timestamp asc'
            df = pd.read_sql_query(sql_query, self.engine, params ={
                                                           'node_id': nodeId,
                                                           'start' : start,
                                                           'end' : end
                                                         })


        except BaseException as e:
          print(e)
  
        finally:
            self.engine.connect().close()

        return

    def getLmp_latest(self,  nodeId='PSEG', numIntervals=12):

        df = None
 

        
        try:
            sql_query = "select top 5 timestamp, node_id, [5 Minute Weighted Avg. LMP] from dbo.lmpTbl where node_id ='PSEG'   order by timestamp desc"

            df = pd.read_sql_query(sql_query, self.engine) 
 
        except BaseException as e:
          print(e)
  
        finally:
            self.engine.connect().close()

        return df

    def getPSEGMeterData(self,  MeterId='9214411', startMonth=1, endMonth=12, include=True):

        df = None
 
        condition = "  (month(timestamp) >='" + str(startMonth) + "' and month(timestamp) <='" + str(endMonth) + "' )"
        if include == True:
           condition
        else:
           condition = "  not " + condition
        
        try:
            sql_query = "select KW from dbo.PSEGMeter where MeterId = '" + MeterId + "' and " + condition + "    order by timestamp asc"

            df = pd.read_sql_query(sql_query, self.engine) 
 
        except BaseException as e:
          print(e)
  
        finally:
            self.engine.connect().close()

        return df

    def get_latest_Forecast(self, isPSEG, isShortTerm=True):

        df = None
         
        try:
            if (isShortTerm == True):
                if (isPSEG):
                    sql_query = "select top 1 timestamp, EvaluatedAt from forecastTbl order by timestamp desc,EvaluatedAt  desc"
                else:
                    sql_query = "select top 1 timestamp, EvaluatedAt from rtoForecastTbl order by timestamp desc,EvaluatedAt  desc"

            else:
                if (isPSEG):
                    sql_query = "select top 1 timestamp, EvaluatedAt from forecast7dayTbl order by timestamp desc,EvaluatedAt desc"
                else:
                    sql_query = "select top 1 timestamp, EvaluatedAt from rtoForecast7dayTbl order by timestamp desc,EvaluatedAt desc"

            df = pd.read_sql_query(sql_query, self.engine) 
 
        except BaseException as e:
            print(e)
  
        finally:
            self.engine.connect().close()

        return df



    def get_current_hr_load(self, loadDf, Area):

        df = None
        
        if (Area == 'ps'):
            HrlyTbl = 'psHrlyLoadTbl'
            InstLoadTbl = 'psInstLoadTbl'
        else:
             HrlyTbl = 'rtoHrlyLoadTbl'
             InstLoadTbl = 'loadTbl'
        
        try:
            if (len(loadDf.index) == 1):

                timestamp = pd.to_datetime(loadDf.index[0])

                timestamp = timestamp.replace(minute=0, second = 0, microsecond =0)
                
                if (timestamp == pd.to_datetime(loadDf.index[0])):
                    timestamp -= timedelta(hours=1)                

                TimeStr = timestamp.strftime("%Y-%m-%dT%H:%M:%S")

                instLoadTblQuery = "SELECT timestamp, Area, Load FROM " + InstLoadTbl + " where timestamp  > CONVERT(DATETIME,'" + TimeStr + "')"
                
                loadDf = pd.read_sql(instLoadTblQuery,self.engine)
                loadDf.reset_index(drop=True,inplace=True)
                loadDf.set_index('timestamp', inplace=True) 
            
            hrlyDataDf = loadDf.resample('H',label='right', closed='right').agg({"Area":'size',"Load":'sum'})
           
            hrlyDataDf.rename(columns={"Area": "NumReads",  "Load":"HrlyInstLoad"},inplace =True)
            hrlyDataDf.reset_index(inplace=True)
            minTimeStamp = hrlyDataDf["timestamp"].min()
            self.clearTbl(minTimeStamp,  HrlyTbl, True)

            ret=self.saveDf(DataTbl=HrlyTbl, Data= hrlyDataDf)


            if (ret==False):
                print("get_current_hr_load could not save hrlyDataDf")

        except BaseException as e:
            print(e)
  
        finally:
            self.engine.connect().close()

            print(hrlyDataDf)
            return hrlyDataDf



    def fetchLoadThresholds (month):
        try:
            sql_query = "SELECT TOP (1000) [MONTH_Num], [  PJM 2020  ], [  PJM 2021  ], [  PSEG 2020  ],[  PSEG 2021  ],[  Billing Day  ],[  ThresholdPJM  ],[  ThresholdPSEG  ] \
                        FROM [ISODB].[dbo].[PJMPSEGForecast2020_2021] where [MONTH_Num] = " +  month

            df = pd.read_sql_query(sql_query, self.engine) 
 
        except BaseException as e:
          print(e)
  
        finally:
            self.engine.connect().close()

        return df

    def saveForecastDf(self,oldestTimestamp, isPSEG, forecastDf, isShortTerm=True):
        
        try:
            if (isPSEG ==True):
                DataTbl = 'forecastTbl'
                HrlyTbl = 'psHrlyForecstTbl'
            else:
                DataTbl = 'rtoForecastTbl'
                HrlyTbl = 'rtoHrlyForecstTbl'
            
            
            self.clearTbl(oldestTimestamp, DataTbl)


            ret = self.saveDf(DataTbl, forecastDf)
            if (ret == False):
                print ("save instant ForecastDf failed")

            
            timestamp = pd.to_datetime(oldestTimestamp)

            #timestamp = timestamp.replace(minute=0, second = 0, microsecond =0)
                
            #if (timestamp == pd.to_datetime(oldestTimestamp)):
            #    timestamp -= timedelta(hours=1)                

            TimeStr = timestamp.strftime("%Y-%m-%dT%H:%M:%S")

            connection = self.engine.connect()

            forecstLoadTblQuery = "SELECT timestamp, Area, LoadForecast FROM " + DataTbl + " where timestamp  > CONVERT(DATETIME,'" + TimeStr + "')"

            forecstLoadDf = pd.read_sql(forecstLoadTblQuery,self.engine)
            result = connection.execute("delete from " + HrlyTbl ) #+ "  where timestamp  > CONVERT(DATETIME,'" + TimeStr + "')")

            self.engine.connect().close()

            forecstLoadDf.reset_index(drop=True,inplace=True)
            forecstLoadDf.set_index('timestamp', inplace=True) 
            
            hrlyDataDf = forecstLoadDf.resample('H',label='right', closed='right').agg({"Area":'size',"LoadForecast":'sum'})
           
            hrlyDataDf.rename(columns={"Area": "ForecstNumReads",  "LoadForecast":"HrlyForecstLoad"},inplace =True)
            hrlyDataDf.reset_index(inplace=True)

            ret=self.saveDf(DataTbl=HrlyTbl, Data= hrlyDataDf)


            if (ret==False):
                print("saveForecastDf could not save hrlyDataDf")

        except BaseException as e:
            print(e)
  
        finally:
            self.engine.connect().close()

            print(hrlyDataDf)
            return hrlyDataDf


    def getPSEGLoad(self, startMonth=1, endMonth=12, include=True):

        df = None
 
        condition = "  (month(timestamp) >='" + str(startMonth) + "' and month(timestamp) <='" + str(endMonth) + "' )"
        if include == True:
           condition
        else:
           condition = "  not " + condition
        
        try:
            sql_query = "select Load from dbo.psMeteredLoad where " + condition + "    order by timestamp asc"

            df = pd.read_sql_query(sql_query, self.engine) 
 
        except BaseException as e:
          print(e)
  
        finally:
            self.engine.connect().close()

        return df

    def getRTOLoad(self, startMonth=1, endMonth=12, include=True):

        df = None
 
        condition = "  (month(timestamp) >='" + str(startMonth) + "' and month(timestamp) <='" + str(endMonth) + "' )"
        if include == True:
           condition
        else:
           condition = "  not " + condition
        
        try:
            sql_query = "select [Load] from dbo.[pjmMeteredLoad] where " + condition + "    order by timestamp asc"

            df = pd.read_sql_query(sql_query, self.engine) 
 
        except BaseException as e:
          print(e)
  
        finally:
            self.engine.connect().close()

        return df


    def mergePSEGTimeSeries(self, startTimeStamp):

        try:
   
            TimeStr = startTimeStamp.strftime("%Y-%m-%dT%H:%M:%S")

            psInstLoadTblQuery = "SELECT timestamp, Load as [PSEG Load] FROM psInstLoadTbl where timestamp  >= CONVERT(DATETIME,'" + TimeStr + "')"
            dfPsInstLoad = pd.read_sql(psInstLoadTblQuery,self.engine)
            dfPsInstLoad.reset_index(drop=True,inplace=True)
            dfPsInstLoad.set_index('timestamp', inplace=True) 

            meterTblQuery = "SELECT timestamp, [RMS_Watts_Tot] as ConsumptionLoad FROM meterTbl   where timestamp  >= CONVERT(DATETIME,'" + TimeStr + "')"
            dfConsumptionLoad = pd.read_sql(meterTblQuery,self.engine)
            dfConsumptionLoad.reset_index(drop=True,inplace=True)
            dfConsumptionLoad.set_index('timestamp', inplace=True) 

            psVeryShortForecastQuery = "SELECT timestamp, LoadForecast as [psVeryShortForecast], EvaluatedAt FROM  forecastTbl where timestamp  >= CONVERT(DATETIME,'" + TimeStr + "')"
            dfPsVeryShortForecast = pd.read_sql(psVeryShortForecastQuery,self.engine)
            dfPsVeryShortForecast = dfPsVeryShortForecast.sort_values('EvaluatedAt').drop_duplicates('timestamp',keep='last')
            #del dfPsVeryShortForecast['EvaluatedAt']
            dfPsVeryShortForecast.reset_index(drop=True,inplace=True)
            dfPsVeryShortForecast.set_index('timestamp', inplace=True) 


            #psForecast7dayTblQuery = 'SELECT timestamp, [Weekly Load Forecast]
            #as ps7DayForecast, EvaluatedAt FROM forecast7dayTbl where
            #timestamp > ' + startTimeStamp.strftime("%Y-%m-%d")
            #dfPs7DayForecast= pd.read_sql(psForecast7dayTblQuery,self.engine)
            #dfPs7DayForecast =
            #dfPs7DayForecast.sort_values('EvaluatedAt').drop_duplicates('timestamp',keep='last')
            #del dfPs7DayForecast['EvaluatedAt']
            #dfPs7DayForecast.reset_index(drop=True,inplace=True)
            #dfPs7DayForecast.set_index('timestamp', inplace=True)

            #rtoForecast7dayTblQuery = 'SELECT timestamp, [Weekly Load
            #Forecast] as rto7DayForecast, EvaluatedAt FROM rtoForecast7dayTbl
            #where timestamp > ' + startTimeStamp.strftime("%Y-%m-%d")
            #dfRto7DayForecast=
            #pd.read_sql(rtoForecast7dayTblQuery,self.engine)
            #dfRto7DayForecast =
            #dfRto7DayForecast.sort_values('EvaluatedAt').drop_duplicates('timestamp',keep='last')
            #del dfRto7DayForecast['EvaluatedAt']
            #dfRto7DayForecast.reset_index(drop=True,inplace=True)
            #dfRto7DayForecast.set_index('timestamp', inplace=True)


            mergedDf = dfConsumptionLoad.join(dfPsInstLoad, how='outer')\
                      .join(dfPsVeryShortForecast, how='outer')
                      
            mergedDf.reset_index(inplace=True)

            self.clearTbl(startTimeStamp, 'PSEGLoadsTbl')

            mergedDf = mergedDf.sort_values('timestamp').drop_duplicates('timestamp',keep='last')
            self.saveDf('PSEGLoadsTbl', mergedDf)

        except BaseException as e:
            print(e)
            return None
  
        finally:

            #print(mergedDf)
            #print(dfPsInstLoad)
            #print(dfConsumptionLoad)
            #print(dfPsVeryShortForecast)

            #print(dfPs7DayForecast)

            return mergedDf


    def mergeRTOTimeSeries(self, startTimeStamp):

        try:
            TimeStr = startTimeStamp.strftime("%Y-%m-%dT%H:%M:%S")

            rtoInstLoadTblQuery = "SELECT timestamp, Load as [RTO Load] FROM loadTbl where timestamp >= CONVERT(DATETIME,'" + TimeStr + "')"
            dfRtoInstLoad = pd.read_sql(rtoInstLoadTblQuery,self.engine)
            dfRtoInstLoad.reset_index(drop=True,inplace=True)
            dfRtoInstLoad.set_index('timestamp', inplace=True) 

            meterTblQuery = "SELECT timestamp, [RMS_Watts_Tot] as ConsumptionLoad FROM meterTbl   where timestamp >= CONVERT(DATETIME,'" + TimeStr + "')"
            dfConsumptionLoad = pd.read_sql(meterTblQuery,self.engine)
            dfConsumptionLoad.reset_index(drop=True,inplace=True)
            dfConsumptionLoad.set_index('timestamp', inplace=True) 


            rtoVeryShortForecastQuery = "SELECT timestamp, LoadForecast as [rtoVeryShortForecast], EvaluatedAt FROM  rtoForecastTbl where timestamp >= CONVERT(DATETIME,'" + TimeStr + "')"
            dfRtoVeryShortForecast = pd.read_sql(rtoVeryShortForecastQuery,self.engine)
            dfRtoVeryShortForecast = dfRtoVeryShortForecast.sort_values('EvaluatedAt').drop_duplicates('timestamp',keep='last')
            dfRtoVeryShortForecast['EvaluatedAt']
            dfRtoVeryShortForecast.reset_index(drop=True,inplace=True)
            dfRtoVeryShortForecast.set_index('timestamp', inplace=True) 


            #rtoForecast7dayTblQuery = 'SELECT timestamp, [Weekly Load
            #Forecast] as rto7DayForecast, EvaluatedAt FROM rtoForecast7dayTbl
            #where timestamp > ' + startTimeStamp.strftime("%Y-%m-%d")
            #dfRto7DayForecast=
            #pd.read_sql(rtoForecast7dayTblQuery,self.engine)
            #dfRto7DayForecast =
            #dfRto7DayForecast.sort_values('EvaluatedAt').drop_duplicates('timestamp',keep='last')
            #del dfRto7DayForecast['EvaluatedAt']
            #dfRto7DayForecast.reset_index(drop=True,inplace=True)
            #dfRto7DayForecast.set_index('timestamp', inplace=True)


            mergedDf = dfConsumptionLoad.join(dfRtoInstLoad, how='outer')\
                      .join(dfRtoVeryShortForecast, how='outer')
                      #.join(dfRto7DayForecast, how='outer')
            mergedDf.reset_index(inplace=True)

            connection = self.engine.connect()

            self.clearTbl(startTimeStamp, 'RtoLoadsTbl' )

            mergedDf = mergedDf.sort_values('timestamp').drop_duplicates('timestamp',keep='last')
            self.saveDf('RtoLoadsTbl', mergedDf)

        except BaseException as e:
            print(e)
            return None
  
        finally:
            self.engine.connect().close()
            #print(mergedDf)
            #print(dfRtoInstLoad)
            #print(dfConsumptionLoad)
            #print(dfRtoVeryShortForecast)
            #print(dfRto7DayForecast)

        return None


   

