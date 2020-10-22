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
from dateutil.relativedelta import relativedelta



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

          print("Save DF Unexpected error:", e)

        finally:
            return ret

    def replaceDf(self, DataTbl='lmpTbl', Data='df'):

        ret = True

        try:
            minTimeStamp = Data["timestamp"].min()
            maxTimeStamp = Data["timestamp"].max()
            startTimeStr = minTimeStamp.strftime("%Y-%m-%dT%H:%M:%S")
            endTimeStr = maxTimeStamp.strftime("%Y-%m-%dT%H:%M:%S")

            connection = self.engine.connect()

            result = connection.execute("delete from " + DataTbl   + " where timestamp >= CONVERT(DATETIME,'" + startTimeStr + "')\
                    and timestamp <= CONVERT(DATETIME,'" + endTimeStr + "')")

            connection.close()

            ret = self.saveDf(DataTbl, Data)

        except BaseException as e:
          print("Replace DF Unexpected error",e)

        finally:
            
            return ret

    def clearTbl(self,timestamp, tblName, include=True):

        ret = True

        try:
            TimeStr = timestamp.strftime("%Y-%m-%dT%H:%M:%S")

            connection = self.engine.connect()
            if (include == True):
                result = connection.execute("delete from " + tblName + "  where timestamp  >= CONVERT(DATETIME,'" + TimeStr + "')")
            else:
                result = connection.execute("delete from " + tblName + "  where timestamp  > CONVERT(DATETIME,'" + TimeStr + "')")
        except BaseException as e:

            print("ClearTbl Unexpected error:",e)

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
          print("Get LMP", e)
  
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
            print("get_latest_Forecast",e)
  
        finally:
            self.engine.connect().close()

        return df

    def getStartEndForecastTimestamp(self, isPSEG, isHrly):

        if (isPSEG == True) and isHrly == False:
            DataTbl = 'forecastTbl'
        elif (isPSEG == True) and isHrly == True:
            DataTbl = 'psHrlyForecstTbl'
        elif (isPSEG == False) and isHrly == False:
            DataTbl = 'rtoForecastTbl'
        elif (isPSEG == False) and isHrly == True:
            DataTbl = 'rtoHrlyForecstTbl'

        df = None
         
        try:
            sql_query = "select top 1 timestamp from " + DataTbl + " order by timestamp "

            df = pd.read_sql_query(sql_query, self.engine) 

            startTimeStamp = df.iloc[0, 0]

            sql_query = sql_query + " desc"

            df = pd.read_sql_query(sql_query, self.engine) 

            endTimeStamp = df.iloc[0, 0]

 
        except BaseException as e:
            print("get_latest_Forecast",e)
  
        finally:
            self.engine.connect().close()

            return startTimeStamp, endTimeStamp

    def getMaxLoadforTimePeriod(self, oldestTimestamp, isPSEG):

        if (isPSEG == True) :
            DataTbl = 'psHrlyForecstTbl'
        elif (isPSEG == False):
            DataTbl = 'rtoHrlyForecstTbl'

        df = None
         
        eastern = timezone('US/Eastern')
        try:


            if (isPSEG):
                startTime = datetime(oldestTimestamp.year, oldestTimestamp.month, 1, tzinfo=eastern)
                endTime =  oldestTimestamp
                numPoints = 1
            else:
                startTime =  datetime(oldestTimestamp.year, 6, 1, tzinfo=eastern)
                endTime =  oldestTimestamp
                numPoints = 6
          
        
            startTimeStr = startTime.strftime("%Y-%m-%dT%H:%M:%S")
            endTimeStr = endTime.strftime("%Y-%m-%dT%H:%M:%S")

            sql_query = "SELECT  TOP (" + str(numPoints) + ") [timestamp] \
                ,[ForecstNumReads]\
                ,[HrlyForecstLoad]\
                ,[Peak]\
                ,[HrlyForecstLoad]/NULLIF([ForecstNumReads],0) as MaxLoad \
                FROM [ISODB].[dbo]." + DataTbl + " where timestamp >= CONVERT(DATETIME,'" + startTimeStr + "') \
                and timestamp < CONVERT(DATETIME,'" + endTimeStr + "')" + " order by MaxLoad desc"
  


            df = pd.read_sql_query(sql_query, self.engine) 

        except BaseException as e:

            print("getMaxLoadforTimePeriod",e)
  
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

            ret = self.saveDf(DataTbl=HrlyTbl, Data= hrlyDataDf)


            if (ret == False):
                print("get_current_hr_load could not save hrlyDataDf")
            else:
                if (Area == 'ps'):
                    self.mergePSEGHrlySeries(minTimeStamp)
                else:
                    self.mergeRTOHrlySeries(minTimeStamp)



        except BaseException as e:
            print("get_current_hr_load",e)
  
        finally:
            self.engine.connect().close()


            return hrlyDataDf



    def fetchLoadThresholds(month):
        try:
            sql_query = "SELECT TOP (1000) [MONTH_Num], [  PJM 2020  ], [  PJM 2021  ], [  PSEG 2020  ],[  PSEG 2021  ],[  Billing Day  ],[  ThresholdPJM  ],[  ThresholdPSEG  ] \
                        FROM [ISODB].[dbo].[PJMPSEGForecast2020_2021] where [MONTH_Num] = " + month

            df = pd.read_sql_query(sql_query, self.engine) 
 
        except BaseException as e:
          print(e)
  
        finally:
            self.engine.connect().close()

        return df

    def saveForecastDf(self,oldestTimestamp, GCPShave, isPSEG, forecastDf, isShortTerm=True):
        
        try:
            if (isPSEG == True):
                DataTbl = 'forecastTbl'
                HrlyTbl = 'psHrlyForecstTbl'
            else:
                DataTbl = 'rtoForecastTbl'
                HrlyTbl = 'rtoHrlyForecstTbl'
            
            self.clearTbl(oldestTimestamp, DataTbl)

            ret = self.saveDf(DataTbl, forecastDf)
            if (ret == False):
                print("save instant ForecastDf failed")

            prevHrTimestamp = oldestTimestamp.replace(minute=0, second = 0, microsecond =0)
            #prevHrTimestamp -= timedelta(hours=90)
            currentHrTimestamp = prevHrTimestamp + timedelta(hours=1)      

            prevHrTimeStr = prevHrTimestamp.strftime("%Y-%m-%dT%H:%M:%S")

            connection = self.engine.connect()

            forecstLoadTblQuery = "SELECT timestamp, Area, LoadForecast FROM " + DataTbl + " where timestamp  > CONVERT(DATETIME,'" + prevHrTimeStr + "')"

            forecstLoadDf = pd.read_sql(forecstLoadTblQuery,self.engine)

            self.engine.connect().close()

            forecstLoadDf.reset_index(drop=True,inplace=True)
            forecstLoadDf.set_index('timestamp', inplace=True) 
            
            hrlyDataDf = forecstLoadDf.resample('H',label='right', closed='right').agg({"Area":'size',"LoadForecast":'sum'})
           
            hrlyDataDf.rename(columns={"Area": "ForecstNumReads",  "LoadForecast":"HrlyForecstLoad"},inplace =True)

            hrlyDataDf.reset_index(inplace=True)
            
            self.clearTbl(currentHrTimestamp, HrlyTbl)   

            ret = self.saveDf(DataTbl=HrlyTbl, Data= hrlyDataDf)

            if (ret == False):
                print("saveForecastDf could not save hrlyDataDf")
            else:
                GCPShave.findPeaks(oldestTimestamp, isPSEG, False, True, self)
                GCPShave.findPeaks(oldestTimestamp,isPSEG, True, True, self)

                if (isPSEG == True):
                    self.mergePSEGHrlySeries(oldestTimestamp)
                else:
                    self.mergeRTOHrlySeries(oldestTimestamp)

        except BaseException as e:
            print("saveForecastDf",e)
  
        finally:
            self.engine.connect().close()

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
          print("getPSEGLoad",e)
  
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
          print("getRTOLoad",e)
  
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

            psVeryShortForecastQuery = "SELECT timestamp, LoadForecast as [psVeryShortForecast], EvaluatedAt, Peak FROM  forecastTbl where timestamp  >= CONVERT(DATETIME,'" + TimeStr + "')"
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
            print("mergePSEGTimeSeries",e)
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


            rtoVeryShortForecastQuery = "SELECT timestamp, LoadForecast as [rtoVeryShortForecast], EvaluatedAt, Peak  FROM  rtoForecastTbl where timestamp >= CONVERT(DATETIME,'" + TimeStr + "')"
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

            self.clearTbl(startTimeStamp, 'RtoLoadsTbl')

            mergedDf = mergedDf.sort_values('timestamp').drop_duplicates('timestamp',keep='last')
            self.saveDf('RtoLoadsTbl', mergedDf)

        except BaseException as e:
            print("mergeRTOTimeSeries",e)
            return None
  
        finally:
            self.engine.connect().close()
            #print(mergedDf)
            #print(dfRtoInstLoad)
            #print(dfConsumptionLoad)
            #print(dfRtoVeryShortForecast)
            #print(dfRto7DayForecast)

        return None

    def mergePSEGHrlySeries(self, startTimeStamp):

        try:
   
            TimeStr = startTimeStamp.strftime("%Y-%m-%dT%H:%M:%S")

            psHrlyLoadTblQuery = "SELECT  [timestamp], [NumReads] as psNumReads, [HrlyInstLoad] FROM [ISODB].[dbo].[psHrlyLoadTbl]  where timestamp  >= CONVERT(DATETIME,'" + TimeStr + "') order by timestamp "
            dfPsHrlyLoad = pd.read_sql(psHrlyLoadTblQuery,self.engine)
            dfPsHrlyLoad.reset_index(drop=True,inplace=True)
            dfPsHrlyLoad.set_index('timestamp', inplace=True) 

            meterHrlyQuery = "SELECT  [timestamp], [NumReads] as meterNumReads, [HrlyWatts]\
                    FROM [ISODB].[dbo].[HrlyMeterTbl] where timestamp  >= CONVERT(DATETIME,'" + TimeStr + "') order by timestamp "
            dfHrlyConsumptionLoad = pd.read_sql(meterHrlyQuery,self.engine)
            dfHrlyConsumptionLoad.reset_index(drop=True,inplace=True)
            dfHrlyConsumptionLoad.set_index('timestamp', inplace=True) 

            psHrlyVeryShortForecastQuery = "SELECT [timestamp],[ForecstNumReads],[HrlyForecstLoad], Peak\
                                            FROM [ISODB].[dbo].[psHrlyForecstTbl] where timestamp  >= CONVERT(DATETIME,'" + TimeStr + "') order by timestamp "
            dfPsHrlyVeryShortForecast = pd.read_sql(psHrlyVeryShortForecastQuery,self.engine)
            dfPsHrlyVeryShortForecast.reset_index(drop=True,inplace=True)
            dfPsHrlyVeryShortForecast.set_index('timestamp', inplace=True) 


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


            mergedDf = dfHrlyConsumptionLoad.join(dfPsHrlyLoad, how='outer')\
                        .join(dfPsHrlyVeryShortForecast, how='outer')
            
            #x = mergedDf['psNumReads'] + mergedDf['ForecstNumReads']
            #y = x[x == 12].index.tolist()
            
            #if (len(y) > 0 and (len(mergedDf) < 10)) or max(mergedDf['psNumReads']) == 12:
          
            mergedDf.reset_index(inplace=True)

            #self.clearTbl(startTimeStamp, 'PSEGHrlyLoadsTbl')

            mergedDf = mergedDf.sort_values('timestamp').drop_duplicates('timestamp',keep='last')

            #self.saveDf('PSEGHrlyLoadsTbl', mergedDf)
            self.replaceDf('PSEGHrlyLoadsTbl', mergedDf)

        except BaseException as e:
            print(e)
            return None
  
        finally:
            #print("hrly merged df",mergedDf)
            #print(dfPsInstLoad)
            #print(dfConsumptionLoad)
            #print(dfPsVeryShortForecast)

            #print(dfPs7DayForecast)

            return mergedDf



    def mergeRTOHrlySeries(self, startTimeStamp):

        try:
   
            TimeStr = startTimeStamp.strftime("%Y-%m-%dT%H:%M:%S")

            rtoHrlyLoadTblQuery = "SELECT  [timestamp], [NumReads] as rtoNumReads, [HrlyInstLoad] FROM [ISODB].[dbo].[rtoHrlyLoadTbl]  where timestamp  >= CONVERT(DATETIME,'" + TimeStr + "') order by timestamp "
            dfRtoHrlyLoad = pd.read_sql(rtoHrlyLoadTblQuery,self.engine)
            dfRtoHrlyLoad.reset_index(drop=True,inplace=True)
            dfRtoHrlyLoad.set_index('timestamp', inplace=True) 

            meterHrlyQuery = "SELECT [timestamp], [NumReads] as meterNumReads, [HrlyWatts]\
                    FROM [ISODB].[dbo].[HrlyMeterTbl] where timestamp  >= CONVERT(DATETIME,'" + TimeStr + "') order by timestamp "
            dfHrlyConsumptionLoad = pd.read_sql(meterHrlyQuery,self.engine)
            dfHrlyConsumptionLoad.reset_index(drop=True,inplace=True)
            dfHrlyConsumptionLoad.set_index('timestamp', inplace=True) 

            rtoHrlyVeryShortForecastQuery = "SELECT [timestamp],[ForecstNumReads],[HrlyForecstLoad], Peak\
                                            FROM [ISODB].[dbo].[rtoHrlyForecstTbl] where timestamp  >= CONVERT(DATETIME,'" + TimeStr + "') order by timestamp "
            dfRtoHrlyVeryShortForecast = pd.read_sql(rtoHrlyVeryShortForecastQuery,self.engine)
            dfRtoHrlyVeryShortForecast.reset_index(drop=True,inplace=True)
            dfRtoHrlyVeryShortForecast.set_index('timestamp', inplace=True) 


            #RtoForecast7dayTblQuery = 'SELECT timestamp, [Weekly Load
            #Forecast]
            #as Rto7DayForecast, EvaluatedAt FROM forecast7dayTbl where
            #timestamp > ' + startTimeStamp.strftime("%Y-%m-%d")
            #dfRto7DayForecast=
            #pd.read_sql(RtoForecast7dayTblQuery,self.engine)
            #dfRto7DayForecast =
            #dfRto7DayForecast.sort_values('EvaluatedAt').drop_duplicates('timestamp',keep='last')
            #del dfRto7DayForecast['EvaluatedAt']
            #dfRto7DayForecast.reset_index(drop=True,inplace=True)
            #dfRto7DayForecast.set_index('timestamp', inplace=True)

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


            mergedDf = dfHrlyConsumptionLoad.join(dfRtoHrlyLoad, how='outer')\
                        .join(dfRtoHrlyVeryShortForecast, how='outer')

            #x = mergedDf['rtoNumReads'] + mergedDf['ForecstNumReads']
            #y = x[x == 12].index.tolist()
            
            #if (len(y) > 0 and (len(mergedDf) < 10)) or max(mergedDf['rtoNumReads']) == 12:

            mergedDf.reset_index(inplace=True)

            #self.clearTbl(startTimeStamp, 'RTOHrlyLoadsTbl')

            mergedDf = mergedDf.sort_values('timestamp').drop_duplicates('timestamp',keep='last') 
            self.replaceDf('RTOHrlyLoadsTbl', mergedDf)


        except BaseException as e:
            print("mergeRTOHrlySeries",e)
            return None
  
        finally:

            #print(mergedDf)
            #print(dfRtoInstLoad)
            #print(dfConsumptionLoad)
            #print(dfRtoVeryShortForecast)

            #print(dfRto7DayForecast)

            return None


