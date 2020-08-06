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
    """description of class"""
    engine = create_engine('mssql+pyodbc://Kapil:Acfjo12#@ISODSN')

    def saveDf(self, DataTbl='lmpTbl', Data='df'):

        try:
            Data.reset_index(drop=True, inplace= True)
            Data.to_sql(DataTbl, self.engine, if_exists = 'append',index=False)

        except sqlalchemy.exc.IntegrityError as e:

          return False

        except BaseException as e:

          print(e)

          print("Save DF Unexpected error:", sys.exc_info()[0])

        finally:
            
            return True

    def emptyTbl(self,tblName):
        connection = self.engine.connect()
        result = connection.execute("delete from " + tblName)

        connection.close()
        return

    def emptyAllTbls(self):

        connection = self.engine.connect()

        result = connection.execute("delete from lmpTbl")  
        result = connection.execute("delete from loadTbl") 
        result = connection.execute("delete from genFuelTbl") 
        result = connection.execute("delete from psMeteredLoad") 
        #result = connection.execute("delete from meterTbl")
        result = connection.execute("delete from pjmMeteredLoad")
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

        return df

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


    def saveForecastDf(self,oldestTimestamp, DataTbl='forecastTbl', Data= 'forecastDf', isShortTerm=True):
        
        try:
            connection = self.engine.connect()
            TimeStr = oldestTimestamp.strftime("%Y-%m-%dT%H:%M:%S")
            sql_query = "delete from " + DataTbl + " where timestamp >= CONVERT(DATETIME,'" + TimeStr +"')"
            result = connection.execute(sql_query)
            Data = Data.sort_values('timestamp').drop_duplicates(['timestamp'],keep='last')
            ret = self.saveDf(DataTbl, Data)

            if ret==True and DataTbl == 'forecastTbl':
                self.mergePSEGTimeSeries(oldestTimestamp)

            if ret==True and DataTbl == 'rtoForecastTbl':
                self.mergeRTOTimeSeries(oldestTimestamp)


        except BaseException as e:
            print(e)
  
        finally:
            self.engine.connect().close()

        return df


    def reconcileForrecastdata(self,  MeterId='9214411', startMonth=1, endMonth=12, include=True):

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

    def getPSEGLoad(self, startMonth=1, endMonth=12, include=True):

        df = None
 
        condition = "  (month(timestamp) >='" + str(startMonth) + "' and month(timestamp) <='" + str(endMonth) + "' )"
        if include == True:
           condition
        else:
           condition = "  not " + condition
        
        try:
            sql_query = "select Load from dbo.psMeteredLoad where "+ condition + "    order by timestamp asc"

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
            sql_query = "select [Load] from dbo.[pjmMeteredLoad] where "+ condition + "    order by timestamp asc"

            df = pd.read_sql_query(sql_query, self.engine) 
 
        except BaseException as e:
          print(e)
  
        finally:
            self.engine.connect().close()

        return df


    def mergePSEGTimeSeries(self, startTimeStamp):

        try:
   
            TimeStr = startTimeStamp.strftime("%Y-%m-%dT%H:%M:%S")

            psInstLoadTblQuery = "SELECT timestamp, Load as [PSEG Load] FROM psInstLoadTbl where timestamp  >= CONVERT(DATETIME,'" + TimeStr +"')"
            dfPsInstLoad = pd.read_sql(psInstLoadTblQuery,self.engine)
            dfPsInstLoad.reset_index(drop=True,inplace=True)
            dfPsInstLoad.set_index('timestamp', inplace=True) 

            meterTblQuery = "SELECT timestamp, [RMS_Watts_Tot] as ConsumptionLoad FROM meterTbl   where timestamp  >= CONVERT(DATETIME,'" + TimeStr +"')"
            dfConsumptionLoad = pd.read_sql(meterTblQuery,self.engine)
            dfConsumptionLoad.reset_index(drop=True,inplace=True)
            dfConsumptionLoad.set_index('timestamp', inplace=True) 

            psVeryShortForecastQuery = "SELECT timestamp, LoadForecast as [psVeryShortForecast], EvaluatedAt FROM  forecastTbl where timestamp  >= CONVERT(DATETIME,'" + TimeStr +"')"
            dfPsVeryShortForecast = pd.read_sql(psVeryShortForecastQuery,self.engine)
            dfPsVeryShortForecast = dfPsVeryShortForecast.sort_values('EvaluatedAt').drop_duplicates('timestamp',keep='last')
            #del dfPsVeryShortForecast['EvaluatedAt']
            dfPsVeryShortForecast.reset_index(drop=True,inplace=True)
            dfPsVeryShortForecast.set_index('timestamp', inplace=True) 


            #psForecast7dayTblQuery = 'SELECT timestamp, [Weekly Load Forecast] as ps7DayForecast, EvaluatedAt FROM forecast7dayTbl   where timestamp > ' + startTimeStamp.strftime("%Y-%m-%d") 
            #dfPs7DayForecast= pd.read_sql(psForecast7dayTblQuery,self.engine)
            #dfPs7DayForecast = dfPs7DayForecast.sort_values('EvaluatedAt').drop_duplicates('timestamp',keep='last')
            #del dfPs7DayForecast['EvaluatedAt']
            #dfPs7DayForecast.reset_index(drop=True,inplace=True)
            #dfPs7DayForecast.set_index('timestamp', inplace=True) 

            #rtoForecast7dayTblQuery = 'SELECT timestamp, [Weekly Load Forecast] as rto7DayForecast, EvaluatedAt FROM rtoForecast7dayTbl   where timestamp > ' + startTimeStamp.strftime("%Y-%m-%d") 
            #dfRto7DayForecast= pd.read_sql(rtoForecast7dayTblQuery,self.engine)
            #dfRto7DayForecast = dfRto7DayForecast.sort_values('EvaluatedAt').drop_duplicates('timestamp',keep='last')
            #del dfRto7DayForecast['EvaluatedAt']
            #dfRto7DayForecast.reset_index(drop=True,inplace=True)
            #dfRto7DayForecast.set_index('timestamp', inplace=True) 


            mergedDf = dfConsumptionLoad.join(dfPsInstLoad, how='outer')\
                      .join(dfPsVeryShortForecast, how='outer')
                      #.join(dfPs7DayForecast, how='outer')
            mergedDf.reset_index(inplace=True)

            connection = self.engine.connect()

            result = connection.execute("delete from PSEGLoadsTbl  where timestamp  >= CONVERT(DATETIME,'" + TimeStr +"')")
            mergedDf = mergedDf.sort_values('timestamp').drop_duplicates('timestamp',keep='last')
            self.saveDf('PSEGLoadsTbl', mergedDf)

        except BaseException as e:
            print (e)
            return None
  
        finally:
            self.engine.connect().close()
            print(mergedDf)
            print(dfPsInstLoad)
            print(dfConsumptionLoad)
            print(dfPsVeryShortForecast)

            #print(dfPs7DayForecast)

        return None


    def mergeRTOTimeSeries(self, startTimeStamp):

        try:
            TimeStr = startTimeStamp.strftime("%Y-%m-%dT%H:%M:%S")

            rtoInstLoadTblQuery ="SELECT timestamp, Load as [RTO Load] FROM loadTbl where timestamp >= CONVERT(DATETIME,'" + TimeStr +"')"
            dfRtoInstLoad = pd.read_sql(rtoInstLoadTblQuery,self.engine)
            dfRtoInstLoad.reset_index(drop=True,inplace=True)
            dfRtoInstLoad.set_index('timestamp', inplace=True) 

            meterTblQuery = "SELECT timestamp, [RMS_Watts_Tot] as ConsumptionLoad FROM meterTbl   where timestamp >= CONVERT(DATETIME,'" + TimeStr +"')"
            dfConsumptionLoad = pd.read_sql(meterTblQuery,self.engine)
            dfConsumptionLoad.reset_index(drop=True,inplace=True)
            dfConsumptionLoad.set_index('timestamp', inplace=True) 


            rtoVeryShortForecastQuery = "SELECT timestamp, LoadForecast as [rtoVeryShortForecast], EvaluatedAt FROM  rtoForecastTbl where timestamp >= CONVERT(DATETIME,'" + TimeStr +"')"
            dfRtoVeryShortForecast = pd.read_sql(rtoVeryShortForecastQuery,self.engine)
            dfRtoVeryShortForecast = dfRtoVeryShortForecast.sort_values('EvaluatedAt').drop_duplicates('timestamp',keep='last')
            dfRtoVeryShortForecast['EvaluatedAt']
            dfRtoVeryShortForecast.reset_index(drop=True,inplace=True)
            dfRtoVeryShortForecast.set_index('timestamp', inplace=True) 


            #rtoForecast7dayTblQuery = 'SELECT timestamp, [Weekly Load Forecast] as rto7DayForecast, EvaluatedAt FROM rtoForecast7dayTbl   where timestamp > ' + startTimeStamp.strftime("%Y-%m-%d") 
            #dfRto7DayForecast= pd.read_sql(rtoForecast7dayTblQuery,self.engine)
            #dfRto7DayForecast = dfRto7DayForecast.sort_values('EvaluatedAt').drop_duplicates('timestamp',keep='last')
            #del dfRto7DayForecast['EvaluatedAt']
            #dfRto7DayForecast.reset_index(drop=True,inplace=True)
            #dfRto7DayForecast.set_index('timestamp', inplace=True) 


            mergedDf = dfConsumptionLoad.join(dfRtoInstLoad, how='outer')\
                      .join(dfRtoVeryShortForecast, how='outer')
                      #.join(dfRto7DayForecast, how='outer')
            mergedDf.reset_index(inplace=True)

            connection = self.engine.connect()

            result = connection.execute("delete from RtoLoadsTbl  where timestamp >= CONVERT(DATETIME,'" + TimeStr +"')")
            mergedDf = mergedDf.sort_values('timestamp').drop_duplicates('timestamp',keep='last')
            self.saveDf('RtoLoadsTbl', mergedDf)

        except BaseException as e:
            print (e)
            return None
  
        finally:
            self.engine.connect().close()
            print(mergedDf)
            print(dfRtoInstLoad)
            print(dfConsumptionLoad)
            print(dfRtoVeryShortForecast)
            #print(dfRto7DayForecast)

        return None


