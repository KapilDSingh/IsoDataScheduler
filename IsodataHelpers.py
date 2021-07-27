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
            connection.close()
            return ret

    def emptyAllTbls(self):

        connection = self.engine.connect()
        result = connection.execute("delete from peakTable")
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


 
    def save_hrly_load(self, isForecast, loadDf,  instTbl, HrlyTbl):     
    
        try:
            ret = True

            oldestTimestamp =loadDf['timestamp'].min()
                
            loadDf = self.getHrlyLoad(isForecast, oldestTimestamp, instTbl)

            loadDf.reset_index(drop=True,inplace=True)
            loadDf.set_index('timestamp', inplace=True) 
           
            if (isForecast == True):
                hrlyDataDf = loadDf.resample('H',label='right', closed='right').agg({"Area":'size',"LoadForecast":'sum'})
                hrlyDataDf.rename(columns={"Area": "ForecstNumReads",  "LoadForecast":"HrlyForecstLoad"},inplace =True)
            else:
                hrlyDataDf = loadDf.resample('H',label='right', closed='right').agg({"Area":'size',"Load":'sum'})
                hrlyDataDf.rename(columns={"Area": "NumReads",  "Load":"HrlyInstLoad"},inplace =True)

            hrlyDataDf.reset_index(inplace=True)
            minTimeStamp = hrlyDataDf["timestamp"].min()
            self.clearTbl(minTimeStamp,  HrlyTbl, True)

            if (isForecast):

                hrlyDataDf["EvaluatedAt"]=oldestTimestamp
                hrlyDataDf["Peak"]=0

            ret = self.saveDf(DataTbl=HrlyTbl, Data= hrlyDataDf)

            

            if (ret == False):
                print("save_hrly_load could not save hrlyDataDf")

        except BaseException as e:
            print("save_hrly_load",e)
  
        finally:
            return ret

    def getHrlyLoad(self, isForecast, timestamp, DataTbl):

        try:
            startTimestamp = timestamp.replace(minute=0, second = 0, microsecond =0)
                
            if (startTimestamp == timestamp):
                startTimestamp -= timedelta(hours=1)                

            HrStartTimeStr = startTimestamp.strftime("%Y-%m-%dT%H:%M:%S")
            EndTimeStr = timestamp.strftime("%Y-%m-%dT%H:%M:%S")
            
            instLoadTblQuery = "SELECT * FROM " + DataTbl + " where timestamp  > CONVERT(DATETIME,'" + HrStartTimeStr + "')"
            loadDf = pd.read_sql(instLoadTblQuery,self.engine)
              

        except BaseException as e:
            print("getHrlyLoad ",e)
  
        finally:
            return loadDf

 
    def saveLoadDf(self, Area, isForecast, loadDf):
        ret = True

        try:
            if (isForecast == True):
                if (Area == 'ps'):
                    DataTbl = 'forecastTbl'
                    HrlyTbl = 'psHrlyForecstTbl'
                else:
                    DataTbl = 'rtoForecastTbl'
                    HrlyTbl = 'rtoHrlyForecstTbl'
            else:
                 if (Area == 'ps'):
                    DataTbl = 'psInstLoadTbl'
                    HrlyTbl = 'psHrlyLoadTbl'
                 else:
                    DataTbl = 'loadTbl'
                    HrlyTbl = 'rtoHrlyLoadTbl'
        
            if (len(loadDf.index) > 1):
                oldestTimestamp =loadDf['timestamp'].min()
                self.clearTbl(oldestTimestamp, DataTbl)

            ret = self.saveDf(DataTbl, loadDf)

            if (ret == True):
                self.save_hrly_load(isForecast, loadDf,  DataTbl, HrlyTbl)

        except BaseException as e:
            print("saveLoadDf",e)
  
        finally:
            return ret
    
    def CheckPeakEnd(self, timestamp, Area):

        try:
            if (Area == 'ps'):

                forecastTbl = 'psHrlyForecstTbl'
                DataTbl = 'psHrlyLoadTbl'
                instDataTbl = 'psInstLoadTbl'

            if (Area == 'PJM RTO'):

                forecastTbl = 'rtoHrlyForecstTbl'
                DataTbl = 'rtoHrlyLoadTbl'
                instDataTbl = 'loadTbl'


            PrevPkTimestamp = timestamp.replace(minute=0, second = 0, microsecond =0)

            peakQuery = "select Peak from " + forecastTbl +  "  where timestamp = '" + PrevPkTimestamp.strftime("%Y-%m-%dT%H:%M:%S")  +"'"

            connection = self.engine.connect()

            resPeak = connection.execute(peakQuery).scalar()

            if ((resPeak == None) or (resPeak ==0)):

                peakLoadQuery = "select [HrlyInstLoad]/ NumReads as Load from " + DataTbl +  "  where timestamp = '" + PrevPkTimestamp.strftime("%Y-%m-%dT%H:%M:%S")  +"'"
                pkLoad = connection.execute(peakLoadQuery).scalar()
               
                loadDf, load, current_Minutes = self.getHrlyLoad(False, timestamp, instDataTbl)

                if (load ==load):
                     currentTimestamp = PrevPkTimestamp + timedelta(hours=1)        
                     sql_query = "Update [ISODB].[dbo]." + forecastTbl + " set Peak= " + str(current_Minutes) + \
                         " where timestamp = '" + currentTimestamp.strftime("%Y-%m-%dT%H:%M:%S") +"'"

                     result = connection.execute(sql_query)

                     pkTblQuery =  "Update [ISODB].[dbo].[peakTable] set EndTime= " + str(current_Minutes) + \
                         " where Area = '" + Area + "' and  timestamp = '" + PrevPkTimestamp.strftime("%Y-%m-%dT%H:%M:%S")  +"'"

                     result = connection.execute(pkTblQuery)
        except BaseException as e:
            print("CheckPeakEnd",e)
  
        finally:
            return result




    def getMaxLoadforTimePeriod(self, endTimeStamp, Area):

        df = None

        eastern = timezone('US/Eastern')

        if (Area == 'ps') :
            DataTbl = 'psHrlyLoadTbl'
            startTime = datetime(endTimeStamp.year, endTimeStamp.month, 1)
            numPoints = 1
        elif (Area == 'PJM RTO'):
            DataTbl = 'rtoHrlyLoadTbl'
            startTime =  datetime(endTimeStamp.year, 6, 1)
            numPoints = 5

        try:
            startTimeStr = startTime.strftime("%Y-%m-%dT%H:%M:%S")
            endTimeStr = endTimeStamp.strftime("%Y-%m-%dT%H:%M:%S")

            connection = self.engine.connect()
            sql_query = "Update rtoHrlyForecstTbl \
            set Peak = 2 where rtoHrlyForecstTbl.timestamp in \
            (SELECT TOP (5) rtoHrlyLoadTbl.timestamp \
                FROM   rtoHrlyLoadTbl INNER JOIN\
                    rtoHrlyForecstTbl ON rtoHrlyLoadTbl.timestamp = rtoHrlyForecstTbl.timestamp\
                WHERE (rtoHrlyForecstTbl.Peak = 1) \
                and rtoHrlyForecstTbl.timestamp >= '2021-6-1'\
                ORDER BY rtoHrlyLoadTbl.HrlyInstLoad / rtoHrlyLoadTbl.NumReads DESC)"

            result = connection.execute(sql_query)

            connection.close()
  
            df = pd.read_sql_query(sql_query, self.engine) 

        except BaseException as e:

            print("getMaxLoadforTimePeriod",e)
  
        finally:

            return df

 
    def fetchLoadThresholds(month):
        try:
            sql_query = "SELECT TOP (1000) [MONTH_Num], [  PJM 2020  ], [  PJM 2021  ], [  PSEG 2020  ],[  PSEG 2021  ],[  Billing Day  ],[  ThresholdPJM  ],[  ThresholdPSEG  ] \
                        FROM [ISODB].[dbo].[PJMPSEGForecast2020_2021] where [MONTH_Num] = " + month

            df = pd.read_sql_query(sql_query, self.engine) 
 
        except BaseException as e:
          print(e)
  
        finally:

            return df



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
            return df


    def getLmp_latest(self,  nodeId='PSEG', numIntervals=12):

        df = None
 

        
        try:
            sql_query = "select top 5 timestamp, node_id, [5 Minute Weighted Avg. LMP] from dbo.lmpTbl where node_id ='PSEG'   order by timestamp desc"

            df = pd.read_sql_query(sql_query, self.engine) 
 
        except BaseException as e:
          print(e)
  
        finally:
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
            return df

    def get_latest_Forecast(self, Area, isShortTerm=True):

        df = None
         
        try:

            if (Area == 'ps'):
                sql_query = "select top 1 timestamp, EvaluatedAt from forecastTbl order by timestamp desc,EvaluatedAt  desc"
            else:
                sql_query = "select top 1 timestamp, EvaluatedAt from rtoForecastTbl order by timestamp desc,EvaluatedAt  desc"

            df = pd.read_sql_query(sql_query, self.engine) 
 
        except BaseException as e:
            print("get_latest_Forecast",e)
  
        finally:

            return df

    def getStartEndForecastTimestamp(self, Area, isHrly):

        if (Area == 'ps') and isHrly == False:
            DataTbl = 'forecastTbl'
        elif (Area == 'ps') and isHrly == True:
            DataTbl = 'psHrlyForecstTbl'
        elif (Area == 'PJM RTO') and isHrly == False:
            DataTbl = 'rtoForecastTbl'
        elif (Area == 'PJM RTO') and isHrly == True:
            DataTbl = 'rtoHrlyForecstTbl'

        df = None
        eastern = timezone('US/Eastern')


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
            return startTimeStamp, endTimeStamp

    def getLmp_period(self, start="2018-07-26 13:10:00.000000", end="2018-07-26 13:20:00.000000", nodeId='PSEG'):
        
        df = None
                
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

            return df

    def call_procedure(self,function_name, params):
    
        connection = self.engine.raw_connection()
        try:
            cursor = connection.cursor()
            cursor.execute(function_name,params)
            
            results = list(cursor.fetchall())
            cursor.close()
            connection.commit()
        finally:
            connection.close()
            return results