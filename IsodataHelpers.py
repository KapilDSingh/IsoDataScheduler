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
from base import BaseClient
from pytz import timezone
import base64
import requests
from urllib.parse import urlencode, quote_plus,urlparse, parse_qsl
import pyperclip


#SELECT TOP (1000) [timestamp]
#      ,[EvaluatedAt]
#      ,[Area]
#      ,[LoadForecast]
#  FROM [ISODB].[dbo].[forecastTbl] where [EvaluatedAt] = (select top 1 EvaluatedAt from forecastTbl where timestamp = timestamp  order by evaluatedAT desc) and Area = 'PSE&G/midatl'



class IsodataHelpers(object):
    """description of class"""
    engine = create_engine('mssql+pyodbc://Kapil:Acfjo12#@ISODSN')

    def saveDf(self, DataTbl='lmpTbl', Data='df'):

        try:
            Data.reset_index(drop=True, inplace= True)
            Data.to_sql(DataTbl, self.engine, if_exists = 'append',index=False)

        except sqlalchemy.exc.IntegrityError as e:

          return

        except BaseException as e:

          print(e)

          print("Unexpected error:", sys.exc_info()[0])

        finally:
            return

    def emptyTbl(self,tblName):
        connection = self.engine.connect()
        result = connection.execute("delete from "+tblName)

        connection.close()
        return

    def emptyAllTbls(self):

        connection = self.engine.connect()

        result = connection.execute("delete from lmpTbl")  
        result = connection.execute("delete from loadTbl") 
        result = connection.execute("delete from genFuelTbl") 
        result = connection.execute("delete from psMeteredLoad") 
        result = connection.execute("delete from meterTbl") 
        connection.close()
        return

    def getLmp_period(self, start = "2018-07-26 13:10:00.000000", end =  "2018-07-26 13:20:00.000000", nodeId='PSEG'):
        
        df =None
        
        #self.engine =create_self.engine('mssql+pymssql://KapilSingh:Acfjo12#@100.25.120.167\EC2AMAZ-I2S81GT:1433/ISODB')
        
        try:
            sql_query ='select timestamp, node_id, [5 Minute Real Time LMP] from dbo.lmpTbl where node_id = %(nodeId)s and ( timestamp between %(start)s  and %(end)s )  order by timestamp asc'
            df = pd.read_sql_query(sql_query, self.engine, params ={
                                                           'node_id': nodeId,
                                                           'start' : start,
                                                           'end' : end
                                                         })


        except BaseException as e:
          print (e)
  
        finally:
            self.engine.connect().close()

        return df

    def getLmp_latest(self,  nodeId='PSEG', numIntervals=12):

        df =None
 

        
        try:
            sql_query ="select top 5 timestamp, node_id, [5 Minute Weighted Avg. LMP] from dbo.lmpTbl where node_id ='PSEG'   order by timestamp desc"

            df = pd.read_sql_query(sql_query, self.engine) 
 
        except BaseException as e:
          print (e)
  
        finally:
            self.engine.connect().close()

        return df

    def getPSEGMeterData(self,  MeterId = '9214411', startMonth = 1, endMonth =12, include=True):

        df =None
 
        condition= "  (month(timestamp) >='" + str(startMonth) +"' and month(timestamp) <='" + str(endMonth)+ "' )";
        if include == True:
           condition
        else:
           condition= "  not " + condition;
        
        try:
            sql_query ="select KW from dbo.PSEGMeter where MeterId = '" + MeterId + "' and " + condition + "    order by timestamp asc"

            df = pd.read_sql_query(sql_query, self.engine) 
 
        except BaseException as e:
          print (e)
  
        finally:
            self.engine.connect().close()

        return df
