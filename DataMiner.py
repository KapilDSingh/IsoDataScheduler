
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
from IsodataHelpers import IsodataHelpers

class DataMiner(object):
    """description of class"""


    http = urllib3.PoolManager()

    headers = {'Ocp-Apim-Subscription-Key': 'b4d42032ca23403aaa7bb8fb72025614'}
    
    def fetch_LMP(self, numRows, isoHelper):
        try:
           
            r = self.http.request('GET', 'https://api.pjm.com/api/v1/unverified_five_min_lmps?rowCount=' + str(numRows) + '&sort=datetime_beginning_ept&order=desc&startrow=1&name=pseg&fields=datetime_beginning_ept,name,type,five_min_rtlmp,hourly_lmp&format=JSON', headers=self.headers)

            jsonData = json.loads(r.data)

            jsonExtractData = jsonData['items']
            lmpDf = pd.DataFrame(jsonExtractData)

            lmpDf.reset_index(drop=True, inplace= True)
            
           
            lmpDf.rename(columns={"datetime_beginning_ept": "timestamp", "five_min_rtlmp": "5 Minute Weighted Avg. LMP", "hourly_lmp": "Hourly Integrated LMP", "type": "Type", "name": "node_id"},inplace =True)
            
            lmpDf['timestamp'] = pd.to_datetime(lmpDf['timestamp'].values).strftime('%Y-%m-%d %H:%M:%S')
            lmpDf['timestamp'] = pd.to_datetime(lmpDf['timestamp'])
            isoHelper.saveDf(DataTbl='lmpTbl', Data= lmpDf)
            i = 1
        except Exception as e:
          print(e)
          print("Unexpected error:", sys.exc_info()[0])
        finally:
            return



    def fetch_InstantaneousLoad(self, numRows, isoHelper):

        try:
            r = self.http.request('GET', 'https://api.pjm.com/api/v1/inst_load?rowCount=' + str(numRows) + '&sort=datetime_beginning_ept&order=desc&startrow=1&fields=datetime_beginning_ept,area,instantaneous_load&format=JSON', headers=self.headers)
            
            jsonData = json.loads(r.data)
            # Transform json input to python objects


            # Filter python objects with list comprehensions
            

            # Transform python object back into json
            #output_json = json.dumps(output_dict)

            jsonExtractData = jsonData['items']
            output_dict = [x for x in jsonExtractData if x['area'] == 'PJM RTO']
            loadDf = pd.DataFrame(output_dict)

            loadDf.reset_index(drop=True, inplace= True)
           
            loadDf.rename(columns={"datetime_beginning_ept": "timestamp", "area": "Area", "instantaneous_load": "Instantaneous Load"},inplace =True)
            
            loadDf['timestamp'] = pd.to_datetime(loadDf['timestamp'].values).strftime('%Y-%m-%d %H:%M:%S')
            loadDf['timestamp'] = pd.to_datetime(loadDf['timestamp'])
            isoHelper.saveDf(DataTbl='loadTbl', Data= loadDf)
            i = 1
        except Exception as e:
          print(e)
          print("Unexpected error:", sys.exc_info()[0])
        finally:
            return

        #https://api.pjm.com/api/v1/gen_by_fuel?rowCount=100&sort=datetime_beginning_ept&order=Desc&startRow=1&is_renewable=True

    def fetch_GenFuel(self, numRows, isoHelper):

        try:

   
            r = self.http.request('GET', 'https://api.pjm.com/api/v1/gen_by_fuel?rowCount=' + str(numRows) + '&sort=datetime_beginning_ept&order=desc&startrow=1&fields=datetime_beginning_ept,fuel_type,mw,fuel_percentage_of_total,is_renewable&format=JSON', headers=self.headers)
            jsonData = json.loads(r.data)

            jsonExtractData = jsonData['items']
            genFuelDf = pd.DataFrame(jsonExtractData)

            genFuelDf.reset_index(drop=True, inplace= True)
            
            
           
            genFuelDf.rename(columns={"datetime_beginning_ept": "timestamp", "fuel_type": "Fuel Type", "mw": "MW", "fuel_percentage_of_total":"Percentage of Total","is_renewable": "Renewable"},inplace =True)
            
            genFuelDf['timestamp'] = pd.to_datetime(genFuelDf['timestamp'].values).strftime('%Y-%m-%d %H:%M:%S')
            genFuelDf['timestamp'] = pd.to_datetime(genFuelDf['timestamp'])
            isoHelper.saveDf(DataTbl='genFuelTbl', Data= genFuelDf)
            i = 1
        except Exception as e:
          print(e)
          print("Unexpected error:", sys.exc_info()[0])
        finally:
            return


    def fetch_LoadForecast(self, isPSEG, isoHelper):
        try:
               if (isPSEG == True):
                    Area = 'pse&g/midatl'
               else:
                    Area = 'midatl'

               params = urllib.parse.urlencode({
                # Request parameters
   
                'rowCount': '50000',
                'sort': 'forecast_datetime_beginning_ept',
                'order': 'asc',
                'startRow': '1',
               
    
                'fields': 'evaluated_at_ept,forecast_datetime_beginning_ept,forecast_area, forecast_load_mw',
   
                'evaluated_at_ept': '5minutesago',
                
                'forecast_area': Area,
                'format':'json'})
               r = self.http.request('GET', "https://api.pjm.com/api/v1/very_short_load_frcst?" + params, headers=self.headers)

               jsonData = json.loads(r.data)

               jsonExtractData = jsonData['items']
               forecastDf = pd.DataFrame(jsonExtractData)

               forecastDf.reset_index(drop=True, inplace= True)
            
           
               forecastDf.rename(columns={"forecast_datetime_beginning_ept": "timestamp", "evaluated_at_ept": "EvaluatedAt", "forecast_area":"Area","forecast_load_mw": "LoadForecast"},inplace =True)
            
               forecastDf['timestamp'] = pd.to_datetime(forecastDf['timestamp'].values).strftime('%Y-%m-%d %H:%M:%S')
               forecastDf['timestamp'] = pd.to_datetime(forecastDf['timestamp'])

               forecastDf['EvaluatedAt'] = pd.to_datetime(forecastDf['EvaluatedAt'].values).strftime('%Y-%m-%d %H:%M:%S')
               forecastDf['EvaluatedAt'] = pd.to_datetime(forecastDf['EvaluatedAt'])
              
               isoHelper.saveDf(DataTbl='forecastTbl', Data= forecastDf)
               i = 1

        except Exception as e:
               print("[Errno {0}] {1}".format(e.errno, e.strerror))
        finally:
                return

    def fetch_hourlyMeteredLoad(self, isPSEG, StartTime, isoHelper):
        try:
             if (isPSEG == True):
                    load_area = 'ps'
             else:
                    load_area = 'midatl'

             params = urllib.parse.urlencode({
             # Request parameters
   
                'rowCount': '50000',
                'sort': 'datetime_beginning_ept',
                'order': 'asc',
                'startRow': '1',
                'datetime_beginning_ept':StartTime,
                'fields': 'datetime_beginning_ept,load_area, mw, is_verified',
                'load_area': load_area,
                'format':'json'})
               
             r = self.http.request('GET', "https://api.pjm.com/api/v1/hrl_load_metered?%s" % params, headers=self.headers)

             jsonData = json.loads(r.data)

             jsonExtractData = jsonData['items']
             forecastDf = pd.DataFrame(jsonExtractData)

             forecastDf.reset_index(drop=True, inplace= True)

             forecastDf.rename(columns={"datetime_beginning_ept": "timestamp",  "load_area":"Area","mw": "Load","is_verified":"Verified"},inplace =True)
            
              
             forecastDf['timestamp'] = pd.to_datetime(forecastDf['timestamp'].values).strftime('%Y-%m-%d %H:%M:%S')
             forecastDf['timestamp'] = pd.to_datetime(forecastDf['timestamp'])

             
             isoHelper.saveDf(DataTbl='psMeteredLoad', Data= forecastDf)
        except Exception as e:
               print("[Errno {0}] {1}".format(e.errno, e.strerror))
        finally:
                return
