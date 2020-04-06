
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
           
            r = self.http.request('GET', 'https://api.pjm.com/api/v1/unverified_five_min_lmps?rowCount='+str(numRows)+'&sort=datetime_beginning_ept&order=desc&startrow=1&name=pseg&fields=datetime_beginning_ept,name,type,five_min_rtlmp,hourly_lmp&format=JSON', headers=self.headers)

            jsonData = json.loads(r.data)

            jsonExtractData = jsonData['items']
            lmpDf = pd.DataFrame(jsonExtractData)

            lmpDf.reset_index(drop=True, inplace= True)
            
           
            lmpDf.rename(columns={"datetime_beginning_ept": "timestamp", "five_min_rtlmp": "5 Minute Weighted Avg. LMP", "hourly_lmp": "Hourly Integrated LMP", "type": "Type", "name": "node_id"},inplace =True)
            
            lmpDf['timestamp']= pd.to_datetime(lmpDf['timestamp'].values).strftime('%Y-%m-%d %H:%M:%S')
            lmpDf['timestamp']= pd.to_datetime(lmpDf['timestamp'])
            isoHelper.saveDf(DataTbl='lmpTbl', Data= lmpDf);
            i=1
        except Exception as e:
          print(e)
          print("Unexpected error:", sys.exc_info()[0])
        finally:
            return



    def fetch_InstantaneousLoad(self, numRows, isoHelper):

        try:
            r = self.http.request('GET', 'https://api.pjm.com/api/v1/inst_load?rowCount='+str(numRows)+'&sort=datetime_beginning_ept&order=desc&startrow=1&fields=datetime_beginning_ept,area,instantaneous_load&format=JSON', headers=self.headers)
            
            jsonData = json.loads(r.data)

            jsonExtractData = jsonData['items']
            loadDf = pd.DataFrame(jsonExtractData)

            loadDf.reset_index(drop=True, inplace= True)
           
            loadDf.rename(columns={"datetime_beginning_ept": "timestamp", "area": "Area", "instantaneous_load": "Instantaneous Load"},inplace =True)
            
            loadDf['timestamp']= pd.to_datetime(loadDf['timestamp'].values).strftime('%Y-%m-%d %H:%M:%S')
            loadDf['timestamp']= pd.to_datetime(loadDf['timestamp'])
            isoHelper.saveDf(DataTbl='loadTbl', Data= loadDf);
            i=1
        except Exception as e:
          print(e)
          print("Unexpected error:", sys.exc_info()[0])
        finally:
            return

        #https://api.pjm.com/api/v1/gen_by_fuel?rowCount=100&sort=datetime_beginning_ept&order=Desc&startRow=1&is_renewable=True

    def fetch_GenFuel(self, numRows, isoHelper):

        try:

   
            r = self.http.request('GET', 'https://api.pjm.com/api/v1/gen_by_fuel?rowCount='+str(numRows)+'&sort=datetime_beginning_ept&order=desc&startrow=1&fields=datetime_beginning_ept,fuel_type,mw,fuel_percentage_of_total,is_renewable&format=JSON', headers=self.headers)
            jsonData = json.loads(r.data)

            jsonExtractData = jsonData['items']
            genFuelDf = pd.DataFrame(jsonExtractData)

            genFuelDf.reset_index(drop=True, inplace= True)
            
            
           
            genFuelDf.rename(columns={"datetime_beginning_ept": "timestamp", "fuel_type": "Fuel Type", "mw": "MW", "fuel_percentage_of_total":"Percentage of Total","is_renewable": "Renewable"},inplace =True)
            
            genFuelDf['timestamp']= pd.to_datetime(genFuelDf['timestamp'].values).strftime('%Y-%m-%d %H:%M:%S')
            genFuelDf['timestamp']= pd.to_datetime(genFuelDf['timestamp'])
            isoHelper.saveDf(DataTbl='genFuelTbl', Data= genFuelDf);
            i=1
        except Exception as e:
          print(e)
          print("Unexpected error:", sys.exc_info()[0])
        finally:
            return