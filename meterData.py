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



class MeterData(object):
    http = urllib3.PoolManager()


    
    def fetchMeterData(self, meters,  numReads, isoHelper):

        try:
            numReads = str(numReads)     
            r = self.http.request('GET','https://io.ekmpush.com/readMeter?key=NjUyMzc0MjQ6Z3NaVmhEd20&meters='+ meters + '&ver=v4&fmt=json&cnt=' + numReads + '&tz=America~New_York&fields=RMS_Volts_Ln_1~RMS_Volts_Ln_2~RMS_Volts_Ln_3~RMS_Watts_Tot~Power_Factor_Ln_1~Power_Factor_Ln_2~Power_Factor_Ln_3&status=good ')
            jsonMeter = json.loads(r.data)
            jsonElectricData = jsonMeter['readMeter']['ReadSet'][0]['ReadData']
            meterDf = pd.DataFrame(jsonElectricData)
            
            meterDf.insert (1, 'timestamp',  pd.to_datetime(meterDf['Date'] + ' '+meterDf['Time']),allow_duplicates = False) 

            meterDf.insert(1, 'MeterID', meters, allow_duplicates = True)
            
            timezone = pytz.timezone("America/New_York")
      

            meterDf['timestamp'] = [timezone.localize(row) for row in  meterDf['timestamp']]

            meterDf.drop(columns=['Good','Date', 'Time','Time_Stamp_UTC_ms'], inplace=True)

            meterDf = meterDf.astype({'RMS_Watts_Tot': float,  'RMS_Volts_Ln_1': float,  'RMS_Volts_Ln_2': float,  'RMS_Volts_Ln_3': float,  
           'Power_Factor_Ln_1': float, 'Power_Factor_Ln_2': float, 'Power_Factor_Ln_3': float})
            print(meterDf)
            isoHelper.saveDf(DataTbl='meterTbl', Data= meterDf);

            i=1

        except Exception as e:
          print(e)
          print("Unexpected error:", sys.exc_info()[0])
        finally:
            return





