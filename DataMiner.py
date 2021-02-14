
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
from IsodataHelpers import IsodataHelpers
import numpy as np
import matplotlib.pyplot as plt


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
            lmpDf = lmpDf.sort_values('timestamp').drop_duplicates('timestamp',keep='last')

            if (numRows > 1):
                oldestTimestamp =lmpDf['timestamp'].min()
                isoHelper.clearTbl(oldestTimestamp, 'lmpTbl')

            isoHelper.saveDf(DataTbl='lmpTbl', Data= lmpDf)
            i = 1
        except Exception as e:
          print("Fetch LMP Unexpected error:",e)

        finally:
            return



    def fetch_InstantaneousLoad(self, numRows,  Area, isoHelper):

        try:

            r = self.http.request('GET', 'https://api.pjm.com/api/v1/inst_load?area=' + Area + '&rowCount=' + str(numRows) + '&sort=datetime_beginning_ept&order=desc&startrow=1&fields=datetime_beginning_ept,area,instantaneous_load&format=JSON', headers=self.headers)
            
            jsonData = json.loads(r.data)


            jsonExtractData = jsonData['items']
            loadDf = pd.DataFrame(jsonExtractData)

            loadDf.reset_index(drop=True, inplace= True)
            
           
            loadDf.rename(columns={"datetime_beginning_ept": "timestamp", "area": "Area", "instantaneous_load": "Load"},inplace =True)
            
            loadDf['timestamp'] = pd.to_datetime(loadDf['timestamp'].values).strftime('%Y-%m-%d %H:%M:%S')
            loadDf['timestamp'] = pd.to_datetime(loadDf['timestamp'])
            loadDf = loadDf.sort_values('timestamp').drop_duplicates('timestamp',keep='last')

            ret = isoHelper.saveLoadDf(Area, False, loadDf)

            if ((numRows ==1) and  ret==True) :
                timestamp = loadDf.loc[0, 'timestamp']

                mergeDt = loadDf['timestamp'][0]
                mergeDt = mergeDt.replace(minute=mergeDt.minute-0)
                     
                if (Area =='ps'):
                    isoHelper.mergePSEGTimeSeries(mergeDt)
                    isoHelper.mergePSEGHrlySeries(mergeDt)
                else:
                    isoHelper.mergeRTOTimeSeries(mergeDt)
                    isoHelper.mergeRTOHrlySeries(mergeDt)

 
        except Exception as e:
  
            print("Fetch Instantaneous Load Unexpected error:", e)
        finally:
            return ret



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
         
          print("Fetch GenFuel Unexpected error:", e)
        finally:
            return


    def fetch_LoadForecast(self, Area, isoHelper,GCPShave):
        try:
               if (Area == 'ps'):
                    AreaCode = 'PSE&G/MIDATL'
               else:
                    AreaCode = 'RTO_COMBINED'

               params = urllib.parse.urlencode({
                # Request parameters
   
                'rowCount': '50000',
                'sort': 'forecast_datetime_beginning_ept',
                'order': 'asc',
                'startRow': '1',
               
    
                'fields': 'evaluated_at_ept,forecast_datetime_beginning_ept,forecast_area, forecast_load_mw',
   
                'evaluated_at_ept': '5minutesago',
                
                'forecast_area': AreaCode,
                'format':'json'})
               r = self.http.request('GET', "https://api.pjm.com/api/v1/very_short_load_frcst?" + params, headers=self.headers)

               jsonData = json.loads(r.data)

               jsonExtractData = jsonData['items']
               forecastDf = pd.DataFrame(jsonExtractData)

               forecastDf.reset_index(drop=True, inplace= True)
            
           
               forecastDf.rename(columns={"forecast_datetime_beginning_ept": "timestamp", \
                   "evaluated_at_ept": "EvaluatedAt", "forecast_area":"Area","forecast_load_mw": "LoadForecast"},inplace =True)

               forecastDf['Area'] = Area

               #forecastDf['timestamp'] = pd.to_datetime(forecastDf['timestamp'].values).strftime('%Y-%m-%d %H:%M:%S')
               forecastDf['timestamp'] = pd.to_datetime(forecastDf['timestamp'])

               #forecastDf['EvaluatedAt'] = pd.to_datetime(forecastDf['EvaluatedAt'].values).strftime('%Y-%m-%d %H:%M:%S')
               forecastDf['EvaluatedAt'] = pd.to_datetime(forecastDf['EvaluatedAt'])
               
               forecastDf['Peak'] = 0 

               #forecastDf = forecastDf.sort_values('EvaluatedAt').drop_duplicates('timestamp',keep='last')
               
               newestEvaluatedAt = forecastDf['EvaluatedAt'].max()

               forecastDf = forecastDf[forecastDf['EvaluatedAt'] == newestEvaluatedAt]

               newestTimestamp =forecastDf['timestamp'].max()

               oldestTimestamp =forecastDf['timestamp'].min()
 
               forecastDf = forecastDf.sort_values('timestamp')


               dfTimeStamp = isoHelper.get_latest_Forecast(Area,isShortTerm=True)
               
               forecastDf.reset_index(drop=True,inplace=True)
               
               if (( dfTimeStamp.empty) or newestTimestamp > dfTimeStamp.iloc[0,0]) :

                   isoHelper.saveLoadDf(Area, True, forecastDf);
                   
                   GCPShave.findPeaks(oldestTimestamp, Area, False, True, isoHelper)
                   GCPShave.findPeaks(oldestTimestamp,Area, True, True, isoHelper)


        except Exception as e:
               print("fetch_LoadForecast",e)
        finally:
                return

    def fetch_YrHrlyEvalLoadForecast(self, Area, isoHelper):
        try:
            if (Area == 'ps'):
                AreaCode = 'pse&g/midatl'
            elif (Area == 'RTO'):
                AreaCode = 'RTO_COMBINED'

            eastern = timezone('US/Eastern')
            endTime =datetime.now(eastern);
            startTime =  datetime(endTime.year, endTime.month, endTime.day-1, tzinfo=eastern)

            periodTime = startTime

            while (periodTime < endTime):

                periodTime = periodTime + timedelta(minutes =5)
                periodTimeStr = periodTime.strftime("%Y-%m-%dT%H:%M:%S")

                if (periodTime > endTime):
                    periodTime = endTime
            
                params = urllib.parse.urlencode({
                # Request parameters
   
                'rowCount': '1',
                'sort': 'evaluated_at_ept',
                'order': 'asc',
                'startRow': '1',
               
    
                'fields': 'evaluated_at_ept,forecast_datetime_beginning_ept,forecast_area, forecast_load_mw',
   
                'evaluated_at_ept': periodTimeStr + " to " + periodTimeStr,
                'forecast_datetime_beginning_ept':periodTimeStr + " to " + periodTimeStr,
                'forecast_area': AreaCode,
                'format':'json'})

                r = self.http.request('GET', "https://api.pjm.com/api/v1/very_short_load_frcst?" + params, headers=self.headers)

                jsonData = json.loads(r.data)

                jsonExtractData = jsonData['items']

                if (len(jsonExtractData) > 0):

                    forecastDf = pd.DataFrame(jsonExtractData)

                    forecastDf.reset_index(drop=True, inplace= True)
            
           
                    forecastDf.rename(columns={"forecast_datetime_beginning_ept": "timestamp", \
                        "evaluated_at_ept": "EvaluatedAt", "forecast_area":"Area","forecast_load_mw": "LoadForecast"},inplace =True)
            
                    forecastDf['timestamp'] = pd.to_datetime(forecastDf['timestamp'].values).strftime('%Y-%m-%d %H:%M:%S')
                    forecastDf['timestamp'] = pd.to_datetime(forecastDf['timestamp'])

                    forecastDf['EvaluatedAt'] = pd.to_datetime(forecastDf['EvaluatedAt'].values).strftime('%Y-%m-%d %H:%M:%S')
                    forecastDf['EvaluatedAt'] = pd.to_datetime(forecastDf['EvaluatedAt'])
                    forecastDf['Area'] = Area
                    forecastDf['Peak'] = 0 

                    isoHelper.saveLoadDf(Area, True, forecastDf);
 
        except Exception as e:
                print("fetch_YrHrlyEvalLoadForecast",e)
        finally:
                return


    def fetch_hourlyMeteredLoad(self, Area, startMeteredPeriod, endMeteredPeriod, update, isoHelper):
        try:
             if (Area == 'ps'):
                    load_area = 'ps'
             else:
                    load_area = 'RTO'

             params = urllib.parse.urlencode({
             # Request parameters
             

                'rowCount': '50000',
                'sort': 'datetime_beginning_ept',
                'order': 'asc',
                'startRow': '1',
                'datetime_beginning_ept':startMeteredPeriod.strftime('%Y-%m-%d %H:%M') + " to " +endMeteredPeriod.strftime('%Y-%m-%d %H:%M'), 
               
                'fields': 'datetime_beginning_ept,load_area, mw, is_verified',
                'load_area': load_area,
                'format':'json'})
             if (update == False):  
                r = self.http.request('GET', "https://api.pjm.com/api/v1/hrl_load_metered?%s" % params, headers=self.headers)
             else:
                r = self.http.request('GET', "https://api.pjm.com/api/v1/hrl_load_metered?rowCount=24&sort=datetime_beginning_ept&order=Desc&startRow=1&fields=datetime_beginning_ept,load_area, mw, is_verified&load_area=" + load_area, headers=self.headers)

             jsonData = json.loads(r.data)

             jsonExtractData = jsonData['items']
             forecastDf = pd.DataFrame(jsonExtractData)

             forecastDf.reset_index(drop=True, inplace= True)

             forecastDf.rename(columns={"datetime_beginning_ept": "timestamp",  "load_area":"Area","mw": "Load","is_verified":"Verified"},inplace =True)
            
              
             forecastDf['timestamp'] = pd.to_datetime(forecastDf['timestamp'].values).strftime('%Y-%m-%d %H:%M:%S')
             forecastDf['timestamp'] = pd.to_datetime(forecastDf['timestamp'])

             if (Area):
                isoHelper.saveDf(DataTbl='psMeteredLoad', Data= forecastDf)
             else:
                isoHelper.saveDf(DataTbl='pjmMeteredLoad', Data= forecastDf)

        except Exception as e:
               print(e)
        finally:
                return



    

    def PSEGLoadCurve(self, startMonth, endMonth, isoHelper, include):

        PSEGLoadDf= isoHelper.getPSEGLoad(startMonth, endMonth, include)


        KWArr = PSEGLoadDf['Load']
        KW=np.sort(KWArr)
        maxKW=10000
        minKW=3000
        bins = np.arange(minKW,maxKW,500)
        dist = np.histogram(KW,bins=bins, density=True)
        x=dist[0]*500
  
        xSum=np.sum(x)

        x=np.insert(x,0,0)
        y=dist[1]

        xCum = np.cumsum(x)
        
        flipxCum = 1-xCum
        #flipxCum =list(map(lambda x : x if (x > 0.00001) else None, flipxCum));
        #flipxCum =list(map(lambda x : x if (x >0) else None, flipxCum));             
        Type=str(startMonth) + "-" + str(endMonth) + str(include);

        histDf = pd.DataFrame({'Type':Type, 'Percentile':flipxCum, 'MW':y})
        #histDf.dropna(inplace=True)
        isoHelper.saveDf(DataTbl='PSEGLoadHist', Data= histDf);
              
        return flipxCum, y

    def genHist(self, meter, isoHelper):
        engine = create_engine('mssql+pyodbc://Kapil:Acfjo12#@ISODSN');
        connection = engine.connect()

        result = connection.execute("delete from ConsumptionHist")  
        connection.close()
        flipxCum, y = self.LoadCurveUtilityMeter(meter, 6, 9, isoHelper, True);
        #plt.scatter( flipxCum,y, marker='o',  color='skyblue', linewidth=1)
      
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

    def genPSEGLoadHist(self,  isoHelper):
        engine = create_engine('mssql+pyodbc://Kapil:Acfjo12#@ISODSN');
        connection = engine.connect()

        result = connection.execute("delete from PSEGLoadHist")  
        connection.close()
        flipxCum, y = self.PSEGLoadCurve(6, 9, isoHelper, True);
        #plt.scatter( flipxCum,y, marker='o',  color='skyblue', linewidth=1)
      
        flipxCum1, y1=self.PSEGLoadCurve(6, 9, isoHelper, False);
        #plt.scatter( flipxCum1,y1, marker='o',  color='red', linewidth=1)
        #plt.show()
        flipxCum2, y2=self.PSEGLoadCurve(1, 12, isoHelper, True);
        #plt.scatter( flipxCum2,y2, marker='o',  color='green', linewidth=1)
        #plt.show()
        flipxCum3, y3=self.PSEGLoadCurve(1, 1, isoHelper, True);
        #plt.scatter( flipxCum3,y3, marker='o',  color='yellow', linewidth=1)
        #plt.show()
        self.PSEGLoadCurve(2, 2, isoHelper, True);
        self.PSEGLoadCurve(3, 3, isoHelper, True);
        self.PSEGLoadCurve(4, 4, isoHelper, True);
        self.PSEGLoadCurve(5, 5, isoHelper, True);
        self.PSEGLoadCurve(6, 6, isoHelper, True);
        self.PSEGLoadCurve(7, 7, isoHelper, True);
        self.PSEGLoadCurve(8, 8, isoHelper, True);
        self.PSEGLoadCurve(9, 9, isoHelper, True);
        self.PSEGLoadCurve(10, 10, isoHelper, True);
        self.PSEGLoadCurve(11, 11, isoHelper, True);
        self.PSEGLoadCurve(12, 12, isoHelper, True);
        return




    def RTOLoadCurve(self, startMonth, endMonth, isoHelper, include):

        RTOLoadDf= isoHelper.getRTOLoad(startMonth, endMonth, include)


        KWArr = RTOLoadDf['Load']
        KW=np.sort(KWArr)
        maxKW=154000
        minKW=54000
        bins = np.arange(minKW,maxKW,1000)
        dist = np.histogram(KW,bins=bins, density=True)
        x=dist[0]*1000
  
        xSum=np.sum(x)

        x=np.insert(x,0,0)
        y=dist[1]

        xCum = np.cumsum(x)
        
        flipxCum = 1-xCum
        flipxCum =list(map(lambda x : x if (x > 0.0000001) else None, flipxCum));
                         
        Type=str(startMonth) + "-" + str(endMonth) + str(include);

        histDf = pd.DataFrame({'Type':Type, 'Percentile':flipxCum, 'MW':y})
        #histDf.dropna(inplace=True)
        isoHelper.saveDf(DataTbl='RTOLoadHist', Data= histDf);
              
        return flipxCum, y


    def genRTOLoadHist(self,  isoHelper):
        engine = create_engine('mssql+pyodbc://Kapil:Acfjo12#@ISODSN');
        connection = engine.connect()

        result = connection.execute("delete from RTOLoadHist")  
        connection.close()
        flipxCum, y = self.RTOLoadCurve(6, 9, isoHelper, True);
        #plt.scatter( flipxCum,y, marker='o',  color='skyblue', linewidth=1)
        #plt.show()
        flipxCum1, y1=self.RTOLoadCurve(6, 9, isoHelper, False);
        #plt.scatter( flipxCum1,y1, marker='o',  color='red', linewidth=1)
        #plt.show()
        flipxCum2, y2=self.RTOLoadCurve(1, 12, isoHelper, True);
        #plt.scatter( flipxCum2,y2, marker='o',  color='green', linewidth=1)
        #plt.show()
        
        return
