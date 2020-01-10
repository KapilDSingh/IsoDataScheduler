
import urllib3
import json
from bs4 import BeautifulSoup
import pandas as pd
from dateutil.parser import parse
import pytz
from datetime import datetime, timedelta
import re
import json
from sqlalchemy import create_engine
import sqlalchemy
import sys
import oasisData


class oasisData(object):
    """description of class"""
    oasis_url = 'http://oasis.pjm.com/system.htm'
    markets_operations_url = 'http://www.pjm.com/markets-and-operations.aspx'

    def parse_date_from_oasis(self, content):
        # find timestamp
        soup = BeautifulSoup(content, 'lxml')

        # the datetime is the only bold text on the page, this could break easily
        ts_elt = soup.find('b')

        # do not pass tzinfos argument to dateutil.parser.parse, it fails arithmetic
        ts = parse(ts_elt.string, ignoretz=True)
        ts = pytz.timezone('US/Eastern').localize(ts)
        #ts = ts.astimezone(pytz.utc)

        # return
        return ts

    def fetch_oasis_data(self, dataType='LMP'):

        http = urllib3.PoolManager()
        
        df = None
        
        try:

            response = http.request('GET', self.oasis_url)

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:

            
            msg = '%s: connection error for %s, %s:\n%s' % (self.NAME, url, kwargs, e)
            print(msg)
            return None
 
            if response.status == 429:
                if retries_remaining > 0:
                    # retry on throttle
                    print('%s: retrying in %d seconds (%d retries remaining), throttled for %s, %s' % (self.NAME, retry_sec, retries_remaining, url, kwargs))
                    sleep(retry_sec)
                    retries_remaining -= 1
                    response = http.request('GET', self.oasis_url)            
                else:
                    print('%s: exhausted retries for oasis, %s' % ( url, kwargs))
                    return None

            else:
                # non-throttle error
               printr('%s: request failure with code oasis for %s, %s' % (response.status_code, url, kwargs))

        if not response:
         return pd.DataFrame()

        # get timestamp
        ts = self.parse_date_from_oasis(response.data)

        # parse to dataframes
        dfs = pd.read_html(response.data, header=0, index_col=0, parse_dates=False)

        if dataType == 'LMP':
           df = dfs[1]
            # parse lmp
          
                      
           df['timestamp'] = ts
           df['node_id'] = df.index
           
           df.reset_index(drop=True, inplace= True)
           df.set_index('node_id', inplace=True)
           #print(df)
           col_name = df.columns[2];
           # rename 'Hourly Integrated LMP for Hour Ending XX' and 'Type' columns
           rename_d = {
               col_name: 'Hourly Integrated LMP'
               
               }
           df.rename(columns=rename_d, inplace=True)
           return df

        elif dataType == 'LOAD':
            # parse real-time load
            df = dfs[4]
            df['timestamp'] = ts
            df['load'] = df.loc['PJM RTO'][0]
            return df

        else:
            raise ValueError('Cannot parse OASIS LMP data for %s' % self.options['data'])
        return None

    def fetch_markets_operations_soup(self):

        http = urllib3.PoolManager()
        response = http.request('GET', self.markets_operations_url)
        if not response:
            return None

        soup = BeautifulSoup(response.data, 'lxml')
        return response

    def saveDf(self, DataTbl='lmpTbl', Data='df'):
        engine = create_engine(r'sqlite:///C:\Users\Kapil\OneDrive\isoData\isoDataDB.db')
      
        try:
           Data.to_sql(DataTbl, engine, if_exists = 'append', index=False)
        except sqlalchemy.exc.IntegrityError as e:
           print(e)
        except BaseException as e:
          print (e)
          print("Unexpected error:", sys.exc_info()[0])
        finally:
           engine.connect().close()
           

    def getLmp_period(self, start = "2018-07-26 13:10:00.000000", end =  "2018-07-26 13:20:00.000000", nodeId='PSEG'):
        
        df =None
        engine = create_engine(r'sqlite:///C:\Users\Kapil\OneDrive\isoData\isoDataDB.db')
        
        try:
            #sql_query ='select timestamp, node_id, [5 Minute Weighted Avg. LMP] from lmpTbl where node_id = %(nodeId)s and ( timestamp between %(start)s  and %(end)s )  order by timestamp asc'
            #df = pd.read_sql(sql_query, engine, params ={
            #                                               'node_id': nodeId,
            #                                               'start' : start,
            #                                               'end' : end
            #                                             })
            sql_query ='select timestamp, node_id, [5 Minute Weighted Avg. LMP] from lmpTbl where node_id = :nodeId and ( timestamp between :start  and :end )  order by timestamp asc'
            df = pd.read_sql(sql_query, engine, params ={
                                                           'nodeId': nodeId,
                                                           'start' : start,
                                                           'end' : end
                                                         })
            #df = pd.read_sql(sql_query, engine, params ={
            #                                               'nodeId': nodeId,
            #                                             })


        except BaseException as e:
          print (e)
  
        finally:
            engine.connect().close()

        return df

    def getLmp_latest(self,  nodeId='PSEG', numIntervals=12):
        
        df =None
        engine = create_engine(r'sqlite:///C:\Users\Kapil\OneDrive\isoData\isoDataDB.db')
        
        try:
         #                                             })
            sql_query ='select  timestamp, node_id, [5 Minute Weighted Avg. LMP] from lmpTbl where node_id = :nodeId order by timestamp desc limit :count'
            df = pd.read_sql(sql_query, engine, params ={
                                                           'nodeId': nodeId,
                                                           'count':numIntervals,
                                                        })
                                                                    
            df.sort_values(by=['timestamp', 'node_id'], ascending=True, inplace=True) 
            #df = pd.read_sql(sql_query, engine, params ={
            #                                               'nodeId': nodeId,
            #                                             })


        except BaseException as e:
          print (e)
  
        finally:
            engine.connect().close()

        return df