class meterData(object):
    """description of class"""
    TZ_NAME = 'America/New_York'
    Fl2Meter_url ='https://summary.ekmmetering.com/summary?key=NjUyMzc0MjQ6Z3NaVmhEd20&meters=550001081&format=json&report=15&limit=10&offset=0&timelimit=1&timezone='+TZ_NAME+'&fields=kWh_Tot_Max~kWh_Tot_Min~RMS_Volts_Ln_1_Average~RMS_Volts_Ln_2_Average~RMS_Volts_Ln_3_Average&normalize=1'



    
def fetch_oasis_data(self, dataType='LMP'):

        http = urllib3.PoolManager(2)
        
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

        current_hour = ts.hour

        # parse to dataframes
        dfs = pd.read_html(response.data, header=0, index_col=0, parse_dates=False)

        if dataType == 'LMP':
           df = dfs[1]
            # parse lmp
          
                      
           df['timestamp'] = ts
           df['node_id'] = df.index
           
           df.reset_index(drop=True, inplace= True)
           
           df=df.loc[df['node_id'] =='PSEG']
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

