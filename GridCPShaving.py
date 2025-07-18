from pprint import pprint as pp
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
plt.style.use('seaborn-poster')
import scipy.signal
from pandas.plotting import register_matplotlib_converters
from datetime import datetime, timedelta
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import FunctionFilter
from sqlalchemy.sql.functions import Function
from sqlalchemy.orm import Session, sessionmaker
from scipy.signal import find_peaks, peak_prominences

class GridCPShaving(object):
    """description of class"""

    def peakSignal(self, forecastDf, Area, isHrly):

        try:
            #register_matplotlib_converters()
            
            if (Area == 'ps'):
               minLoadHeight = 6000
               prominenceHgt = 1

            else:
                minLoadHeight = 100000
                prominenceHgt = 3

            if (isHrly == True):
                divisor = forecastDf.ForecstNumReads
                series = forecastDf.HrlyForecstLoad   / divisor
                # find all the peaks that associated with the positive peaks
                peaks_positive, _ = find_peaks(series, height = minLoadHeight, prominence = prominenceHgt, \
                                               threshold = None, distance=24)

                prominences = peak_prominences(series, peaks_positive)[0]
                contour_heights = series[peaks_positive] - prominences

                forecastDf.loc[:, 'Peak'] =0;

                if (len(peaks_positive) > 0):
                    forecastDf.loc[peaks_positive, 'Peak'] = 1
                
                #forecastDf.set_index('timestamp', inplace=True) 
                #plt.figure(figsize = (1200,800))
                #plt.plot_date(forecastDf.index, series, linewidth = 2)

                #plt.plot_date(forecastDf.index[peaks_positive], series[peaks_positive], 'ro', label = 'positive peaks')
                
                #plt.vlines(x=forecastDf.index[peaks_positive], ymin=contour_heights, ymax= series[peaks_positive])

                #plt.ylabel('Load Forecast (MW)')
                #plt.show()
                #forecastDf.reset_index(inplace=True)
            else:
                # find all the peaks that associated with the positive peaks
                peaks_positive, _ = scipy.signal.find_peaks(forecastDf.LoadForecast, height = minLoadHeight,\
                   threshold = None, distance=480)
                contour_heights = None

                forecastDf.loc[:, 'Peak'] =0;

                if (len(peaks_positive) > 0):
                    forecastDf.loc[peaks_positive, 'Peak'] = 1

        except Exception as e:
            print("peakSignal",e)
        finally:
           
            return forecastDf, contour_heights



    def findPeaks(self, oldestTimestamp, Area, isHrly, isoHelper):

        if (Area == 'ps') and isHrly == False:
            DataTbl = 'forecastTbl'
        elif (Area == 'ps') and isHrly == True:
            DiagnosticsTbl = 'psHrlyDiagnosticTbl'
            DataTbl = 'psHrlyForecstTbl'
        elif (Area == 'PJM RTO') and isHrly == False:
            DataTbl = 'rtoForecastTbl'
        elif (Area == 'PJM RTO') and isHrly == True:
            DiagnosticsTbl = 'rtoHrlyDiagnosticTbl'
            DataTbl = 'rtoHrlyForecstTbl'

        forecastDf = None
        dailyPeakOn = False
        peakOn = False

        try:
            connection = isoHelper.engine.connect()

            firstTimeStamp, endTimeStamp = isoHelper.getStartEndForecastTimestamp(Area, isHrly)
            startTimeStamp = oldestTimestamp.replace(hour = 0, minute=0, second = 0, microsecond =0)

            startTimeStr = startTimeStamp.strftime("%Y-%m-%dT%H:%M:%S")
            endTimeStr = endTimeStamp.strftime("%Y-%m-%dT%H:%M:%S")

            sql_query = "update " + DataTbl + " set Peak = 0" + " where timestamp >= CONVERT(DATETIME,'" + startTimeStr + \
                "')  and timestamp <= CONVERT(DATETIME,'" + endTimeStr + "')"
               
            result = connection.execute(sql_query)
            
            if (isHrly==True):

                sql_query = "SELECT  [timestamp] \
                        ,[ForecstNumReads]\
                        ,[HrlyForecstLoad]\
                        ,[Peak]\
                        ,[EvaluatedAt]\
                        FROM [ISODB].[dbo]." + DataTbl + " where timestamp >= CONVERT(DATETIME,'" + startTimeStr + "') \
                        and timestamp <= CONVERT(DATETIME,'" + endTimeStr + "')" + " order by timestamp"
            else:
                sql_query = "SELECT  [timestamp] \
                        ,[LoadForecast]\
                        ,[EvaluatedAt] ,[Area],[Peak]\
                        FROM [ISODB].[dbo]." + DataTbl + " where timestamp >= CONVERT(DATETIME,'" + startTimeStr + "') \
                        and timestamp <= CONVERT(DATETIME,'" + endTimeStr + "')" + " order by timestamp"


            forecastDf = pd.read_sql_query(sql_query, isoHelper.engine) 
            forecastDf.reset_index(drop =True, inplace=True)

            forecastDf, peakProminence = self.peakSignal(forecastDf, Area, isHrly)

            #if (isHrly==True) :
            #     ###########################################################
            #    forecastDf.loc[13,'Peak']= 1
            #     ###########################################################

            ret = isoHelper.replaceDf(DataTbl, forecastDf)
                     
            if (isHrly==True) :

                peakDf = forecastDf[forecastDf['Peak'] > 0]
                peakDf.reset_index(drop=True, inplace=True)

                if  (len(peakDf) == 1):

                    if (Area == 'PJM RTO'):
                        minLoad = 135000
                    else:
                        minLoad = 8900

                    peakStartTime = peakDf['timestamp'][0]  + timedelta(hours =-1)
                    oneHourAftPeakHour = peakDf['timestamp'][0]  + timedelta(hours =1)

                    timeNow =  datetime.now()
                    currentTimeStamp = pd.Timestamp(timeNow.strftime("%Y-%m-%dT%H:%M:%S"))
                    currentTimeStamp5min = currentTimeStamp.round('5min')


                    dailyPeakOn = True

                    HighestPeaksDf = self.CheckCPShaveHour(DataTbl, minLoad,  isoHelper)
                    if  (np.datetime64(peakDf['timestamp'][0])  in HighestPeaksDf['timestamp'].values):
                        peakDf.at[0, 'Peak'] = 2
                        peakOn = True
 
                        if  ((peakStartTime <= currentTimeStamp5min) and (currentTimeStamp5min <= peakDf['timestamp'][0])):
                            print ('Time = ', currentTimeStamp5min.strftime("%d/%m/%Y %H:%M"), 'Area = ', Area, "START Shaving")

                            EvalAt = peakDf['EvaluatedAt'][0]
                            NumReads = peakDf['ForecstNumReads'][0]
                            HrlyForecstLoad =  peakDf['HrlyForecstLoad'][0]
                            load = peakDf['HrlyForecstLoad'][0] /  peakDf['ForecstNumReads']
                            currentHrlyLoad = load.loc[0]

                        elif (currentTimeStamp5min > peakDf['timestamp'][0]) and (currentTimeStamp5min <= oneHourAftPeakHour) :

                            params= [peakDf['timestamp'][0], Area]
                            Results = isoHelper.call_procedure("[NewIsLoadDecreasing] ?, ?",params)

                            EvalAt = Results[0][1]
                            NumReads =  Results[0][2]
                            HrlyForecstLoad =  Results[0][3]
                            currentHrlyLoad = Results[0][4]
                            prevHrlyLoad = Results[0][7]

                            if (prevHrlyLoad > currentHrlyLoad):
                                dailyPeakOn = False
                                print ('Time = ', currentTimeStamp5min.strftime("%d/%m/%Y %H:%M"), 'Area = ', Area, "STOP Shaving")
                            else:
                                print ('Time = ', currentTimeStamp5min.strftime("%d/%m/%Y %H:%M"), 'Area = ', Area, "Extended Shaving")
                                dailyPeakOn = True

                        else:
                                dailyPeakOn = False
                                print ('Time = ', currentTimeStamp5min.strftime("%d/%m/%Y %H:%M"), 'Area = ', Area, "STOP Shaving. It is either before start time or after peak shaving time")

                                EvalAt = currentTimeStamp5min
                                NumReads =  0
                                HrlyForecstLoad =  0
                                currentHrlyLoad = 0
                                prevHrlyLoad = 0

                        data =[ [peakDf['timestamp'][0], Area, peakDf['Peak'][0], EvalAt, peakStartTime, NumReads, HrlyForecstLoad, currentHrlyLoad, dailyPeakOn]]

                        peakSignalDf = pd.DataFrame(data, columns=['timestamp', 'Area', 'Peak', 'EvaluatedAt', 'startPeakTime', 'ForecstNumReads', 'HrlyForecstLoad', 'HrlyLoad', 'DailyPeakON'])

                        ret= isoHelper.saveDf(DataTbl='peakSignalTbl', Data= peakSignalDf)
                        if (ret == False):
                            print ("2 ret =", ret)
                    else:
                        peakOn = False


        except BaseException as e:
            print("findPeaks",e)
        finally:
            connection.close()
            return  (peakOn and dailyPeakOn)

    def CheckCPShaveHour(self, forecastTbl, minLoad,  isoHelper):

        try:
            connection = isoHelper.engine.connect()


            sql_query = "SELECT TOP (5) timestamp \
                FROM  " + forecastTbl + \
            " WHERE (Peak > 0) AND (timestamp >= '2025-6-1') and \
            (timestamp < '2025-10-1') and \
            (HrlyForecstLoad / ForecstNumReads) > "  + str(minLoad)  + \
            " order by (HrlyForecstLoad / ForecstNumReads) desc"

            HighestPeaksDf = pd.read_sql_query(sql_query, isoHelper.engine)    
            HighestPeaksDf.reset_index(drop=True,inplace=True)

            HighestPeaksDf['timestamp'] =HighestPeaksDf['timestamp'].apply(lambda x: x.round(freq='T'))
            for timestamp in HighestPeaksDf['timestamp']:
               sql_query = "Update " +  forecastTbl + \
                        " set Peak = 2 where timestamp = '" + timestamp.strftime("%Y-%m-%dT%H:%M:%S")+"'"

               result = connection.execute(sql_query)

            connection.close()
  

        except BaseException as e:
            print("CheckCPShaveHour",e)
        finally:
            return HighestPeaksDf

    def checkPeakEnd(self,  Area, currentTimeStamp, isoHelper):

        if (Area == 'ps'):

            forecastTbl = 'psHrlyForecstTbl'
            DataTbl = 'psHrlyLoadTbl'

        

        elif (Area == 'PJM RTO'):

            forecastTbl = 'rtoHrlyForecstTbl'
            DataTbl = 'rtoHrlyLoadTbl'

        forecastDf = None

        try:
            zeroTimeStamp = currentTimeStamp.replace(hour = 0, minute=0, second = 0, microsecond =0)

            sql_query = "Update [ISODB].[dbo]." + forecastTbl + " set Peak=3 where timestamp in "\
                            "(SELECT   [ISODB].[dbo]." + forecastTbl + ".timestamp \
                        FROM  [ISODB].[dbo]." + forecastTbl + " INNER JOIN \
                         [ISODB].[dbo]." + DataTbl + " ON [ISODB].[dbo]." + forecastTbl + \
                         ".timestamp = DATEADD (hour , -1 ,  [ISODB].[dbo]." + DataTbl + ".timestamp )  \
                         WHERE       ( ([ISODB].[dbo]." + forecastTbl + ".Peak > 0) and \
                         ([ISODB].[dbo]." + forecastTbl + ".HrlyForecstLoad/ [ISODB].[dbo]." + forecastTbl + ".ForecstNumReads \
                         < [ISODB].[dbo]." + DataTbl + ".HrlyInstLoad / [ISODB].[dbo]." + DataTbl + ".NumReads)))"

            connection = isoHelper.engine.connect()

            result = connection.execute(sql_query)

            
            numIncorrectPeaks = connection.execute("SELECT COUNT(*) FROM " + forecastTbl + " where Peak=3").scalar()

            sql_query = "SELECT   t2.timestamp FROM  [ISODB].[dbo]." + DataTbl + " as t1 INNER JOIN  \
                [ISODB].[dbo]." + DataTbl + " as t2 ON t2.timestamp = DATEADD (hour , -1 ,  t1.timestamp ) INNER JOIN \
                [ISODB].[dbo]." + forecastTbl + " ON [ISODB].[dbo]." + forecastTbl + ".timestamp = DATEADD (hour , -1 ,  t1.timestamp )\
                WHERE       ( ([ISODB].[dbo]." + forecastTbl + ".Peak=2) and (t1.HrlyInstLoad/ t1.NumReadsb \
                > t2.HrlyInstLoad / t2.NumReads) and t2.timestamp > '"+ zeroTimeStamp + "') order by t1.timestamp desc"


            ExtendEndTimeStamp = connection.execute(sql_query).scalar()

            if (ExtendEndTimeStamp != null):
                startTimeStamp, endTimeStamp = isoHelper.getStartEndForecastTimestamp(Area, false)
                sql_query = "update peakTable set Overtime = '" + endTimeStamp.strftime("%Y-%m-%dT%H:%M:%S") + \
                    "' where timestamp = '" + ExtendEndTimeStamp.strftime("%Y-%m-%dT%H:%M:%S") + "' and area = '" + Area + "'"
                result = connection.execute(sql_query)

            


        except BaseException as e:
            print("checkPeaks",e)
        finally:
            connection.close()
            return 

    #def checkDemandMgmtStart(self,  isoHelper):
   

    #    try:
    #        sql_query = "Update [ISODB].[dbo]." + forecastTbl + " set Peak=3 where timestamp in "\
    #                        "(SELECT   [ISODB].[dbo]." + forecastTbl + ".timestamp \
    #                    FROM  [ISODB].[dbo]." + forecastTbl + " INNER JOIN \
    #                     [ISODB].[dbo]." + DataTbl + " ON [ISODB].[dbo]." + forecastTbl + \
    #                     ".timestamp = DATEADD (hour , -1 ,  [ISODB].[dbo]." + DataTbl + ".timestamp )  \
    #                     WHERE       ( ([ISODB].[dbo]." + forecastTbl + ".Peak > 0) and \
    #                     ([ISODB].[dbo]." + forecastTbl + ".HrlyForecstLoad/ [ISODB].[dbo]." + forecastTbl + ".ForecstNumReads \
    #                     < [ISODB].[dbo]." + DataTbl + ".HrlyInstLoad / [ISODB].[dbo]." + DataTbl + ".NumReads)))"

    #        connection = isoHelper.engine.connect()

    #        result = connection.execute(sql_query)

            
    #        numIncorrectPeaks = connection.execute("SELECT COUNT(*) FROM " + forecastTbl + " where Peak=3").scalar()


    #    except BaseException as e:
    #        print("checkPeaks",e)
    #    finally:
    #        connection.close()
    #        return 

    def getShaveSignal(self, startTimestamp,  isoHelper):

        try:                
            StartTimeStr = startTimestamp.strftime("%Y-%m-%dT%H:%M:%S")
            
            shaveSignalQuery = "SELECT  [timestamp], [Area], [Peak], [EvaluatedAt],  [startPeakTime], [HrlyLoad], [DailyPeakON] \
                    FROM [ISODB].[dbo].[peakSignalTbl]  where DailyPeakON > 0 and EvaluatedAt >=  CONVERT(DATETIME,'" + StartTimeStr + "' order by EvaluatedAt  )"

            shaveDf = pd.read_sql(shaveSignalQuery,isoHelper.engine)
              

        except BaseException as e:
            print("getShaveSignal ",e)
  
        finally:
            return shaveDf

    def leanShaveSignal(self, shaveDf):

        try:                
         
            # Python3 code to demonstrate working of
            # Grouped Consecutive Range Indices of Elements
            # Using groupby() + defaultdict() + len() + loop
            from itertools import groupby
            from collections import defaultdict

            # initializing lists
            test_list = shaveDf[['DailyPeakON']]

            # printing string
            print("The original list : " + str(test_list))

            idx = 0
            res = defaultdict(list)

            # grouping Consecutives
            for key, sub in groupby(test_list):
                ele = len(list(sub))
     
                # append strt index, and till index
                res[key].append((idx, idx + ele - 1))
                idx += ele

            # printing results
            print("The grouped dictionary : " + str(dict(res)))

        except BaseException as e:
            print("leanShaveSignal ",e)
  
        finally:
            return shaveDf

