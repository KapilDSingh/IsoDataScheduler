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
                maxLoadHeight = 3000
                prominenceHgt = 10

            else:
                maxLoadHeight = 45000
                prominenceHgt = 45

            if (isHrly == True):
                divisor = forecastDf.ForecstNumReads
                series = forecastDf.HrlyForecstLoad   / divisor
                # find all the peaks that associated with the positive peaks
                peaks_positive, _ = find_peaks(series, height = maxLoadHeight, prominence = prominenceHgt, \
                                               threshold = None, distance=24)
                ## find all the peaks that associated with the negative peaks
                #peaks_negative, _ = scipy.signal.find_peaks(-forecastDf.HrlyForecstLoad \
                #   / divisor, height = minLoadHeight, threshold = None, distance=10)
                prominences = peak_prominences(series, peaks_positive)[0]
                contour_heights = series[peaks_positive] - prominences

                forecastDf.loc[:, 'Peak'] =0;

                if (len(peaks_positive) > 0):
                    forecastDf.loc[peaks_positive, 'Peak'] = 1
                
            
                #if (len(peaks_negative)>0):
                #    forecastDf.loc[peaks_negative, 'Peak'] = -1

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
                peaks_positive, _ = scipy.signal.find_peaks(forecastDf.LoadForecast, height = maxLoadHeight,\
                   threshold = None, distance=480)
                contour_heights = None

                forecastDf.loc[:, 'Peak'] =0;

                if (len(peaks_positive) > 0):
                    forecastDf.loc[peaks_positive, 'Peak'] = 1
                

   

        except Exception as e:
            print("peakSignal",e)
        finally:
           
            return forecastDf, contour_heights



    def findPeaks(self, oldestTimestamp, Area, isHrly, isIncremental, isoHelper):
        if (Area == 'ps') and isHrly == False:
            DataTbl = 'forecastTbl'
        elif (Area == 'ps') and isHrly == True:
            DataTbl = 'psHrlyForecstTbl'
        elif (Area == 'PJM RTO') and isHrly == False:
            DataTbl = 'rtoForecastTbl'
        elif (Area == 'PJM RTO') and isHrly == True:
            DataTbl = 'rtoHrlyForecstTbl'

        forecastDf = None

        try:
            connection = isoHelper.engine.connect()

            startTimeStamp, endTimeStamp = isoHelper.getStartEndForecastTimestamp(Area, isHrly)
            zeroTimeStamp = oldestTimestamp.replace(hour = 0, minute=0, second = 0, microsecond =0)

            startTimeStamp = zeroTimeStamp
            startTimeStr = startTimeStamp.strftime("%Y-%m-%dT%H:%M:%S")
            endTimeStr = endTimeStamp.strftime("%Y-%m-%dT%H:%M:%S")

            sql_query = "update " + DataTbl + " set Peak = 0" + " where timestamp >= CONVERT(DATETIME,'" + startTimeStr + \
                "')  and timestamp <= CONVERT(DATETIME,'" + endTimeStr + "')"
               
            result = connection.execute(sql_query)

            periodTimeStamp = startTimeStamp

            while (periodTimeStamp < endTimeStamp):

                periodTimeStamp = periodTimeStamp + timedelta(hours = 24)

                if (periodTimeStamp > endTimeStamp):
                    periodTimeStamp = endTimeStamp
            
                startTimeStr = startTimeStamp.strftime("%Y-%m-%dT%H:%M:%S")
                endTimeStr = periodTimeStamp.strftime("%Y-%m-%dT%H:%M:%S")

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

                peakDf = forecastDf[forecastDf['Peak'] > 0]

                if (len(peakDf) > 0):

                    if ((isHrly==True)):

                            peakDf.insert(len(peakDf.columns), 'Prominence',peakProminence)

                            sql_query = "SELECT [timestamp] ,[ForecstNumReads],[HrlyForecstLoad],[Peak],[EvaluatedAt],\
                                     [Area],[IsActive] ,[InitialTimestamp],[PeakStart],[PeakEnd], Overtime \
                                     FROM [ISODB].[dbo].[peakTable] where Area = '" + Area + "' \
                                    and  timestamp >= '" + startTimeStamp.strftime("%Y-%m-%dT%H:%M:%S") + "'"

                            prevPeakDf = pd.read_sql_query(sql_query, isoHelper.engine)    
                            prevPeakDf.reset_index(drop=True,inplace=True)
                            print("***********************findPeaks************")
                            print("peakDf = ", peakDf)
                            print("prevPeakDf = ", prevPeakDf)

                            dfMerge =pd.merge(peakDf,prevPeakDf,on=['timestamp','timestamp'],how="outer",indicator=True)
                        
                            inactivePeaks = dfMerge[dfMerge['_merge']=='right_only']
                            newPeaks = dfMerge[dfMerge['_merge']=='left_only']
                            bothPeaks =  dfMerge[dfMerge['_merge']=='both']
                            print ("inactivePeaks=", inactivePeaks)
                            print ("newPeaks=", newPeaks)
                            print ("bothPeaks=", bothPeaks)

                            for timestamp in inactivePeaks['timestamp']:
                                sql_query = "delete from  peakTable  where timestamp = '" + timestamp.strftime("%Y-%m-%dT%H:%M:%S") + "' and area = '" + Area + "'"
                                result = connection.execute(sql_query)

                            for timestamp in bothPeaks['timestamp']:
   
                                EvalAt = peakDf.loc[peakDf.timestamp == timestamp, 'EvaluatedAt']

                                sql_query = "update peakTable set EvaluatedAt = '" + EvalAt.iloc[0].strftime("%Y-%m-%dT%H:%M:%S") + \
                                    "' where timestamp = '" + timestamp.strftime("%Y-%m-%dT%H:%M:%S") + "' and area = '" + Area + "'"
                                result = connection.execute(sql_query)
                        
                            newPeaks.reset_index(drop=True,inplace=True)
                            peakDf.reset_index(drop=True,inplace=True)

                            if (len(newPeaks) > 0 and len(peakDf) >0):
                                newPeaks=peakDf.loc[newPeaks.timestamp == peakDf.timestamp]

                                newPeaks["Area"]=Area
                                newPeaks["PeakEnd"] = peakDf["timestamp"]
                                newPeaks["EvaluatedAt"] = peakDf["EvaluatedAt"] 
                                newPeaks["IsActive"] = True
                                newPeaks["Overtime"] = newPeaks["PeakEnd"] 
                                newPeaks["InitialTimestamp"] = newPeaks["EvaluatedAt"] 
                                newPeaks["Prominence"] = peakDf["Prominence"]

                                for timestamp in newPeaks['timestamp']:

                                    EvalAt = newPeaks.loc[newPeaks.timestamp == timestamp, 'EvaluatedAt']
                                    if (timestamp-timedelta (hours=1) > EvalAt.iloc[0] ):
                                        newPeaks.loc[newPeaks.timestamp == timestamp, 'PeakStart'] = timestamp-timedelta (hours=1)
                                    else:
                                        newPeaks.loc[newPeaks.timestamp == timestamp, 'PeakStart'] =  EvalAt.iloc[0]
                                
                                res = isoHelper.saveDf("peakTable", newPeaks)

                    isoHelper.replaceDf(DataTbl, forecastDf)
                     
 
                    if ((isHrly==True)):
                        forecastDf = self.CheckCPShaveHour(Area,startTimeStamp, periodTimeStamp, forecastDf, isoHelper)
                        isoHelper.replaceDf(DataTbl, forecastDf)
                        self.checkPeakEnd(  Area,  periodTimeStamp, isoHelper)

                startTimeStamp = periodTimeStamp

                
                newestEvaluatedAt = forecastDf['EvaluatedAt'].max()

                RefreshDataLst =  [[Area, isHrly, newestEvaluatedAt, zeroTimeStamp]]
                refreshDf = pd.DataFrame(RefreshDataLst, columns = ['Area', 'isHrly', 'NewEvalTime', 'StartDayTime'])
                

                isoHelper.saveDf('RefreshDataTbl', refreshDf)

        except BaseException as e:
            print("findPeaks",e)
        finally:

            connection.close()

            return forecastDf

    def CheckCPShaveHour(self, Area, startTimeStamp, endTimeStamp, forecastDf, isoHelper):

        try:
            divisor = forecastDf.ForecstNumReads
            

            MaxLoadDf = isoHelper.getMaxLoadforTimePeriod(endTimeStamp, Area)
            forecastDf.reset_index(drop =True, inplace=True)

            MaxForecastDf = forecastDf['Peak' == 1]
            if  max(MaxLoadDf['MaxLoad'])  < max(MaxForecastDf['HrlyForecstLoad']/divisor):
                forecastDf['Peak'==1].Peak = 2
        except BaseException as e:
            print("CheckCPShaveHour",e)
        finally:
            return forecastDf



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

    def checkDemandMgmtStart(self,  isoHelper):
   

        try:
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


        except BaseException as e:
            print("checkPeaks",e)
        finally:
            connection.close()
            return 

