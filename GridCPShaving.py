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
                prominenceHgt = 1000

            else:
                maxLoadHeight = 45000
                prominenceHgt = 3000

            if (isHrly == True):
                divisor = forecastDf.ForecstNumReads
                series = forecastDf.HrlyForecstLoad   / divisor
                # find all the peaks that associated with the positive peaks
                peaks_positive, _ = find_peaks(series, height = maxLoadHeight, prominence = prominenceHgt, \
                                               threshold = None, distance=20)
                ## find all the peaks that associated with the negative peaks
                #peaks_negative, _ = scipy.signal.find_peaks(-forecastDf.HrlyForecstLoad \
                #   / divisor, height = minLoadHeight, threshold = None, distance=10)
                prominences = peak_prominences(series, peaks_positive)[0]
            else:
                # find all the peaks that associated with the positive peaks
                peaks_positive, _ = scipy.signal.find_peaks(forecastDf.LoadForecast, height = maxLoadHeight,\
                   threshold = None, distance=240)

            forecastDf.loc[:, 'Peak'] =0;

            if (len(peaks_positive) > 0):
                forecastDf.loc[peaks_positive, 'Peak'] = 1
            
            #if (len(peaks_negative)>0):
            #    forecastDf.loc[peaks_negative, 'Peak'] = -1
            #plt.figure(figsize = (800, 1800))
            #contour_heights = series[peaks_positive] - prominences
            #plt.plot(series)
            #plt.plot(peaks_positive, series[peaks_positive], "x")
            #plt.vlines(x=peaks_positive, ymin=contour_heights, ymax=series[peaks_positive])
            #plt.show()
            #forecastDf.set_index('timestamp', inplace=True) 
            ##plt.figure(figsize = (80, 80))
            #plt.plot_date(forecastDf.index, forecastDf.HrlyForecstLoad  / forecastDf.ForecstNumReads, linewidth = 2)

            #plt.plot_date(forecastDf.index[peaks_positive], forecastDf.HrlyForecstLoad[peaks_positive] / forecastDf.ForecstNumReads[peaks_positive], 'ro', label = 'positive peaks')
            #plt.plot_date(forecastDf.index[peaks_negative], forecastDf.HrlyForecstLoad[peaks_negative] / forecastDf.ForecstNumReads[peaks_negative], 'go', label = 'negative peaks')
 
            #plt.xlabel('timestamp')
            #plt.ylabel('Load Forecast (MW)')
            ##plt.legend(loc = 4)
            #plt.show()

            #forecastDf.reset_index(inplace=True)
        except Exception as e:
            print("peakSignal",e)
        finally:
           
            return forecastDf



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

            startTimeStamp, endTimeStamp = isoHelper.getStartEndForecastTimestamp(Area, isHrly)

            if (isIncremental == True):
                startTimeStamp = endTimeStamp - timedelta(hours = 24)
            else:
                connection = isoHelper.engine.connect()

                result = connection.execute("update " + DataTbl + " set Peak = 0")

                connection.close()


            periodTimeStamp = startTimeStamp

            while (periodTimeStamp < endTimeStamp):

                periodTimeStamp = periodTimeStamp + timedelta(hours = 24*30)

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


                forecastDf = self.peakSignal(forecastDf, Area, isHrly)
 
                if (isIncremental == True):
                    forecastDf = forecastDf[forecastDf['timestamp'] >= oldestTimestamp]
                    
                    if (isHrly==True):
                        peakForecastDf = forecastDf[forecastDf['Peak'] > 0]
                        if (len(peakForecastDf) > 0):
                            peakForecastDf["Area"]=Area
                            peakForecastDf["StartTime"] = peakForecastDf["timestamp"] -timedelta (hours=1)
                            peakForecastDf["EndTime"] = peakForecastDf["timestamp"]

                            print(peakForecastDf)
                            isoHelper.saveDf("peakForecastTbl", peakForecastDf)


                if ((isHrly == True) and (len(forecastDf) > 0)):
                    forecastDf = self.CheckCPShaveHour(Area, forecastDf, isoHelper)

                if (len(forecastDf) > 0):
                    isoHelper.replaceDf(DataTbl, forecastDf)

                startTimeStamp = periodTimeStamp
 
        except BaseException as e:
            print("findPeaks",e)
        finally:
            return forecastDf

    def CheckCPShaveHour(self, Area, forecastDf, isoHelper):

        try:
            return forecastDf
            divisor = forecastDf.ForecstNumReads
            normalizedForecastDf= forecastDf.HrlyForecstLoad / divisor

            maxIdx = normalizedForecastDf.idxmax() 

            startTimeStamp = forecastDf['timestamp'].min()
            print('maxIdx = ', maxIdx,forecastDf)

            prevPkDf = isoHelper.getMaxLoadforTimePeriod(startTimeStamp, Area)
            if (prevPkDf.empty == True) :
                periodMaxLoad = 0
            else:
                numRows =  len(prevPkDf) 
           
                if (numRows > 0):
                     periodMaxLoad = prevPkDf.loc[numRows - 1, 'MaxLoad']
                else:
                    periodMaxLoad = 0

            maxForecastLoad = normalizedForecastDf[maxIdx]
            Peak = forecastDf.loc[maxIdx, 'Peak']

            if ((Peak == 1) and  (maxForecastLoad > periodMaxLoad)):
                 forecastDf.loc[maxIdx, 'Peak'] = 2

        except BaseException as e:
            print("CheckCPShaveHour",e)
        finally:
            return forecastDf



    def checkPeaks(self,  Area, isHrly, isoHelper):
        if (Area == 'ps') and isHrly == False:

            forecastTbl = 'forecastTbl'
            DataTbl = 'psInstLoadTbl'

        elif (Area == 'ps') and isHrly == True:

            forecastTbl = 'psHrlyForecstTbl'
            DataTbl = 'psHrlyLoadTbl'

        elif (Area == 'PJM RTO') and isHrly == False:

            forecastTbl = 'rtoForecastTbl'
            DataTbl = 'loadTbl'

        elif (Area == 'PJM RTO') and isHrly == True:

            forecastTbl = 'rtoHrlyForecstTbl'
            DataTbl = 'rtoHrlyLoadTbl'

        forecastDf = None

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

            print("Area isHrly",Area, isHrly, numIncorrectPeaks)

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

            print("Area isHrly",Area, isHrly, numIncorrectPeaks)

        except BaseException as e:
            print("checkPeaks",e)
        finally:
            connection.close()
            return 

