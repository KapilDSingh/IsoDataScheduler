import matplotlib.pyplot as plt
import pandas as pd
plt.style.use('seaborn-poster')
import scipy.signal
from pandas.plotting import register_matplotlib_converters
from datetime import datetime, timedelta

class GridCPShaving(object):
    """description of class"""

    def peakSignal(self, forecastDf, isPSEG, isHrly):

        try:
            #register_matplotlib_converters()
            
            if (isPSEG):
                maxLoadHeight = 1000
                minLoadHeight = -1000
            else:
                maxLoadHeight = 115000
                minLoadHeight = -87000

            if (isHrly == True):
                divisor = forecastDf.ForecstNumReads
                
                # find all the peaks that associated with the positive peaks
                peaks_positive, _ = scipy.signal.find_peaks(forecastDf.HrlyForecstLoad \
                    / divisor, height = maxLoadHeight, threshold = None, distance=10)

                # find all the peaks that associated with the negative peaks
                peaks_negative, _ = scipy.signal.find_peaks(-forecastDf.HrlyForecstLoad \
                   / divisor, height = minLoadHeight, threshold = None, distance=10)
          
            else:
                # find all the peaks that associated with the positive peaks
                peaks_positive, _ = scipy.signal.find_peaks(forecastDf.LoadForecast, height = maxLoadHeight,\
                   threshold = None, distance=30)

                # find all the peaks that associated with the negative peaks
                peaks_negative, _ = scipy.signal.find_peaks(-forecastDf.LoadForecast, height = minLoadHeight,\
                   threshold = None, distance=30)

            forecastDf.loc[:, 'Peak'] =0;

            if (len(peaks_positive) > 0):
                forecastDf.loc[peaks_positive, 'Peak'] = 1
            
            if (len(peaks_negative)>0):
                forecastDf.loc[peaks_negative, 'Peak'] = -1

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



    def findPeaks(self, oldestTimestamp, isPSEG, isHrly, isIncremental, isoHelper):
        if (isPSEG == True) and isHrly == False:
            DataTbl = 'forecastTbl'
        elif (isPSEG == True) and isHrly == True:
            DataTbl = 'psHrlyForecstTbl'
        elif (isPSEG == False) and isHrly == False:
            DataTbl = 'rtoForecastTbl'
        elif (isPSEG == False) and isHrly == True:
            DataTbl = 'rtoHrlyForecstTbl'


        forecastDf = None

        try:
            startTimeStamp, endTimeStamp = isoHelper.getStartEndForecastTimestamp(isPSEG, isHrly)

            if (isIncremental == True):
                startTimeStamp = endTimeStamp - timedelta(hours = 24)

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


                forecastDf = self.peakSignal(forecastDf, isPSEG, isHrly)
 
                if (isIncremental == True):
                    forecastDf = forecastDf[forecastDf['timestamp'] >= oldestTimestamp]

                if ((isHrly == True) and (len(forecastDf) > 0)):
                    forecastDf = self.CheckCPShaveHour(isPSEG, forecastDf, isoHelper)

                if (len(forecastDf) > 0):
                    isoHelper.replaceDf(DataTbl, forecastDf)

                startTimeStamp = periodTimeStamp
 
        except BaseException as e:
            print("findPeaks",e)
        finally:
            return forecastDf

    def CheckCPShaveHour(self, isPSEG, forecastDf, isoHelper):

        try:
            divisor = forecastDf.ForecstNumReads
            normalizedForecastDf= forecastDf.HrlyForecstLoad / divisor

            maxIdx = normalizedForecastDf.idxmax() 

            startTimeStamp = forecastDf['timestamp'].min()
            print('maxIdx = ', maxIdx,forecastDf)

            prevPkDf = isoHelper.getMaxLoadforTimePeriod(startTimeStamp, isPSEG)
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
