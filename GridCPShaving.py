import matplotlib.pyplot as plt
import pandas as pd
plt.style.use('seaborn-poster')
import scipy.signal
from pandas.plotting import register_matplotlib_converters

class GridCPShaving(object):
    """description of class"""

    def peakSignal(self, forecastDF, isPSEG):

        try:
            register_matplotlib_converters()
            
            if (isPSEG):
                maxLoadHeight = 7500
                minLoadHeight = -5000
            else:
                maxLoadHeight = 115000
                minLoadHeight = -87000

            # find all the peaks that associated with the positive peaks
            peaks_positive, _ = scipy.signal.find_peaks(forecastDF.LoadForecast, height = maxLoadHeight, threshold = None, distance=30)

            # find all the peaks that associated with the negative peaks
            peaks_negative, _ = scipy.signal.find_peaks(-forecastDF.LoadForecast, height = minLoadHeight, threshold = None, distance=30)

            forecastDF.loc[:, 'Peak'] =0;
            if (len(peaks_positive) > 0):
                forecastDF.loc[peaks_positive, 'Peak'] = 1
            
            if (len(peaks_negative)>0):
                forecastDF.loc[peaks_negative, 'Peak'] = -1

            forecastDF.set_index('timestamp', inplace=True) 
            #plt.figure(figsize = (140, 80))
            #plt.plot_date(forecastDF.index, forecastDF.LoadForecast, linewidth = 2)

            #plt.plot_date(forecastDF.index[peaks_positive], forecastDF.LoadForecast[peaks_positive], 'ro', label = 'positive peaks')
            #plt.plot_date(forecastDF.index[peaks_negative], forecastDF.LoadForecast[peaks_negative], 'go', label = 'negative peaks')
 
            #plt.xlabel('timestamp')
            #plt.ylabel('Load Forecast (MW)')
            ##plt.legend(loc = 4)
            #plt.show()
        except Exception as e:
            print(e)
        finally:
           
            return forecastDF


    def findAllPeaks(self, isPSEG, isoHelper):

        if (isPSEG):
            DataTbl = "forecastTbl"
        else:
            DataTbl = "rtoForecastTbl"

         
        try:
            sql_query = "SELECT [timestamp] \
                        ,[EvaluatedAt]\
                        ,[Area] \
                        ,[LoadForecast] \
                        ,[Peak] \
                        FROM [ISODB].[dbo]." + DataTbl + " order by timestamp"

            forecastDf = pd.read_sql_query(sql_query, isoHelper.engine) 
            forecastDf.reset_index(drop=True,inplace=True)
            
            
            forecastDf = self.peakSignal(forecastDf, isPSEG)

            connection = isoHelper.engine.connect()

            result = connection.execute("delete from " + DataTbl)
            isoHelper.engine.connect().close()

            forecastDf.reset_index(inplace=True)
            forecastDf = forecastDf.sort_values('timestamp').drop_duplicates('timestamp',keep='last')
            ret=isoHelper.saveDf(DataTbl, Data= forecastDf)
 
        except BaseException as e:
            print(e)
        finally:
            return forecastDf


    def peakHrlySignal(self, forecastDF, isPSEG):

        try:
            register_matplotlib_converters()
            
            if (isPSEG):
                maxLoadHeight = 7500
                minLoadHeight = -5000
            else:
                maxLoadHeight = 115000
                minLoadHeight = -87000

            # find all the peaks that associated with the positive peaks
            peaks_positive, _ = scipy.signal.find_peaks(forecastDF.HrlyForecstLoad / forecastDF.ForecstNumReads, height = maxLoadHeight, threshold = None, distance=10)

            # find all the peaks that associated with the negative peaks
            peaks_negative, _ = scipy.signal.find_peaks(-forecastDF.HrlyForecstLoad / forecastDF.ForecstNumReads, height = minLoadHeight, threshold = None, distance=10)
            
            forecastDF.loc[:, 'Peak'] =0;

            if (len(peaks_positive) > 0):
                forecastDF.loc[peaks_positive, 'Peak'] = 1
            
            if (len(peaks_negative)>0):
                forecastDF.loc[peaks_negative, 'Peak'] = -1

            forecastDF.set_index('timestamp', inplace=True) 
            #plt.figure(figsize = (140, 80))
            #plt.plot_date(forecastDF.index, forecastDF.HrlyForecstLoad  / forecastDF.ForecstNumReads, linewidth = 2)

            #plt.plot_date(forecastDF.index[peaks_positive], forecastDF.HrlyForecstLoad[peaks_positive] / forecastDF.ForecstNumReads[peaks_positive], 'ro', label = 'positive peaks')
            #plt.plot_date(forecastDF.index[peaks_negative], forecastDF.HrlyForecstLoad[peaks_negative] / forecastDF.ForecstNumReads[peaks_negative], 'go', label = 'negative peaks')
 
            #plt.xlabel('timestamp')
            #plt.ylabel('Load Forecast (MW)')
            ##plt.legend(loc = 4)
            #plt.show()
        except Exception as e:
            print(e)
        finally:
           
            return forecastDF



    def findAllHrlyPeaks(self, isPSEG, isoHelper):

        if (isPSEG):
            DataTbl = "psHrlyForecstTbl"
        else:
            DataTbl = "rtoHrlyForecstTbl"

         
        try:
            sql_query = "SELECT  [timestamp] \
                        ,[ForecstNumReads]\
                        ,[HrlyForecstLoad]\
                        ,[Peak]\
                        FROM [ISODB].[dbo]." + DataTbl + " order by timestamp"

            forecastDf = pd.read_sql_query(sql_query, isoHelper.engine) 
            forecastDf.reset_index(drop=True,inplace=True)
            
            
            forecastDf = self.peakHrlySignal(forecastDf, isPSEG)

            connection = isoHelper.engine.connect()

            result = connection.execute("delete from " + DataTbl)
            isoHelper.engine.connect().close()

            forecastDf.reset_index(inplace=True)
            forecastDf = forecastDf.sort_values('timestamp').drop_duplicates('timestamp',keep='last')
            ret=isoHelper.saveDf(DataTbl, Data= forecastDf)
 
        except BaseException as e:
            print(e)
        finally:
            return forecastDf



