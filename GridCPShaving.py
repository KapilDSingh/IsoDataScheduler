import matplotlib.pyplot as plt
import pandas as pd
plt.style.use('seaborn-poster')
import scipy.signal
from pandas.plotting import register_matplotlib_converters

class GridCPShaving(object):
    """description of class"""

    def peakSignal(self, forecastDF, isPSEG):

        try:
            #register_matplotlib_converters()
            
            if (isPSEG):
                maxLoadHeight = 4500
                minLoadHeight = -4000
            else:
                maxLoadHeight = 11500
                minLoadHeight = -100000

            # find all the peaks that associated with the positive peaks
            peaks_positive, _ = scipy.signal.find_peaks(forecastDF.LoadForecast, height = maxLoadHeight, threshold = None, distance=500)

            # find all the peaks that associated with the negative peaks
            peaks_negative, _ = scipy.signal.find_peaks(-forecastDF.LoadForecast, height = minLoadHeight, threshold = None, distance=500)

            if (len(peaks_positive) > 0):
                forecastDF.Peak[peaks_positive] = 1
            
            if (len(peaks_negative)>0):
                forecastDF.Peak[peaks_negative] = -1


            #plt.figure(figsize = (14, 8))
            #plt.plot_date(forecastDF.index, forecastDF.LoadForecast, linewidth = 2)

            #plt.plot_date(forecastDF.index[peaks_positive], forecastDF.LoadForecast[peaks_positive], 'ro', label = 'positive peaks')
            #plt.plot_date(forecastDF.index[peaks_negative], forecastDF.LoadForecast[peaks_negative], 'go', label = 'negative peaks')
 
            #plt.xlabel('timestamp')
            #plt.ylabel('Load Forecast (MW)')
            #plt.legend(loc = 4)
            #plt.show()
        except Exception as e:
            print(e)
        finally:
           
            return forecastDF



