from datetime import datetime, timedelta
import numpy as np
import matplotlib.pyplot as plt  
import time



class graphics(object):

    def __init__(self):


        self.hlOP = None
        self.hlSP = None
        self.hlPV = None
        self.pltFig = None

        self.axOP = None
        self.axPV = None
        self.zeroModel()

    def zeroModel(self):
        self.model = np.zeros((4,0))

    def updatePlot(self):
        
        x = self.model[0,:]
        new_y =  self.model[3,:]
        new_y2 =  self.model[2,:]

                       
        #plt.subplot(2,1,1)
	    # updating data values
        self.hlOP.set_xdata(x)
        self.hlOP.set_ydata(new_y)

        minVal = min(new_y)
        maxVal = max(new_y)

        plt.ylim(minVal,maxVal)
        plt.xlim(0, max(x))

        self.axOP.plot(x,new_y, 'b:',label= 'OP')

        self.hlPV.set_xdata(x)
        self.hlPV.set_ydata(new_y2)

        minVal = min(new_y2)
        maxVal = max(new_y2)
        plt.ylim(minVal,maxVal)

        self.axPV.plot(x,new_y2, 'r--', label= 'PV')


	    # drawing updated values
        self.pltFig.canvas.draw()

	    # This will run the GUI event
	    # loop until all UI events
	    # currently waiting have been processed
        self.pltFig.canvas.flush_events()

        time.sleep(0.1)

    def initPlot(self):

        # creating initial data values
        # of x and y
        x = np.linspace(0, 100, 100)
        y = np.sin(x)
        y2 = 10 * np.cos(x)

        # to run GUI event loop
        plt.ion()

        self.pltFig = plt.figure( figsize=(10, 8))

        self.axOP = plt.subplot(211)

        self.hlOP, = self.axOP.plot(x, y, 'b:',label= 'OP')

        plt.ylabel('Current (Amps)')
        plt.grid()

        plt.legend()

        self.axPV = plt.subplot(212, sharex=self.axOP)
 
 
        self.hlPV, = self.axPV.plot(x, y2, 'r--',label= 'PV')

        plt.ylabel('Net Power (KW)')
        plt.xlabel('Time (minutes)')
        plt.grid()
        plt.legend()




