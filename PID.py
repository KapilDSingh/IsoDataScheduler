#!/usr/bin/python
#
# This file is part of IvPID.
# Copyright (C) 2015 Ivmech Mechatronics Ltd. <bilgi@ivmech.com>
#
# IvPID is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# IvPID is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# title           :PID.py
# description     :python pid controller
# author          :Caner Durmusoglu
# date            :20151218
# version         :0.1
# notes           :
# python_version  :2.7
# ==============================================================================

"""Ivmech PID Controller is simple implementation of a Proportional-Integral-Derivative (PID) Controller in the Python Programming Language.
More information about PID Controller: http://en.wikipedia.org/wiki/PID_controller
"""
import time
import numpy as np
import matplotlib.pyplot as plt  

class PID:
    """PID Controller
    """

    def __init__(self, P=0.2, I=0.0, D=0.0, current_time=None):

        self.Kp = P
        self.Ki = I
        self.Kd = D

        self.sample_time = 0.00
        self.current_time = current_time if current_time is not None else time.time()
        self.last_time = self.current_time
        self.start_time = self.current_time

        self.hlOP = None
        self.hlSP = None
        self.hlPV = None
        self.pltFig = None

        self.axOP = None
        self.axPV = None

        self.clear()

    def clear(self):

        """Clears PID computations and coefficients"""
        self.SetPoint = 0.0

        self.PTerm = 0.0
        self.ITermAccumulate = 0.0
        self.DTerm = 0.0
        self.ITerm = 0.0

        self.last_error = 0.0
        self.current_time = None
        self.last_time = None
        self.start_time = None

        # Windup Guard
        self.int_error = 0.0
        self.windup_guard = 8.0

        self.output = 0.0

        self.model = np.zeros((4,0))

        #self.pltFig =plt.figure()


    def update(self, feedback_value, current_time=None):
        """Calculates PID value for given reference feedback

        .. math::
            u(t) = K_p e(t) + K_i \int_{0}^{t} e(t)dt + K_d {de}/{dt}

        .. figure:: images/pid_1.png
           :align:   center

           Test PID with Kp=1.2, Ki=1, Kd=0.001 (test_pid.py)

        """
        error = self.SetPoint - feedback_value

        self.current_time = current_time if current_time is not None else time.time()

        if ( self.last_time is None):
             self.last_time = self.current_time

        if ( self.start_time is None):
             self.start_time = self.current_time

        delta_time = (self.current_time - self.last_time)/60
        delta_error = error - self.last_error

        #if (delta_time >= self.sample_time):
        self.PTerm = self.Kp * error
        self.ITermAccumulate += error * delta_time
        self.ITerm = self.Ki * self.ITermAccumulate

        if (self.ITerm < -74):
            self.ITerm = -74
        elif (self.ITerm > 8):
            self.ITerm = 8

        self.DTerm = 0.0
        if delta_time > 0:
            self.DTerm = self.Kd * delta_error / delta_time

        # Remember last time and last error for next calculation
        self.last_time = self.current_time
        self.last_error = error

        self.output = self.PTerm + self.ITerm + self.DTerm

        elapsed_time = self.current_time - self.start_time
        newModelValues =np.array([[elapsed_time/60], [self.SetPoint], [feedback_value], [self.output] ])

        #print ("newModelValues ", newModelValues, "delta_time = ", delta_time)

        self.model = np.hstack(( self.model, newModelValues))

    def setKp(self, proportional_gain):
        """Determines how aggressively the PID reacts to the current error with setting Proportional Gain"""
        self.Kp = proportional_gain

    def setKi(self, integral_gain):
        """Determines how aggressively the PID reacts to the current error with setting Integral Gain"""
        self.Ki = integral_gain

    def setKd(self, derivative_gain):
        """Determines how aggressively the PID reacts to the current error with setting Derivative Gain"""
        self.Kd = derivative_gain

    def setWindup(self, windup):
        """Integral windup, also known as integrator windup or reset windup,
        refers to the situation in a PID feedback controller where
        a large change in setpoint occurs (say a positive change)
        and the integral terms accumulates a significant error
        during the rise (windup), thus overshooting and continuing
        to increase as this accumulated error is unwound
        (offset by errors in the other direction).
        The specific problem is the excess overshooting.
        """
        self.windup_guard = windup

    def setSampleTime(self, sample_time):
        """PID that should be updated at a regular interval.
        Based on a pre-determined sampe time, the PID decides if it should compute or return immediately.
        """
        self.sample_time = sample_time

def readConfig ():
	global targetT
	with open ('/tmp/pid.conf', 'r') as f:
		config = f.readline().split(',')
		pid.SetPoint = float(config[0])
		targetT = pid.SetPoint
		pid.setKp (float(config[1]))
		pid.setKi (float(config[2]))
		pid.setKd (float(config[3]))

def createConfig ():
	if not os.path.isfile('/tmp/pid.conf'):
		with open ('/tmp/pid.conf', 'w') as f:
			f.write('%s,%s,%s,%s'%(targetT,P,I,D))

#createConfig()

#while 1:
#	readConfig()
#	#read temperature data
#	a0 = adc.read_voltage(0)
#	temperature = (a0 - 0.5) * 100

#	pid.update(temperature)
#	targetPwm = pid.output
#	targetPwm = max(min( int(targetPwm), 100 ),0)

#	print "Target: %.1f C | Current: %.1f C | PWM: %s %%"%(targetT, temperature, targetPwm)

#	# Set PWM expansion channel 0 to the target setting
#	pwmExp.setupDriver(0, targetPwm, 0)
#	time.sleep(0.5)