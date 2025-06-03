       
import urllib
from sqlalchemy.dialects.mssql import pyodbc
import json
import pandas as pd
from dateutil.parser import parse
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import sqlalchemy
import sys
from sqlalchemy import text
from pymodbus.constants import Endian
import logging
from pymodbus.client.sync import ModbusTcpClient
from pymodbus.constants import Defaults

from pymodbus.payload import BinaryPayloadDecoder
from dateutil.relativedelta import relativedelta
import socket
from sqlalchemy import update
from sqlalchemy.orm import Session
from IsodataHelpers import IsodataHelpers
from pymodbus.payload import BinaryPayloadBuilder
import time
import numpy as np

class regDataHelper(object):

    engine = create_engine('mssql+pyodbc://Kapil:Acfjo12#@ISODSN')

    def connectInverter(self):
    
        #---------------------------------------------------------------------------# 
        # configure the modbusClient logging
        #---------------------------------------------------------------------------# 
        logging.basicConfig()
        log = logging.getLogger()
        log.setLevel(logging.INFO)

        modbusConnection = False
 
        modbusClient = ModbusTcpClient('4hunterstreet.dyndns.org', 502, timeout =30)

        modbusConnection = modbusClient.connect( )

        if (modbusConnection ):
           return modbusClient
        else:
            return None

    def readRegValue(self, modbusClient, regAddress, size, valType, bitIdx = None):

        try:
            regValue = None
    
            result = modbusClient.read_holding_registers  (regAddress, size, unit=1)
            decoder = BinaryPayloadDecoder.fromRegisters(result.registers, byteorder=Endian.Big, wordorder=Endian.Big)

            if (valType == 'string'):

                regByteValue = decoder.decode_string(int(size))
                regValue = regByteValue.decode()

            elif (valType == 'uint16'):

                regStrValue =decoder.decode_16bit_uint()
                regValue = str(regStrValue)
     
            elif (valType == 'int16'):

                regValue = str(decoder.decode_16bit_int())
                regValue = str(regValue)

            elif (valType == 'uint64'):

                regValue = str(decoder.decode_64bit_uint())
                regValue = str(regValue)

            elif (valType == 'bitfield'):

                bits1Value = []
                bits2Value = decoder.decode_bits()
                bits1Value += decoder.decode_bits()
                if (bitIdx < 8):
                    regValue = str(bits1Value[bitIdx])
                else:
                    regValue = str(bits2Value[bitIdx - 8])

            else :
                regValue=None
                   
            #print("readRegValue, Register = ", regAddress, " Register Value = ", regValue)

        except BaseException as e:
                print("readRegValue ",e)
                return None
  
        finally:
            return  regValue

    def writeRegValue(self, modbusClient, regAddress, valType, regValue):

        try:

            builder = BinaryPayloadBuilder(byteorder=Endian.Big, wordorder=Endian.Big)
    
            if (valType == 'string'):

                builder.add_string(regValue)

            elif (valType == 'float'):
                builder.add_32bit_float(regValue)

            elif (valType == 'uint16'):

                builder.add_16bit_uint(regValue)

            elif (valType == 'int16'):

                builder.add_16bit_int(regValue)

            elif (valType == 'uint64'):

                builder.add_64bit_uint(regValue)

            elif (valType == 'bitfield'):

                builder.add_bits(regValue)
            else :
                regValue=None
  
            registers = builder.to_registers()
            
            payload = builder.build()

            # We can write registers
            rr =  modbusClient.write_registers(regAddress, registers, unit=1)
            assert not rr.isError()

            #print("writeRegValue, Register = ", regAddress, " Register Value = ", regValue)

        except BaseException as e:
                print("Exception writeRegValue, Register = ", regAddress, " Register Value = ", regValue)
                print("writeRegValue ",e)
                regValue=None
  
        finally:
            return  valType, regValue



    def peakShave(self, modbusClient,  loadKW, State_Watts_Dir, pid, graphics):
        try:
            batteryVoltage = None
            CurrentAmps = None
            newAmps = None
            supplyKW = None

            temperatureCelsius=  self.readRegValue(modbusClient, 157, 1, 'int16')

            if (temperatureCelsius == None):
                temperatureCelsius = 25

            temperatureFahrenheit = float(temperatureCelsius)  * 9 / 5 + 32

            if (temperatureFahrenheit < 86):
                 self.writeRegValue(modbusClient, 1024, 'int16' , -300)

            elif (temperatureFahrenheit < 87):
                  self.writeRegValue(modbusClient, 1024, 'int16' , -200)
            elif (temperatureFahrenheit >= 87):
                  self.writeRegValue(modbusClient, 1024, 'int16' , 0)
                  pid.ITermAccumulate = 0

            batteryVoltage = self.readRegValue(modbusClient, 493, 1, 'int16')
            batteryVoltage = float(batteryVoltage)
            batteryVoltage = batteryVoltage / 330

            CurrentAmps = float(self.readRegValue(modbusClient, 492, 1, 'int16')) / 10
  
            newAmps = pid(loadKW)

            self.writeRegValue(modbusClient, 1627, 'uint16' , int (newAmps * 10))
            p, i, d = pid.components 
            print (" DISCHARGING p= ",p, " i = ",i, " d = ",d)

            print ("DISCHARGING loadKW = ", loadKW," newAmps = ",  newAmps, "currentDirection =", State_Watts_Dir)
            
            elapsed_time = time.time() - graphics.StartTime

            newModelValues =np.array([[elapsed_time/60], [pid.setpoint], [loadKW], [newAmps] ])

            print ("newModelValues ", newModelValues)

            graphics.model = np.hstack(( graphics.model, newModelValues))

            graphics.updatePlot()

        except BaseException as e:
                print("peakShaving ",e)
  
        finally:
            return  loadKW, newAmps


    def chargeBatteries(self, modbusClient, pid, graphics):

        try:
            batteryVoltage = None
            newChgCurrent = None

            temperatureCelsius= self.readRegValue(modbusClient, 157, 1, 'int16')

            if (temperatureCelsius == None):
                temperatureCelsius = 25

            temperatureFahrenheit = float(temperatureCelsius)  * 9 / 5 + 32

            if (temperatureFahrenheit < 83):
                  self.writeRegValue(modbusClient, 1024, 'int16' , 100)
            elif (temperatureFahrenheit < 85):
                  self.writeRegValue(modbusClient, 1024, 'int16' , 50)
            elif (temperatureFahrenheit >= 85):
                  self.writeRegValue(modbusClient, 1024, 'int16' , 0)


            batteryVoltage = self.readRegValue(modbusClient, 493, 1, 'int16')
            batteryVoltage = float(batteryVoltage)
            batteryVoltage = batteryVoltage / 330

            newChgCurrent = pid (batteryVoltage)

            if (newChgCurrent < 0):
                    newChgCurrent=0

            self.writeRegValue(modbusClient, 1626, 'uint16' ,round (newChgCurrent * 10))
            p, i, d = pid.components 
            print ("CHARGING  p= ",p, " i = ",i, " d = ",d)

            print ("CHARGING batteryVoltage = ", batteryVoltage," newChgCurrent = ", newChgCurrent)
            
            elapsed_time = time.time() - graphics.StartTime

            newModelValues =np.array([[elapsed_time/60], [pid.setpoint], [batteryVoltage], [newChgCurrent] ])

            print ("newModelValues ", newModelValues)

            graphics.model = np.hstack(( graphics.model, newModelValues))

            graphics.updatePlot()

        except BaseException as e:
                print("chargeBatteries ",e)
  
        finally:
            return  batteryVoltage, newChgCurrent

    def updateDBRegValues(self,  modbusClient): 
        try:
            roRegDf = self.getReg(0,  "11word",  "InvRORegisters")

            regValue =self.updateRegValue(roRegDf.iloc[0].squeeze(),  "InvRORegisters", modbusClient)

            roAllRegDf = self.getAllReg("InvRORegisters")

            updatedRORegTblDf = self.updateRegTbl( "InvRORegisters", modbusClient)

            updatedRWRegTblDf = self.updateRegTbl("InvRWRegisters",modbusClient)

            updatedRWRegTblDf = self.updateRegTbl("InvCertificationDefault",modbusClient)

        except BaseException as e:
                print("updateDBRegValues ",e)
  
        finally:
            return

    def getReg(self, regAddress, size, DataTbl):

        try:
            dataConnection = self.engine.connect()

            if (DataTbl == "InvCertificationDefault") :
                    regTblQuery = text("SELECT * FROM dbo.InvCertificationDefault where Address =" + str(regAddress) + "' order by Address")
            else:
                regTblQuery = text("SELECT * FROM dbo." + DataTbl + " where Address =" + str(regAddress) + " and Size = '" + size + "' order by Address, Size")

            regDf = pd.read_sql_query(regTblQuery, dataConnection)

        except BaseException as e:
                print("getReg ",e)
  
        finally:
            dataConnection.close()
            return regDf

    def getAllReg(self, DataTbl):

            allRegDf = None
            try:
                dataConnection = self.engine.connect()

                if (DataTbl == "InvCertificationDefault") :
                     regTblQuery = "SELECT * FROM dbo.InvCertificationDefault  order by Address"
                else:
                    regTblQuery = "SELECT * FROM dbo." + DataTbl + " order by Address, Size"

                allRegDf = pd.read_sql_query(regTblQuery, dataConnection)

            except BaseException as e:
                 print("getAllReg ",e)
  
            finally:
               dataConnection.close()
               return allRegDf

    def updateRegValue(self,  regRow, DataTbl, modbusClient):

        try:

            regAddress = regRow["Address"]
                
            if (DataTbl != "InvCertificationDefault") :

                if ('R' not in regRow["Access"]):
                    return True

                valType =  regRow["Type"]
       
                if (isinstance(regRow["Size"], str)):
                    sizeText = regRow["Size"]

                    if ( 'word' in sizeText):
                        size = int(sizeText.replace('word', ''))
                        bitIdx = None
                    elif ('bit' in sizeText):
                        bitIdx =  int(sizeText.replace('bit', ''))
                        size = 1
                            
                else:
                    size = regRow["Size"]
                    sizeText = str(size)
                    bitIdx = None
            else:
                sizeText = str(1)
                size = 1
                bitIdx = None

                if (regAddress == 1365):
                    valType = 'int16'
                else:
                    valType = 'uint16'


            regValue = self.readRegValue(modbusClient, regAddress, size, valType, bitIdx ) 
            result = self.updateReg (regAddress, sizeText, regValue, DataTbl)

        except BaseException as e:
                print("updateRegValue ",e)


                return False
  
        finally:
            return True


    def updateReg(self, regAddress, size, Value, DataTbl):

        try:

            dataConnection = self.engine.connect()

            if (DataTbl != "InvCertificationDefault") :

                regUpdateQuery = text("Update " + DataTbl + " SET CurrentVal =  '" + Value + "' where Address = " + str(regAddress) + " and  Size ='" + size + "'")

            else:

                regUpdateQuery = text("Update " + DataTbl + " SET CurrentVal =  '" + Value + "' where Address = " + str(regAddress) )

            dataConnection.execute(regUpdateQuery)
            #dataConnection.commit()

        except BaseException as e:
                print("updateReg ",e)
                dataConnection.close()
                return False
  
        finally:
            dataConnection.close()
            return True

    def updateRegTbl(self, DataTbl, modbusClient):

            roAllRegDf = self.getAllReg(DataTbl)

            roAllRegDf.apply(self.updateRegValue, axis = 1, DataTbl = DataTbl, modbusClient =modbusClient)
            return roAllRegDf

 