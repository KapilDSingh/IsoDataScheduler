       
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
  
         

            print(result.registers)

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
            print("Writing Registers:")
            print(registers)
            print("\n")
            payload = builder.build()

            # We can write registers
            rr =  modbusClient.write_registers(regAddress, registers, unit=1)
            assert not rr.isError()

        except BaseException as e:
                print("writeRegValue ",e)
                regValue=None
  
        finally:
            return  valType, regValue


    def chargeBatteries(self, modbusClient):

        try:


            temperatureCelsius= self.readRegValue(modbusClient, 157, 1, 'int16')

            temperatureFahrenheit = float(temperatureCelsius)  * 9 / 5 + 34

            print (temperatureFahrenheit)

            if (temperatureFahrenheit < 79):
                  self.writeRegValue(modbusClient, 1024, 'uint16' , 50)
            elif (temperatureFahrenheit < 82):
                  self.writeRegValue(modbusClient, 1024, 'uint16' , 10)
            elif (temperatureFahrenheit >= 82):
                  self.writeRegValue(modbusClient, 1024, 'uint16' , 0)


            batteryVoltage = self.readRegValue(modbusClient, 493, 1, 'int16')

            batteryVoltage = float(batteryVoltage)

            batteryVoltage = batteryVoltage / 340

            chargingCurrent = float(self.readRegValue(modbusClient, 492, 1, 'int16')) / 10

            newChgCurrent =( (14.3 - batteryVoltage) / (14 - 11) )* 20

            if (abs(newChgCurrent - chargingCurrent) > 0.2):
                self.writeRegValue(modbusClient, 1626, 'uint16' ,round (newChgCurrent * 10))



            #if (batteryVoltage  <= 13.3):
            #     self.writeRegValue(modbusClient, 1024, 'uint16' , 50)
            #     self.writeRegValue(modbusClient, 1626, 'uint16' , 100)
            #elif (batteryVoltage  <= 13.5):
            #    self.writeRegValue(modbusClient, 1024, 'uint16' , 50)
            #    self.writeRegValue(modbusClient, 1626, 'uint16' , 70)
            #elif (batteryVoltage  <= 13.8):
            #    self.writeRegValue(modbusClient, 1024, 'uint16' , 50)
            #    self.writeRegValue(modbusClient, 1626, 'uint16' , 50)
                 
            #elif (batteryVoltage  <= 14.0):
            #    self.writeRegValue(modbusClient, 1024, 'uint16' , 50)
            #    self.writeRegValue(modbusClient, 1626, 'uint16' , 30)
            #elif (batteryVoltage <= 14.3):
            #     self.writeRegValue(modbusClient, 1024, 'uint16' , 13)
            #else:
            #     self.writeRegValue(modbusClient, 1024, 'uint16' , 11)

            print (batteryVoltage)
        except BaseException as e:
                print("writeRegValue ",e)
                regValue=None
  
        finally:
            return  batteryVoltage, chargingCurrent

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

    def readRegValue(self, modbusClient, regAddress, size, valType, bitIdx = None):

        try:
            readRegValue = None
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
  
         

            print(result.registers)

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
            print("Writing Registers:")
            print(registers)
            print("\n")
            payload = builder.build()

            # We can write registers
            rr =  modbusClient.write_registers(regAddress, registers, unit=1)
            assert not rr.isError()

        except BaseException as e:
                print("writeRegValue ",e)
                regValue=None
  
        finally:
            return  valType, regValue


