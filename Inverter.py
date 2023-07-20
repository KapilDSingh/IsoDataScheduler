from inverterDataHelper import regDataHelper
import logging
from pymodbus.client import ModbusTcpClient
from pymodbus.constants import Defaults

Defaults.Timeout = 100


def connectInverter():
    
    #---------------------------------------------------------------------------# 
# configure the client logging
#---------------------------------------------------------------------------# 
    logging.basicConfig()
    log = logging.getLogger()
    log.setLevel(logging.INFO)

    connection = False
 
    modbusClient = ModbusTcpClient('4hunterstreet.dyndns.org', 502, timeout =30)
    while connection == False:
     connection = client.connect( )

    if (connection ):
        return modbusClient

        #roRegDf = regHelper.getReg(0,  "11word",  "InvRORegisters")

        #regValue =regHelper.updateRegValue(roRegDf.iloc[0].squeeze(),  "InvRORegisters")
        #roAllRegDf = regHelper.getAllReg("InvRORegisters")


        #updatedRORegTblDf = regHelper.updateRegTbl( "InvRORegisters", client)

        #updatedRWRegTblDf = regHelper.updateRegTbl("InvRWRegisters",client)

        #updatedRWRegTblDf = regHelper.updateRegTbl("InvCertificationDefault",client)

        #client.close()

    else:
            print("main: ", "Failed to connect")
            return None

