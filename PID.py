import time
from simple_pid import PID as Pid

def initChargePid(inverterHelper, modbusClient):

        ChrgKp =1.2
        ChrgKi = 0.002
        ChrgKd = 0.05
        ChrgSetPoint = 14.0

        pidCharge = Pid(ChrgKp, ChrgKi, ChrgKd, setpoint = ChrgSetPoint)

        pidCharge.sample_time = 30
        pidCharge.output_limits = (0, 12)

        inverterHelper.writeRegValue(modbusClient, 1626, 'uint16' ,round (0))
        valType, regValue =  inverterHelper.writeRegValue(modbusClient, 1024, 'int16',0)

        return pidCharge

def initDisChargePid(inverterHelper, modbusClient):

        DisChrgKp = -1.2
        DisChrgKi = -0.002
        DisChrgKd = -0.05
        DisChrgSetPoint = 0

        pidDisCharge = Pid(DisChrgKp, DisChrgKi, DisChrgKd, setpoint = DisChrgSetPoint)

        pidDisCharge.sample_time = 30
        pidDisCharge.output_limits = (0,45)

        inverterHelper.writeRegValue(modbusClient, 1627, 'uint16' ,round (0))
        valType, regValue =  inverterHelper.writeRegValue(modbusClient, 1024, 'int16',0)


        return pidDisCharge
