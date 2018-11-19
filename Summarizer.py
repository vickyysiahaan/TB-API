import paho.mqtt.client as mqtt
import json, time, random
import os, inspect, sys, signal
from time import strftime, localtime
import threading, traceback
import numpy as np
from multiprocessing import Process
from RegOID import Regs_Label,BitPos1_Label, BitPos2_Label, BitPos3_Label, BitPos4_Label
import DBLocal as DB1

Summ0 = {"CV": 0, "CCL": 0, "DCL": 0, "EDV": 0, "Status": 0, "Warning": 0, "Alarm": 0, "Error": 0, "Detected_Mod": 0, "Bus_Volt": 0, "Bus_Curr": 0, "Capacity": 0, "BackupTime": 0, "Timestamp": 0}
Mod0 = {"DC": 0, "FCC": 0, "RC": 0, "SOC": 0, "SOH": 0, "Cycle_Count": 0, "Voltage": 0, "Max_Cell_Voltage": 0, "Min_Cell_Voltage": 0, "Current": 0, "Max_Cell_Temp": 0, "Min_Cell_Temp": 0, "Max_FET_Temp": 0, "Max_PCB_Parts_Temp": 0, "Cell_Temp1": 0, "Cell_Temp2": 0, "Cell_Temp3": 0, "FET_Temp": 0, "PCB_Parts_Temp": 0, "C1V": 0, "C2V": 0, "C3V": 0, "C4V": 0, "C5V": 0, "C6V": 0, "C7V": 0, "C8V": 0, "C9V": 0, "C10V": 0, "C11V": 0, "C12V": 0, "C13V": 0, "Manufacture_Name": 0, "Device_Name": 0, "Serial_Number": 0, "BarCode": 0, "Status": 0, "Warning": 0, "Alarm": 0, "Error": 0, "Timestamp": 0}
Comm0 = {"InternalID": 0, "Type": 0, "InternalIPaddress": 0, "InternalNetmask": 0, "InternalGateway": 0, "snmpIPaddress": 0, "snmpNetmask": 0, "snmpGateway": 0, "snmpVersion": 0, "snmpCommunity": 0, "snmpPort": 0, "sysOID": 0, "MasterIPaddress": 0, "Active_Code": 0, "Timestamp": 0}

global Period
Period = [0,0,0,0,0,0,0,0,0,0]
t0 = [None,None,None,None,None,None,None,None,None,None]

#Connecting to Database
db1 = DB1.DataBase()

FolderPath = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

_FINISH = False

client = None

#Host address
HOST = 'localhost'

topic1 = "Comm/+"
topic2 = "Summary/+"
topic3 = "Mod/+/+"
topic4 = "Notification/+"

with open(FolderPath + '/json/Local/Comm.json') as json_data:
    Comm = json.load(json_data)

MasterID = Comm['InternalID']

DeviceIDs=[]
DetectedDevice = 0
OverallSummary = dict()

for i in range(1,11):
    command = """Notf%d = dict() """ %i
    exec(command)        

def Summarizer():
    global DeviceIDs
    global DetectedDevice
    global OverallSummary
    global _FINISH
    global Period
    
    for i in range(1,11):
        command = """global Notf%d""" %i
        exec(command)

    time.sleep(5)
    print("Summarizer is Running")
    
    isBeingDC = False
    tBegin = 0
    
    while(not(_FINISH)):
        stat = list()
        warn = list()
        alr = list()
        err = list()

        _stat = [[[],[],[]], [[],[],[]], [], [], [], [], [], [], [], [], []]
        _warn = [[], [], [], [], [], [], [], [], [], [], [], []]
        _alr = [[], [], [], [], [], [], [], [], [], [], [], [], [], []]
        _err = [[], [], [], [], [], [], [], [], [], [], [], [], [], [], []]
        
        BusVoltList = []
        BusCurrList = []
        CapacityList = []
        DetectedModuleList = []

        _DeviceIDs = []
        _DeviceIDs += DeviceIDs
        NoModDevices = []
        try:
            for i in range(1,11):
                if i in DeviceIDs:
                    Trial = 1
                    while(Trial<=5):
                        try:
                            with open(FolderPath + '/json/Global/Summary/Summary%d.json' %i) as json_data:
                                command = """data%i = json.load(json_data)"""%i
                                exec(command)
                            break
                        except:
                            Trial += 1
                            time.sleep(0.25)
                            
                    exec("""
global _DetMod
_DetMod = data%i['Detected_Mod']""" %i)

                    if _DetMod != 0:
                        command = """BusVoltList.append(data%d["Bus_Volt"])""" %i
                        exec(command)
                        command = """BusCurrList.append(data%d["Bus_Curr"])""" %i
                        exec(command)
                        command = """CapacityList.append(data%d["Capacity"])""" %i
                        exec(command)
                        command = """DetectedModuleList.append(data%d["Detected_Mod"])""" %i
                        exec(command)
                    
                        command = """stat.append(Notf%d["status"])""" %i
                        exec(command)
                        command = """warn.append(Notf%d['warning'])""" %i
                        exec(command)
                        command = """alr.append(Notf%d['alarm'])""" %i
                        exec(command)
                        command = """err.append(Notf%d['error'])""" %i
                        exec(command)
                    else:
                        del _DeviceIDs[_DeviceIDs.index(i)]
                        NoModDevices.append(i)
                        
            if len(BusVoltList)!= 0:
                AverageBusVolt = round(np.mean(BusVoltList),2)
                OverallSummary['Bus_Volt'] = AverageBusVolt
            else:
                OverallSummary['Bus_Volt'] = 0
            TotalBusCurr = round(np.sum(BusCurrList),2)
            OverallSummary['Bus_Curr'] = TotalBusCurr
            TotalCapacity = round(np.sum(CapacityList),2)
            OverallSummary['Capacity'] = TotalCapacity
            TotalDetectedModule = int(np.sum(DetectedModuleList))
            OverallSummary['Detected_Mod'] = TotalDetectedModule

            if OverallSummary['Bus_Curr'] < 0:
                (hour, minute) = divmod(OverallSummary['Capacity'], abs(OverallSummary['Bus_Curr']))
                minute = minute*60/abs(OverallSummary['Bus_Curr'])
                OverallSummary['BackupTime'] = '%d h %d m' %(hour,minute)
            else:
                OverallSummary['BackupTime'] = 'Unknown (not Discharged)'

            DetectedSlaveIDs = list(DeviceIDs)
            try:
                DetectedSlaveIDs.remove(MasterID)
                OverallSummary['ConnectedSlaves'] = DetectedDevice - 1 
            except:
                OverallSummary['ConnectedSlaves'] = DetectedDevice 
            
            if len(DetectedSlaveIDs) == 0:
                DetectedSlaveIDs = None
            OverallSummary['ConnectedSlaveIDs'] = DetectedSlaveIDs


            #status
            gen_stat = ''
            
            for r in range(0,len(stat)):
                Group = _DeviceIDs[r]
                for s in range(0,len(stat[r])):
                    if s == 0 or s == 1:
                        prot = [i+1 for i,x in enumerate(stat[r][s]) if x == 0]
                        if len(prot) != 0:
                            if len(prot) == DetectedModuleList[r]:
                                _stat[s][0].append('Group%d[All]'%Group)
                            else:
                                _stat[s][0].append('Group%d[' %Group + ','.join(str(e) for e in prot) + ']')
                        dis = [i+1 for i,x in enumerate(stat[r][s]) if x == 1]
                        if len(dis) != 0:
                            if len(dis) == DetectedModuleList[r]:
                                _stat[s][1].append('Group%d[All]'%Group)
                            else:
                                _stat[s][1].append('Group%d[' %Group + ','.join(str(e) for e in dis) + ']')
                        en = [i+1 for i,x in enumerate(stat[r][s]) if x == 2]
                        if len(en) != 0:
                            if len(en) == DetectedModuleList[r]:
                                _stat[s][2].append('Group%d[All]'%Group)
                            else:
                                _stat[s][2].append('Group%d[' %Group + ','.join(str(e) for e in en) + ']')
                                
                    else:
                        statMod = [i+1 for i,x in enumerate(stat[r][s]) if x == 1]
                        if len(statMod) != 0:
                            if len(statMod) == DetectedModuleList[r]:
                                _stat[s].append('Group%d[All]'%Group)
                            else:
                                _stat[s].append('Group%d[' %Group + ', '.join(str(e) for e in statMod) + ']')

            for t in range(0, len(_stat)):
                if t == 0 or t == 1:
                    if t == 0:
                        gen_stat += 'Charge_Operation_Mode: '
                    else:
                        gen_stat += 'Discharge_Operation_Mode: '

                    if len(_stat[t][0]) != 0:
                        gen_stat += 'Protection in ' + ', '.join(_stat[t][0]) + ','
                    if len(_stat[t][1]) != 0:
                        gen_stat += 'Disable in ' + ', '.join(_stat[t][1]) + ','
                    if len(_stat[t][2]) != 0:
                        gen_stat += 'Enable in ' + ', '.join(_stat[t][2]) + ','

                    gen_stat += '\n'
                else:
                    if len(_stat[t]) != 0:
                        gen_stat += BitPos1_Label[t] + ' in ' + ', '.join(_stat[t]) + '\n'
            
            if len(NoModDevices) != 0:
                gen_stat += 'No Battery is Detected in Group['+ ','.join(str(x) for x in NoModDevices) +']\n'
                
            OverallSummary['Status'] = gen_stat
            
            #warning
            gen_warn = ''
            for r in range(0,len(warn)):
                Group = _DeviceIDs[r]
                for s in range(0,len(warn[r])):    
                    warnMod = [i+1 for i,x in enumerate(warn[r][s]) if x == 1]
                    if len(warnMod) != 0:
                        if len(warnMod) == DetectedModuleList[r]:
                            _warn[s].append('Group%d[All]'%Group)
                        else:
                            _warn[s].append('Group%d[' %Group + ','.join(str(e) for e in warnMod) + ']')

            for t in range(0, len(_warn)):
                if len(_warn[t]) != 0:
                    gen_warn += BitPos2_Label[t] + ' in ' + ', '.join(_warn[t]) + '\n'

            if gen_warn == '':
                gen_warn = 'No Warning.'
            OverallSummary['Warning'] = gen_warn
            
            #alarm
            gen_alr = ''
            for r in range(0,len(alr)):
                Group = _DeviceIDs[r]
                for s in range(0,len(alr[r])):    
                    alrMod = [i+1 for i,x in enumerate(alr[r][s]) if x == 1]
                    if len(alrMod) != 0:
                        if len(alrMod) == DetectedModuleList[r]:
                            _alr[s].append('Group%d[All]'%Group)
                        else:
                            _alr[s].append('Group%d[' %Group + ','.join(str(e) for e in alrMod) + ']')

            for t in range(0, len(_alr)):
                if len(_alr[t]) != 0:
                    gen_alr += BitPos3_Label[t] + ' in ' + ', '.join(_alr[t]) + '\n'

            if gen_alr == '':
                gen_alr = 'No Alarm.'
            OverallSummary['Alarm'] = gen_alr
            
            #error
            gen_err = ''
            for r in range(0,len(err)):
                Group = _DeviceIDs[r]
                for s in range(0,len(err[r])):    
                    errMod = [i+1 for i,x in enumerate(err[r][s]) if x == 1]
                    if len(errMod) != 0:
                        if len(errMod) == DetectedModuleList[r]:
                            _err[s].append('Group%d[All]'%Group)
                        else:
                            _err[s].append('Group%d[' %Group + ','.join(str(e) for e in errMod) + ']')

            for t in range(0, len(_err)):
                if len(_err[t]) != 0:
                    gen_err += BitPos4_Label[t] + ' in ' + ', '.join(_err[t]) + '\n'

            if gen_err == '':
                gen_err = 'No Error.'
            OverallSummary['Error'] = gen_err

            OverallSummary['Timestamp'] = strftime("%Y-%m-%d %H:%M:%S", localtime())

            with open(FolderPath + '/json/Global/Summary/GlobalSummary.json', 'w') as file:
                file.write(json.dumps(OverallSummary))

            
            if OverallSummary['Bus_Curr'] < 0:
                if tBegin == 0:
                    BeginTimestamp = strftime("%Y-%m-%d %H:%M:%S", localtime())
                    tBegin = time.time()
                    isBeingDC = True
                    client.publish('Event', "Discharge Begin",1)
            else:
                if isBeingDC == True:
                    EndTimestamp = strftime("%Y-%m-%d %H:%M:%S", localtime())
                    tEnd = time.time()
                    Duration = round((tEnd-tBegin)/60,2) #in minute
                    isBeingDC = False
                    client.publish('Event', "Discharge End",1)
                    print(BeginTimestamp, EndTimestamp, Duration)
                    db1.InsertBackupEvent(BeginTimestamp, EndTimestamp, Duration)
                    print("BackupEvent Logged")
                    db1.commit()

            Refresher()
            if max(Period) > 15:
                time.sleep(15)
            elif max(Period) >= 5 and max(Period) <= 15:
                time.sleep(max(Period))
            else:
                time.sleep(5)
            
        except Exception as e:
            tb = traceback.format_exc()
            db1.InsertErrorLog('Summarizer', tb)
            db1.commit()
            #print(e)
            pass
            #raise
            
        
def Refresher():
    global DeviceIDs
    global DetectedDevice
    
    for i in range(1,11):
        command = """global Notf%d""" %i
        exec(command)

    #print("Refresher is Running")
    
    try:
        for i in range(1,11):
            if not(i in DeviceIDs):
                command = """Notf%d = dict()""" %i
                exec(command)
                    
                #Refresh Module Data
                for j in range(1,17):
                    Mod0['Timestamp']= strftime("%Y-%m-%d %H:%M:%S", localtime())
                    with open(FolderPath + '/json/Global/Mod/Mod[%d,%d].json' %(i,j), 'w') as file:
                        file.write(json.dumps(Mod0))

                #Refresh Summary Data
                Summ0['Timestamp']=strftime("%Y-%m-%d %H:%M:%S", localtime())
                with open(FolderPath + '/json/Global/Summary/Summary%d.json' %i, 'w') as file:
                    file.write(json.dumps(Summ0))

                #Refresh Comm Data
                Comm0['Timestamp']= strftime("%Y-%m-%d %H:%M:%S", localtime())
                    
                with open(FolderPath + '/json/Global/Comm/Comm%d.json' %i, 'w') as file:
                    file.write(json.dumps(Comm0))

        DetectedDevice=0
        DeviceIDs=[]
        #print("Refreshed")
        
    except Exception as e:
        tb = traceback.format_exc()
        db1.InsertErrorLog('Summarizer', tb)
        db1.commit()
        #print(e)
        #print("ID: ", i, ",     Module: ", j)
        pass

class ServiceExit(Exception):
    pass

def service_shutdown(signum, frame):
    print('Caught signal %d' % signum)
    raise ServiceExit
        
if __name__ == '__main__':
    # Register the signal handlers
    signal.signal(signal.SIGTERM, service_shutdown)
    signal.signal(signal.SIGINT, service_shutdown)
    
    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(client, userdata, rc, *extra_params):
        # Subscribing to receive RPC requests
        client.subscribe(topic1)
        client.subscribe(topic2)
        client.subscribe(topic3)
        client.subscribe(topic4)
        print('Topic subscribed!')

    def on_message(client, userdata, msg):    
        #print('Message received at Topic: ' + msg.topic)
        #print('Topic: ' + msg.topic + '\nMessage: ' + str(msg.payload))
        data = json.loads(msg.payload)
        name = msg.topic.split('/')

        global DeviceIDs
        global DetectedDevice
        ID = int(name[1])
        command = """global Notf%d""" %ID
        exec(command)
        
        if not(ID in DeviceIDs):
            DeviceIDs.append(ID)
            DeviceIDs.sort()
            DetectedDevice = len(DeviceIDs)
            
        if len(name) == 2:
            filename = name[0] + name[1] + '.json'
        elif len(name) == 3:
            filename = name[0] + '[' + name[1] + ',' + name[2] + '].json'

        with open(FolderPath + '/json/Global/' + name[0] + '/'+ filename, 'w') as file:
            file.write(json.dumps(data))

        if name[0] == "Notification":
            command = """Notf%d.update(data)""" %ID
            exec(command)

        if name[0] == "Summary":
            #Steady State
            try:
                t1 = time.time()
                Period[ID-1] = t1 - t0[ID-1]
                t0[ID-1] = t1

            #Initial State
            except:
                t0[ID-1] = time.time()
            
    client = mqtt.Client()
    # Register connect callback
    client.on_connect = on_connect
    # Registed publish message callback
    client.on_message = on_message
    client.connect(HOST, 1883, 60)
    
    try:
        p1 = threading.Thread(target=Summarizer, args=())
        p1.setDaemon(True)
        p1.start()
        client.loop_forever()
    except ServiceExit:
        _FINISH = True
        p1.join()
        tb = traceback.format_exc()
        db1.InsertErrorLog('Summarizer', tb)
        db1.commit()
        pass
    except:
        tb = traceback.format_exc()
        db1.InsertErrorLog('Summarizer', tb)
        db1.commit()

