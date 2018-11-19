import sys, csv, requests, json, os, inspect, time, pprint
from datetime import datetime
from time import strftime, localtime
from openpyxl import Workbook
import argparse, ast, math, queue, threading

# Get Folder Path
FolderPath = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

# Create a pretty printer object
pp = pprint.PrettyPrinter(indent=4)

##### Constants #####
# Entity Types
TENANT = 'TENANT'
CUSTOMER = 'CUSTOMER'
USER = 'USER'
DASHBOARD = 'DASHBOARD'
ASSET = 'ASSET'
DEVICE = 'DEVICE'
ALARM = 'ALARM'

# Formats
XLSX = 'XLSX'
CSV = 'CSV'

#Aggregation modes
AVG = 'AVG'
MIN = 'MIN'
MAX = 'MAX'
NONE = 'NONE'
SUM = 'SUM'
COUNT = 'COUNT'

# Main Function
def main(args):
    try:
        mode = args.mode
        entity_type = args.entity_type
        entity_id = args.entity_id
        startTs = args.startTs
        endTs = args.endTs
        Interval = args.interval*1000
        isTelemetry = args.isTelemetry
        Limit = args.limit
        Agg = args.agg
        Format = args.format
        keyList = ast.literal_eval(str(args.keyList))
    except:
        pass
    
    if(mode == "getToken"):
        getToken()
    elif mode == "getKeyList":
        getKeyList(entity_type, entity_id,isTelemetry)
    elif mode == "getLatestValue":
        getLatestValue(entity_type, entity_id)
    elif mode == "exportLog":
        exportLog(entity_type, entity_id, keyList, startTs, endTs, Interval, isTelemetry, Limit, Agg, Format)
    else:
        raise ValueError("Unimplemented mode")
    
# Function to get JWT_Token
def getToken():
    url = 'http://35.202.49.101:8080/api/auth/login'
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    loginJSON = {'username': 'tekno@vioint.co.id', 'password': 'vio'}
    tokenAuthResp = requests.post(url, headers=headers, json=loginJSON).json()
    token = tokenAuthResp['token']
    
    #Return token in string format
    return token

# Function to Get All (Arrtibute/Telemetry) Variable Name in Device
def getKeyList(entity_type, entity_id, isTelemetry=True):
    # Args:
    # - entity_type   : DEVICE, ASSET, OR ETC
    # - entity_id     : ID of the entity
    # Return:
    # - KeyList          : List of variable name

    JWT_Token = getToken()
    if isTelemetry:
        url = 'http://35.202.49.101:8080/api/plugins/telemetry/%s/%s/keys/timeseries' %(entity_type,entity_id)
    else:
        url = 'http://35.202.49.101:8080/api/plugins/telemetry/%s/%s/keys/attributes' %(entity_type,entity_id)
    headers = {'Accept':'application/json', 'X-Authorization': "Bearer "+JWT_Token}
    KeyList = requests.get(url, headers=headers, json=None).json()
    
    return KeyList

# Function to Get Latest Variable Value in Device
def getLatestValue(entity_type, entity_id, isTelemetry=True):
    # Args:
    # - entity_type   : DEVICE, ASSET, OR ETC
    # - entity_id     : ID of the entity
    # Return:
    # - LatestValue   : Dictionary of variable names and their latest value

    JWT_Token = getToken()
    if isTelemetry:
        url = 'http://35.202.49.101:8080/api/plugins/telemetry/%s/%s/values/timeseries' %(entity_type,entity_id)
    else:
        url = 'http://35.202.49.101:8080/api/plugins/telemetry/%s/%s/values/attributes' %(entity_type,entity_id)
    headers = {'Accept':'application/json', 'X-Authorization': "Bearer "+JWT_Token}
    LatestValue = requests.get(url, headers=headers, json=None).json()
    
    return LatestValue

def LogQuery(entity_type, entity_id, keyList, startTs, endTs, Interval = 60, isTelemetry=True, limit=500, Agg=NONE):
    try:
        JWT_Token = getToken()

        if isTelemetry:
            url = 'http://35.202.49.101:8080/api/plugins/telemetry/%s/%s/values/timeseries?keys=' %(entity_type,entity_id)        
        else:
            url = 'http://35.202.49.101:8080/api/plugins/attributes/%s/%s/values/attributes?keys=' %(entity_type,entity_id)

        for i,key in enumerate(keyList):
            if i != len(keyList)-1:
                url += key + ','
            else:
                url += key + '&'

        url += 'startTs=%d&endTs=%d&interval=%d&' %(startTs, endTs, Interval)

        if limit != None:
            url += 'limit=%d&' %limit
        url += 'agg=%s' %Agg

        headers = {'Accept':'application/json', 'X-Authorization': "Bearer "+JWT_Token}
        Log_JSON = requests.get(url, headers=headers, json=None).json()
        #print(Log_JSON)

        if len(Log_JSON)!= 0:
            var = list(Log_JSON.keys())
            val = list(Log_JSON.values())

            #Separate Timestamp and Variable Value
            tsList= []
            valList= []
            for i,subval in enumerate(val):
                for j, item in enumerate(subval):
                    subval[j]= list(subval[j].values())
                val[i] = list(map(list, zip(*val[i])))
                tsList.append(val[i][0])
                valList.append(val[i][1])
            
            # Check if there is any difference in Timestamp lists
            for i,item in enumerate(tsList):
                if i == 0:
                    _tsList = item
                else:
                    diffItems = list(set(tsList[i])-set(tsList[i-1]))
                    if len(diffItems) != 0:
                        print("ada perbedaan ts")
                        for diffItem in diffItems:
                            index = tsList[i].index(diffItem)
                            for j in _tsList:
                                _tsList[j].insert(index,diffItem)

            # Convert Timestamp (in UNIX ms) to string of Year-Month-Date Hour-Min-Secs Format
            #for i,item in enumerate(_tsList):
                #_tsList[i] = datetime.fromtimestamp(item/1000).strftime("%Y-%m-%d %H:%M:%S")
                
            # Transpose Value Matrice
            Records = list(map(list, zip(*valList)))

            #Ubah tipe data
            for row in Records:
                for i,item in enumerate(row):
                    row[i]=ast.literal_eval(item)
                    try:
                        row[i]=round(row[i],3)
                    except:
                        pass
        else:
            tsList = []
            Records = []
            
        return [tsList,Records]
    except Exception as e:
        #raise
        #print(e)
        return -1

def LogCollecter(rowPart,colPart,ts,rec,*argv):
    Result = LogQuery(*argv)
    ts[rowPart][colPart]=Result[0]
    rec[rowPart][colPart]=Result[1]
    
# Function to Get Historical Value of Variables in Device in .csv or .xlsx format 
def exportLog(entity_type, entity_id, keyList, startTs, endTs, Interval = 60, isTelemetry=True, limit=500, Agg=NONE, Format=XLSX):
    # Args:
    # - entity_type     : DEVICE, ASSET, OR ETC
    # - entity_id       : ID of the entity
    # - keyList         : List of variable name
    # - startTs         : start timestamp
    # - endTs           : end timestamp
    # - Interval        : aggregation interval
    # - isTelemetry     : 1 for telemetry, 0 for attribute
    # - limit           : records limit
    # - Agg             : Aggregation mode
    # - Format          : Export file format
    # Return:
    # - Filename        : Filename with extension
    
    try:    
        t0 = time.time()

        #Key Partition (Column)
        totalKey = len(keyList)
        keys = []
        
        totalColPartition = math.ceil(totalKey/5)
        
        for i in range(0,totalColPartition):
            try:
                keys.append(keyList[i*5:(i*5)+5])
            except:
                keys.append(keyList[i*5:])
        print(keys)
        
        #Record Partition (Row)
        totalRowPartition = math.ceil((endTs-startTs)/604800000)
        tsRange = list(range(startTs,endTs,604800000))+[endTs]
        threads = [[None]*totalColPartition]*totalRowPartition

        _ts = [[None]*totalColPartition]*totalRowPartition
        _rec = [[None]*totalColPartition]*totalRowPartition
                    
        for i in range(0,totalRowPartition):
            for j in range(0,totalColPartition):
                t = threading.Thread(target=LogCollecter, args=[i, j, _ts, _rec, entity_type, entity_id, keys[j], tsRange[i], tsRange[i+1], Interval, isTelemetry, limit, Agg])
                t.setDaemon(True)
                t.start()
                threads[i][j]=t
        
        # Join all the threads
        for i in range(0,totalRowPartition):
            for j in range(0,totalColPartition):
                threads[i][j].join()

        t1 = time.time()
        print("query duration:", t1-t0, "s")

        _rec_ = []
        for item in _rec:
            _rec_.extend(item)
            
        # Check if there is any difference in Timestamp lists
        print(len(_ts), len(_ts[0]), len(_ts[1]), len(_ts[2]), len(_ts[3]))
        print()
        pp.pprint(_ts)
        #print(len(_ts_), len(_ts_[0]), len(_ts_[1]), len(_ts_[2]))

        _ts_ = []
        for i,subts in enumerate(_ts):
            _ts_.append([])
            for j,item in enumerate(subts):
                if j == 0:
                    _ts_[i]=item
                else:
                    diffItems = list(set(subts[j])-set(subts[j-1]))
                    if len(diffItems) != 0:
                        print("ada perbedaan ts")
                        #for 
                        
        for i,item in enumerate(_ts):
            if i == 0:
                ts = item
            else:
                #print(set(_ts_[i])-set(_ts_[i-1]))
                diffItems = list(set(_ts_[i])-set(_ts_[i-1]))
                if len(diffItems) != 0:
                    print("ada perbedaan ts")
                    for diffItem in diffItems:
                        index = _ts_[i].index(diffItem)
                        for j in ts:
                            ts[j].insert(index,diffItem)
        
        for i in range(0, len(_rec_[0])):
            for j in range(0, totalColPartition):
                if j == 0:
                    rec.append(_rec[j][i])
                else:
                    rec[i]+=_rec[j][i]
        
        # Add Timestamp to Records (This represents a row in Excel or CSV file)
        for i, item in enumerate(rec):
            rec[i].insert(0, ts[i])

        t2 = time.time()
        print("Processing duration:", t2-t1, "s")
        print("Total duration:", t2-t0, "s")
        
        #Export Data Log into CSV or XLSX format
        Filename = "DataLog_" + datetime.fromtimestamp(startTs/1000).strftime("%Y-%m-%d") + "_sd_" + datetime.fromtimestamp(endTs/1000).strftime("%Y-%m-%d")
        if Format == CSV:
            with open(FolderPath + '/ExportResult/%s.csv' %Filename, mode='w') as file:
                file_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                column = ['Timestamp']+ keyList
                file_writer.writerow(column)

                for i, item in enumerate(rec):
                    file_writer.writerow(item)

            return FolderPath + '/ExportResult/%s.csv' %Filename
        
        else:      
            wb = Workbook()

            # grab the active worksheet
            ws = wb.active

            column = ['Timestamp']+ list(Log_JSON.keys())
            ws.append(column)
            
            for i, item in enumerate(rec):
                ws.append(item)

            # Save the file
            wb.save(FolderPath + '/ExportResult/%s.xlsx' %Filename)

            return FolderPath + '/ExportResult/%s.xlsx' %Filename
        
    except Exception as e:
        #print(e)
        raise
        return -1
'''
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, help="Telemetry controller API", default=None)
    parser.add_argument("--entity_type", type=str, help="type of the entity", default=DEVICE)
    parser.add_argument("--entity_id", type=str, help="ID of the entity", default=None)
    parser.add_argument("--keyList", type=str, help="List of variable name", default=None)
    parser.add_argument("--startTs", type=int, help="Start Timestamp in UNIX miliseconds", default=None)
    parser.add_argument("--endTs", type=int, help="End Timestamp in UNIX miliseconds", default=None)
    parser.add_argument("--interval", type=int, help="Aggregation interval in seconds", default=1200)
    parser.add_argument("--isTelemetry", type=bool, help="1 for telemetry, 0 for attributes", default=1)
    parser.add_argument("--limit", type=int, help="Records Limit", default=500)
    parser.add_argument("--agg", type=str, help="Aggregation Mode", default=AVG)
    parser.add_argument("--format", type=str, help="Log Export File Format", default=XLSX)
    
    args = parser.parse_args(sys.argv[1:]);
    
    main(args);
'''
#pp.pprint(getKeyList('DEVICE', '25db7820-e302-11e8-8cdd-71469a7af993'))
#pp.pprint(getLatestValue('DEVICE', '25db7820-e302-11e8-8cdd-71469a7af993'))
#print(exportLog('DEVICE', 'f6bffe60-d1ba-11e8-87ee-4be867fcc47c',['I_1','I_2','I_3','V_1','V_2','V_3','E_Active','E_Reactive'],1541467800000, 1543541400000, 1200000, True, 500, AVG, CSV))
print(exportLog('DEVICE', 'f6bffe60-d1ba-11e8-87ee-4be867fcc47c',['I_1','I_2','I_3','V_1','V_2','V_3','V_12','V_23','V_31','PF_avg','Freq','E_Active','E_Reactive'],1541467800000, 1543541400000, 1200000, True, 500, AVG, CSV))
#exportLog('DEVICE', 'f6bffe60-d1ba-11e8-87ee-4be867fcc47c',['I_1','I_2'],1541467800000, 1543541400000, 1200000, True, 500, AVG, CSV)
