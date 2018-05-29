import sys
import csv
from datetime import datetime as dt

# fullLog = '../input/log20170630.csv'
# smallLog = '../input/sampleSet.csv'
# timeoutFile = '../input/inactivity_period.txt'
notInterestedIn = ['zone','code','size','idx','norefer','noagent','find','crawler','browser']
# outputFile = '../output/sessionization.txt'
epoch = dt(1970, 1, 1)
count = 1

logFile = sys.argv[1]
timeoutFile = sys.argv[2]
outputFile = sys.argv[3]

def readCsv(filename):
    global count
    csvDict = []
    with open(filename,"rb") as csvFile:
        csvData = csv.DictReader(csvFile)
        for row in csvData:
            for header in notInterestedIn:
                del row[header]
            row['index'] = count
            csvDict.append(row)
            count += 1
    return csvDict

def readTimeout(filename):
    theFile = open(filename, "r")
    theInts = []
    for val in theFile.read().split():
        theInts.append(int(val))
    theFile.close()
    return theInts

def getEpoch(date,time,combinedFlag):
    if not combinedFlag:
        datetime = date + " " + time
        return int((dt.strptime(datetime, "%Y-%m-%d %H:%M:%S") - epoch).total_seconds())
    else:
        return int((dt.strptime(date, "%Y-%m-%d %H:%M:%S") - epoch).total_seconds())

def writeFile(dataToWrite,path):
    with open(path, 'wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        for line in dataToWrite:
            writer.writerow([line['ip'],line['firstAccess'],line['lastAccess'],line['duration'],line['count']])

def processData(data):
    timeoutSeconds = readTimeout(timeoutFile)
    print timeoutSeconds
    lastIndex = {}
    outputData = []
    for line in data:
        #if not any(d['ip'] == line['ip'] and d['lastAccess'] == line['epoch'] for d in outputData):
        if lastIndex.get(line['ip']):
            foundIndex = lastIndex.get(line['ip'])
            #it exists in outputData
            #increment count, update last access time and increase session duration by the time difference
            for dicts in outputData:
                if dicts.get('index') == foundIndex:
                    if (getEpoch(line['date'],line['time'],0) - getEpoch(dicts['lastAccess'],0,1)) <= timeoutSeconds[0]:
                        print "time diff for " + line['ip'] + "= "
                        print getEpoch(line['date'],line['time'],0) - getEpoch(dicts['lastAccess'],0,1)
                        print timeoutSeconds[0]
                        dicts['lastAccess'] = line['date'] + " " + line['time']
                        dicts['count'] += 1
                        if (getEpoch(dicts['lastAccess'],0,1) - getEpoch(dicts['firstAccess'],0,1)) == 0:
                            dicts['duration'] = 1
                        else:    
                            dicts['duration'] = getEpoch(dicts['lastAccess'],0,1) - getEpoch(dicts['firstAccess'],0,1) + 1
                    else:
                        line['firstAccess'] = line['date'] + " " + line['time']
                        line['lastAccess'] = line['date'] + " " + line['time']
                        line['count'] = 1
                        line['duration'] = 1
                        outputData.append(line)
                        lastIndex.update({line['ip']: line['index']})        
        else:   
             #if it doesn't exist in outputData, create it
            line['firstAccess'] = line['date'] + " " + line['time']
            line['lastAccess'] = line['date'] + " " + line['time']
            line['count'] = 1
            line['duration'] = 1
            outputData.append(line)
            lastIndex.update({line['ip']: line['index']})        
    
    print lastIndex
    print outputData
    return outputData        


csvDict = readCsv(logFile)
data = processData(csvDict)
writeFile(data,outputFile)
