import fileinput
import json
import os
import re
import shutil
import sys
import enumerator
from config import globals
from magnite.main.connectors import *
from magnite.main.dataInsertion import dataInsertion
import requests

class main():

    def __init__(self,test):
        self.test = test
        self.fixtures = globals(self).get("fixtures")
        self.driver = self.fixtures+"/driver.json"
        self.enum = enumerator.bid
        self.sqlDataEnum = enumerator.database
        self.processFile = self.fixtures+"/bidRequest.txt"
        self.sqlFile = self.fixtures + "/data/insertData.json"
        self.driverFile = os.path.join(self.fixtures, "driver.json")
        self.redisFile = os.path.join(self.fixtures,"redisDataSources.json")
        self.cleanData = os.path.join(self.fixtures,"data","cleanData.txt")
        self.deleteData = os.path.join(self.fixtures, "data","deleteData.txt")
        self.inputLoc = os.path.join(self.fixtures,"processFiles/")

    def setVariablesFromJsonFile(self):
        with open(self.driver) as jsonFile:
            data = json.load(jsonFile)
            testData = data.get(self.test)
        for values in self.enum:
            regex = re.compile(r'\b' + str(values.value) + r'\b')
            for line in fileinput.input(self.processFile, inplace=1):
                if len(re.findall(regex, line)) > 0:
                    if "bundle" in line or "ip" in line:
                        line = line.replace(line,'"'+re.findall(regex, line)[0]+'"'+":"+'"'+str(testData.get(values.name))+'",')
                        sys.stdout.write(" " * 6 + line + '\n')
                    else:
                        line = line.replace(line,'"'+re.findall(regex, line)[0]+'"'+" : "+str(testData.get(values.name)) + ',')
                        sys.stdout.write(" " * 6 + line + '\n')
                else:
                    sys.stdout.write(line)
        newFileName = os.path.join(self.inputLoc,"bidrequest_{0}".format(self.test))
        shutil.copyfile(self.processFile,newFileName)

    def createListOfTestCases(self):
        jsonPath = os.path.join(self.fixtures, "driver.json")
        with open(jsonPath) as jFile:
            data = json.load(jFile)
            testPath = os.path.join(self.fixtures,"testCases.txt")
            with open(testPath,"w+") as f:
                for keys in data.keys():
                    if self.test in keys:
                        f.write(keys+'\n')
            f.close()
        jFile.close()
    def updateDatafileFromDriver(self):
        with open(self.driver) as jsonFile:
            data = json.load(jsonFile)
            testData = data.get(self.test)
        dictObj ={}
        for values in self.sqlDataEnum:
            keys = str(values).split(".")[1]
            vals = values.value
            dictObj[vals] =  testData.get(keys)
        # print(dictObj)
        with(open(self.sqlFile)) as sqlFile:
            sqldata = json.load(sqlFile)
        sqlDict ={}
        for data in sqldata.get("tables"):
            cols = sqldata.get("tables").get(data)
            sqlDict[data] = cols
        for keys in sqlDict:
            tabInfo = sqlDict.get(keys)
            for keys in tabInfo:
                tabInfo[keys] = dictObj.get(keys)
                cols = tabInfo
        jobj = json.dumps(sqldata)
        with open(self.sqlFile,'w') as jfile:
            jfile.write(jobj)
        deletes = []
        with open(self.deleteData,'r') as deleteFile:
            for lines in deleteFile:
                deleteQuery = lines.split("=")[0]
                for keys in dictObj:
                    if keys in lines:
                        deleteQuery = deleteQuery+" = "+str(dictObj.get(keys))
                deletes.append(deleteQuery)
        print(deletes)
        with open(self.cleanData,"w") as clean:
            for dels in deletes:
                clean.write(dels+'\n')
        clean.close()

    def readJson(self,fileName):
        jFile = json.load(open(fileName))
        if "redis" in fileName:
            data = jFile
        else:
            data = jFile.get(self.test)
        return data


    def runDatainserts(self,data):
        jsonData = self.readJson(self.driverFile)
        cacheData = self.readJson(self.redisFile)
        caches = jsonData.get("caches")
        returnObj = []
        for cache in caches:
            print("running inserts for {0}".format(cache))
            loaderDataInfo = cacheData.get(cache)
            method = getattr(data,loaderDataInfo.get("method"))
            methodCall = method()
            key = methodCall[0]
            metadata = methodCall[1]
            cacheName = methodCall[2]
            insertType = methodCall[3]
            retVal = connectToCache(cacheName, 6379, metadata.get("mapping"), key, "insert",insertType)
            returnObj.append(retVal)
            print("{0} record is inserted for {1}".format(retVal,cache))
        return returnObj

    def teardown(self, data):
        jsonData = self.readJson(self.driverFile)
        cacheData = self.readJson(self.redisFile)
        caches = jsonData.get("caches")
        cacheNames  = []
        retData = []
        for cache in caches:
            cacheNames.append(cacheData.get(cache).get("method"))
        print(cacheNames)
        for funcs in cacheNames:
            method = getattr(data, funcs)
            methodCall = method()
            key = "methodCall[0]"
            metadata = "methodCall[1]"
            insertType = "delete"
            cache = methodCall[2]
            retVal = connectToCache(cache, 6379, metadata, key, "delete",insertType)
            retData.append("deleted :  " + key[0] + "   :" + str(retVal))
        return retData

    def teardownDbData(self):
        with open(self.cleanData) as deletes:
            for lines in deletes:
                print("deleting {0}".format(lines))
                connectToPostgres("integration-dev.crvrygavls2u.us-west-2.rds.amazonaws.com","qacore","qa#core07#19",5432,lines)


# if __name__ == '__main__':
    # data = dataInsertion("MagniteBidRequestIpValidation")
    # main("MagniteBidRequestIpValidation").teardown(data)
    # main("MagniteBidRequestIpValidation").teardownDbData()
    # main("MagniteBidRequestIpValidation").setVariablesFromJsonFile()
    # main("MagniteBidRequestIpValidation").updateDatafileFromDriver()
    # dataInsertion("MagniteBidRequestIpValidation").insertDataIntoTables()
    # main("MagniteBidRequestIpValidation").runDatainserts(data)
    # main("MagniteBidRequestIpValidation").createListOfTestCases()
    # main().runnerRunner()