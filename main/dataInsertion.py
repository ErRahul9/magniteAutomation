import json
import os.path
import sys
from datetime import timedelta, datetime, time

from pyparsing import unicode

from config import globals
from enumerator import database
from magnite.main import enumerator
from magnite.main.connectors import *


class dataInsertion():
    def __init__(self):
        self.test = sys.argv[1]




    def readConf(self):
        retObj = {}
        fixtures = globals(self).get("fixtures")
        tests = globals(self).get("test")
        redisFile = os.path.join(fixtures,"redisDataSources.json")
        dataFile = os.path.join(fixtures,"data", "insertData.json")
        data = os.path.join(fixtures,"sample.json")
        driver = json.load(open(os.path.join(fixtures,"driver.json")))
        # caches = [data for data in driver.get(self.test).get("caches")]
        # print(caches)
        retObj["driver"] = driver
        retObj["redisFile"] = redisFile
        retObj["dataFile"] = dataFile
        return retObj




    def insertMetadataCache(self):
        metadata = json.load(open(self.readConf().get("redisFile")))
        jMeta = metadata.get("metadata").get("meta")
        insertType = metadata.get("metadata").get("type")
        driverFile = self.readConf().get("driver")
        testData = driverFile.get(self.test)
        createNewJsonObject  = {"mapping" :{}}
        # createNewJsonObject["mapping"] = jMeta.get("metadata")
        mapping = createNewJsonObject["mapping"]
        for keys in jMeta:
            if "threshold" not in keys:
                mapping[keys] = testData.get(keys)
            else:
                # mapping[keys] = testData.get("thresholds").get(keys)
                mapping[keys] = testData.get(keys)
        key = "crid_"+str(testData.get("creative_id"))
        cache = metadata.get("metadata").get("url")
        # cache = "core-dev-bidder-metadata.pid24g.clustercfg.usw2.cache.amazonaws.com"
        print(key ,createNewJsonObject ,cache,insertType)
        return key ,createNewJsonObject ,cache,insertType

    def insertBidderObject(self):
        metadata = json.load(open(self.readConf().get("redisFile")))
        meta = metadata.get("price")
        insertType = meta.get("type")
        getCPIData = meta.get("cpi")
        driverFile = self.readConf().get("driver")
        testData = driverFile.get(self.test)
        createNewJsonObject  = {"mapping":{}}
        mapping = createNewJsonObject["mapping"]
        mapping[str(testData.get("width"))+":"+str(testData.get("height"))+"_avg_cpi"] = getCPIData.get("avg_cpi")
        mapping[str(testData.get("width")) + ":" + str(testData.get("height")) + "_min_cpi"] = getCPIData.get("min_cpi")
        mapping[str(testData.get("width")) + ":" + str(testData.get("height")) + "_max_cpi"] = getCPIData.get("max_cpi")
        mapping["viewability_rate"] = getCPIData.get("viewability_rate")
        key = testData.get("companyURL")
        cache = metadata.get("price").get("url")
        # cache = "core-dev-bidder-price-optimize.pid24g.clustercfg.usw2.cache.amazonaws.com"
        # cache = "core-dev-bidder-price.pid24g.clustercfg.usw2.cache.amazonaws.com"
        print(key, createNewJsonObject, cache,insertType)
        return key,createNewJsonObject,cache,insertType



    def insertRecencyData(self):
        metadata = json.load(open(self.readConf().get("redisFile")))
        insertType = metadata.get("recency").get("type")
        jMeta = metadata.get("metadata").get("meta")
        driverFile = self.readConf().get("driver")
        testData = driverFile.get(self.test)
        createNewJsonObject = {"mapping": {}}
        dt = round(time.time()*1000) - 11*1000
        mapping = createNewJsonObject["mapping"]
        getChecks = testData.get("recency")
        for i in range(0,len(getChecks)):
            times =  getChecks[i]
            dt = int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds())*1000 - times *60000
            mapping[str(testData.get("advertiserId")+i)] = dt
        key = testData.get("ip")
        cache = "core-dev-recency.pid24g.clustercfg.usw2.cache.amazonaws.com"
        print(key, createNewJsonObject, cache,insertType)
        return key,createNewJsonObject,cache,insertType


    def insertMembershipData(self):
        metadata = json.load(open(self.readConf().get("redisFile")))
        insertType = metadata.get("members").get("type")
        driverFile = self.readConf().get("driver")
        testData = driverFile.get(self.test)
        value = []
        key = testData.get("ip")
        value.append(testData.get("segmentId"))
        cache = metadata.get("members").get("url")
        # cache = "core-dev-membership-opm.pid24g.clustercfg.usw2.cache.amazonaws.com"
        createNewJsonObject = {"mapping": {}}
        mapping = createNewJsonObject["mapping"]
        mapping[key] = [items for items in value]
        return key, createNewJsonObject, cache,insertType

    #
    # def insertSegmentAndIp(self):
    #     metadata = json.load(open(self.readConf().get("redisFile")))
    #     insertType = metadata.get("CampaignBySeg").get("type")
    #     driverFile = self.readConf().get("driver")
    #     testData = driverFile.get(self.test)
    #     key = testData.get("ip")
    #     value = testData.get("segmentId")
    #     cache = metadata.get("CampaignBySeg").get("url")
    #     # cache = "core-dev-rtb-dev-campaign-rep-group.pid24g.clustercfg.usw2.cache.amazonaws.com"
    #     createNewJsonObject = {"mapping": {}}
    #     mapping = createNewJsonObject["mapping"]
    #     mapping[key] = value
    #     return key, createNewJsonObject, cache, insertType


    def insertCampaignBySegment(self):
        metadata = json.load(open(self.readConf().get("redisFile")))
        insertType = metadata.get("CampaignBySeg").get("type")
        driverFile = self.readConf().get("driver")
        testData = driverFile.get(self.test)
        key = testData.get("segmentId")
        value = testData.get("campaign_id")
        cache = metadata.get("CampaignBySeg").get("url")
        # cache = "core-dev-rtb-dev-campaign-rep-group.pid24g.clustercfg.usw2.cache.amazonaws.com"
        createNewJsonObject = {"mapping": {}}
        mapping = createNewJsonObject["mapping"]
        mapping[key] = value
        return key, createNewJsonObject, cache, insertType

    def insertBudget(self):
        getDate = str(datetime.now()).split(" ")[0]
        getnextDate =  str(datetime.now() + timedelta(1)).split(" ")[0]
        metadata = json.load(open(self.readConf().get("redisFile")))
        insertType = metadata.get("budget").get("type")
        driverFile = self.readConf().get("driver")
        testData = driverFile.get(self.test)
        camp_id = testData.get("campaign_id")
        key = str(camp_id)+":"+getDate
        value = metadata.get("budget").get("values").get("budget1")
        cache = metadata.get("budget").get("url")
        # cache = "core-dev-rtb-dev-budget-rep-group.pid24g.clustercfg.usw2.cache.amazonaws.com"
        createNewJsonObject = {"mapping": {}}
        mapping = createNewJsonObject["mapping"]
        mapping[key] = value
        return key,createNewJsonObject,cache,insertType


    def insertApprovalData(self):
        metadata = json.load(open(self.readConf().get("redisFile")))
        insertType = metadata.get("approval").get("type")
        driverFile = self.readConf().get("driver")
        testData = driverFile.get(self.test)
        key = testData.get("companyURL")
        creatives = testData.get("creativeMetadata")
        createNewJsonObject = {"mapping": {}}
        cache = metadata.get("approval").get("url")
        # cache = "core-dev-rtb-dev-creative-approval-cache.pid24g.clustercfg.usw2.cache.amazonaws.com"
        mapping = createNewJsonObject["mapping"]
        mapping[key] = [data.get("creative_id") for data in creatives]
        return key,createNewJsonObject,cache,insertType


    def insertRecencyData(self):
        with open(self.metaPath) as meta:
            jMeta = json.load(meta)
        with open(self.jsonfile) as f:
            data = json.load(f)
            testData = data.get(self.test)
        createNewJsonObject = {"mapping": {}}
        dt = round(time.time()*1000) - 11*1000
        mapping = createNewJsonObject["mapping"]
        getChecks = testData.get("recency")
        for i in range(0,len(getChecks)):
            times =  getChecks[i]
            dt = int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds())*1000 - times *60000
            mapping[str(testData.get("advertiserId")+i)] = dt
        key = testData.get("ip")
        # cache =
        cache = "core-dev-recency.pid24g.clustercfg.usw2.cache.amazonaws.com"
        print(key, createNewJsonObject, cache)
        return key,createNewJsonObject,cache

    def insertHouseholdScore(self):
        with open(self.metaPath) as meta:
            jMeta = json.load(meta)
        with open(self.jsonfile) as f:
            data = json.load(f)
            testData = data.get(self.test)
        createNewJsonObject = {"mapping": {}}
        mapping = createNewJsonObject["mapping"]
        mapping["household_score"] = testData.get("scores").get("household_score")
        key = testData.get("ip")
        cache = "core-dev-household-score.pid24g.clustercfg.usw2.cache.amazonaws.com"
        print(key, createNewJsonObject, cache)
        return key,createNewJsonObject,cache

    def insertDataIntoTables(self):
        metadata = json.load(open(self.readConf().get("dataFile")))
        sqlstatement = ''
        for table in metadata.get("tables"):
            tablename = "bidder."+table
            keylist = "("
            valuelist = "("
            firstPair = True
            for key,value in metadata.get("tables").get(table).items():
                if not firstPair:
                    keylist += ", "
                    valuelist += ", "
                firstPair = False
                keylist += key
                if type(value) in (str, unicode):
                    valuelist += "'" + value + "'"
                else:
                    valuelist += str(value)
            keylist += ")"
            valuelist += ")"
            sqlstatement += "INSERT INTO " + tablename + " " + keylist + " VALUES " + valuelist + "\n"
        stmts = sqlstatement.split("\n")
        for inserts in stmts:
            if len(inserts) > 0:
                print(inserts)
                connectToPostgres("integration-dev.crvrygavls2u.us-west-2.rds.amazonaws.com", "qacore", "qa#core07#19", 5432,inserts)
        return stmts




    # def deletePostgresData(self):
    #     sql = self.insertDataIntoTables()
    #     for stmts in sql:
    #         if (len(stmts)> 0):
    #             print(stmts.split(" "))




if __name__ == '__main__':
    print(dataInsertion().insertDataIntoTables())
