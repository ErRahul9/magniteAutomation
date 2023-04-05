import json
import os.path
import sys
from datetime import timedelta, datetime
import time
from pyparsing import unicode
# from dotenv import load_dotenv
from config import globals
from enumerator import database
from magnite.main import enumerator
from connectors import *


class dataInsertion():
    def __init__(self,test):
        self.test = test

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
                mapping[keys] = testData.get(keys)
        key = "crid_"+str(testData.get("creative_id"))
        cache = metadata.get("metadata").get("url")
        print(key ,createNewJsonObject ,cache,insertType)
        return key ,createNewJsonObject ,cache,insertType

    def insertBidderObject(self):
        metadata = json.load(open(self.readConf().get("redisFile")))
        meta = metadata.get("price")
        insertType = meta.get("type")
        getCPIData = meta.get("cpi")
        driverFile = self.readConf().get("driver")
        testData = driverFile.get(self.test)
        viewability = 0
        perf = 0
        scores = testData.get("scores")
        for keys, values  in scores.items():
            if "viewability_score" in keys:
                viewability = values
            elif "performance" in keys:
                perf = values

        createNewJsonObject  = {"mapping":{}}
        mapping = createNewJsonObject["mapping"]
        mapping[str(testData.get("width"))+":"+str(testData.get("height"))+"_avg_cpi"] = getCPIData.get("avg_cpi")
        mapping[str(testData.get("width")) + ":" + str(testData.get("height")) + "_min_cpi"] = getCPIData.get("min_cpi")
        mapping[str(testData.get("width")) + ":" + str(testData.get("height")) + "_max_cpi"] = getCPIData.get("max_cpi")
        mapping[str(testData.get("width")) + ":" + str(testData.get("height")) + "_performance"] = perf
        mapping[str(testData.get("width")) + ":" + str(testData.get("height")) + "_viewability_rate"] = viewability
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
        getChecks = testData.get("scores").get("recency")
        for i in range(0,len(getChecks)):
            times =  getChecks[i]
            dt = int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds())*1000 - times *60000
            mapping[str(testData.get("advertiserId")+i)] = dt
        if "vast" in testData.get("recency_type"):
            key = testData.get("ip") + "_vast"
        else:
            key = testData.get("ip")
        # key = testData.get("ip")
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
        print("inserting membership data for {0} with segment {1}".format(key,value))
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

    def insertBudgetHourly(self):
        getDate = str(datetime.now()).split(" ")[0]
        metadata = json.load(open(self.readConf().get("redisFile")))
        insertType = metadata.get("budgetHourly").get("type")
        cache = metadata.get("budgetHourly").get("url")
        driverFile = self.readConf().get("driver")
        testData = driverFile.get(self.test)
        values = testData.get("scores")
        key = str(testData.get("campaign_id"))+":"+getDate+":day_parting"
        createNewJsonObject = {"mapping": {}}
        # mapping = createNewJsonObject["mapping"]
        createNewJsonObject["mapping"] = values
        # print(mapping)
        print(key)
        print(values)
        print(createNewJsonObject)
        return key, createNewJsonObject, cache, insertType
        # mapping[values] = values

    def insertApprovalData(self):
        metadata = json.load(open(self.readConf().get("redisFile")))
        insertType = metadata.get("approval").get("type")
        driverFile = self.readConf().get("driver")
        testData = driverFile.get(self.test)
        key = testData.get("companyURL")
        # creatives = testData.get("creativeMetadata")
        createNewJsonObject = {"mapping": {}}
        cache = metadata.get("approval").get("url")
        mapping = createNewJsonObject["mapping"]
        # mapping[key] = [data.get("creative_id") for data in creatives]
        mapping[key] = [testData.get("creative_id")]
        return key,createNewJsonObject,cache,insertType

    def insertBlockedGlobal(self):
        metadata = json.load(open(self.readConf().get("redisFile")))
        insertType = metadata.get("blockedGlobal").get("type")
        driverFile = self.readConf().get("driver")
        testData = driverFile.get(self.test)
        cache = metadata.get("blockedGlobal").get("url")
        key = str(testData.get("companyURL"))
        value = "Blocked_12"
        createNewJsonObject = {"mapping": {}}
        mapping = createNewJsonObject["mapping"]
        mapping[key] = value
        return key, createNewJsonObject, cache, insertType
        # 1021124_pub_block :"kom.googletest1.online"

    def insertBlockedCampaign(self):
        metadata = json.load(open(self.readConf().get("redisFile")))
        insertType = metadata.get("blockedCampaign").get("type")
        driverFile = self.readConf().get("driver")
        testData = driverFile.get(self.test)
        value = []
        cache = metadata.get("blockedCampaign").get("url")
        key = str(testData.get("campaign_id"))+"_pub_block"
        value.append(testData.get("companyURL"))
        createNewJsonObject = {"mapping": {}}
        mapping = createNewJsonObject["mapping"]
        mapping[key] = value
        return key, createNewJsonObject, cache, insertType

    def insertRecencyData(self):
        metadata = json.load(open(self.readConf().get("redisFile")))
        insertType = metadata.get("recency").get("type")
        driverFile = self.readConf().get("driver")
        testData = driverFile.get(self.test)
        createNewJsonObject = {"mapping": {}}
        dt = round(time.time()*1000) - 11*1000
        mapping = createNewJsonObject["mapping"]
        getChecks = testData.get("scores").get("recency")
        for i in range(0,len(getChecks)):
            times =  getChecks[i]
            dt = int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds())*1000 - times *60000
            mapping[str(testData.get("advertiser_id")+i)] = dt
        key = testData.get("ip")
        # cache =
        cache = "core-dev-recency.pid24g.clustercfg.usw2.cache.amazonaws.com"
        print(key, createNewJsonObject, cache)
        return key,createNewJsonObject,cache,insertType

    def insertHouseholdScore(self):
        metadata = json.load(open(self.readConf().get("redisFile")))
        insertType = metadata.get("household").get("type")
        driverFile = self.readConf().get("driver")
        testData = driverFile.get(self.test)
        cache = metadata.get("household").get("url")
        createNewJsonObject = {"mapping": {}}
        mapping = createNewJsonObject["mapping"]
        scoreMapping = testData.get("scores").get("household_score")
        mapping["household_score"] = scoreMapping
        key = testData.get("ip")
        print(key, createNewJsonObject, cache)
        return key,createNewJsonObject,cache,insertType



