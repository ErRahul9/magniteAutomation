from datetime import datetime , timedelta
import json
import os
from config import globals

import pytz


class datasetups():
    def __init__(self,test):
        self.test = test
        self.fixtures = globals(self).get("fixtures")
        self.driver = self.fixtures + "/driver.json"
        self.updatedJson = self.fixtures+"/updatedJson.json"


    def readDriver(self):
        with open(self.driver) as jsonFile:
            data = json.load(jsonFile)
            testData = data.get(self.test)
        # print(testData)
        # print(dataset)
        # testData = {}
        # for keys, values in dataset.items():
        #     if len(keys) != 0:
        #         testData[keys] = values

        tz = pytz.timezone(testData.get("timezone"))
        getTime = datetime.now(tz)
        # print(testData)
        for keys,values in testData.items():
            if "timeZoneCheck" in keys and len(testData["timeZoneCheck"]) > 0:
                timeDeltaStart = datasetups(self.test).currTime(getTime, values.get("startTimeDiff"))
                timeDeltaend = datasetups(self.test).currTime(getTime, values.get("endTimeDiff"))
                testData["startTime"] = str(timeDeltaStart).split(".")[0]
                testData["endTime"] = str(timeDeltaend).split(".")[0]
        with open(self.driver,"w+") as jsonFileUpdated:
            jsonObject = json.dumps(data)
            # json_formatted_str = json.dumps(jsonObject, indent=2)
            jsonFileUpdated.write(jsonObject)

    def currTime(self,time,delta):
        return time + timedelta(minutes=delta)


    # def pst(self):







if __name__ == '__main__':
    datasetups("MagniteBidRequestTimeZoneEST")
    # datasetups("MagniteBidRequestTimeZonePST").currTime("America/Los_Angeles")
    # datasetups("MagniteBidRequestTimeZoneEST").currTime("America/Chicago")
    datasetups("MagniteBidRequestTimeZoneEST").readDriver()
    # datasetups("MagniteBidRequestTimeZoneMST").readDriver()

