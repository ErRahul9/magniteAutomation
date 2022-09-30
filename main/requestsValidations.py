import json

import requests
from main import *


# headers = {'Content-type': 'application/json'}
# url = "http://bidder.coredev.west2.steelhouse.com/magnite/bidder"
# vals = "MagniteAutomation/magnite/fixtures/bidRequest.txt"
# x = requests.post(url=url, data=vals, headers= headers)
# print(x.text)


class runnerRunner():

    def __init__(self):

        self.keyword = sys.argv[1]
        self.testcases = []
        self.ROOTDIR = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
        self.fixPath = os.path.join(self.ROOTDIR, "{}/testCases.txt".format("fixtures"))
        self.testNames = open(self.fixPath, 'r')
        for tests in self.testNames:
            if self.keyword in tests:
                self.testcases.append(tests.strip())


    def APIRunner(self):
        fileLoc = os.path.join(self.ROOTDIR, "{}/{}".format("fixtures","processFiles"))
        files = os.listdir(fileLoc)
        v = ""
        # with open('/Users/rahulparashar/PycharmProjects/MagniteAutomation/magnite/fixtures/bidRequest.txt') as f:
        for fileNames in files:
            with open(os.path.join(fileLoc,fileNames)) as f:
                for line in f.readlines():
                    v += line.replace('\n', '').replace(' ', '')
            headers = {'Content-type': 'application/json'}
            url = "http://bidder.coredev.west2.steelhouse.com/magnite/bidder"
            x = requests.post(url=url, data=v, headers=headers)
            print(x)
            print(x.status_code)
            print(x.text)



    def settingUp(self):
        main(self.keyword).createListOfTestCases()
        fixPath = os.path.join(self.fixPath)
        testCases = open(fixPath)
        testcase = [test for test in testCases.read().split('\n')]
        if len(self.testcases) >= 1:
            test = [test for test in testcase if test in self.testcases]
            for tests in test:
                print("running test for {}".format(tests))
                data = dataInsertion(tests)
                main(tests).teardown(data)
                main(tests).teardownDbData()
                main(tests).setVariablesFromJsonFile()
                main(tests).updateDatafileFromDriver()
                dataInsertion(tests).insertDataIntoTables()
                main(tests).runDatainserts(data)

if __name__ == '__main__':

    runnerRunner().settingUp()

    runnerRunner().APIRunner()