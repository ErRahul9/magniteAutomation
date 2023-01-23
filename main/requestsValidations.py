import datetime
import json
import subprocess
import time
from dotenv import load_dotenv



import requests

from dataInsertion import dataInsertion
from main import *
from dataload import process_reload


class runnerRunner():

    def __init__(self):

        self.keyword = sys.argv[1]
        self.load = sys.argv[2]
        self.testcases = []
        self.ROOTDIR = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
        self.fixPath = os.path.join(self.ROOTDIR, "{}/testCases.txt".format("fixtures"))
        self.testNames = open(self.fixPath, 'r')
        main(self.keyword).createListOfTestCases()
        for tests in self.testNames:
            if self.keyword in tests:
                self.testcases.append(tests.strip())

    def refreshSecurityToken(self):
        p = subprocess.Popen(['okta-awscli', '--profile', 'core', '--okta-profile', 'core'])
        print(p.communicate())

    def APIRunner(self):

        fileLoc = os.path.join(self.ROOTDIR, "{}/{}".format("fixtures", "processFiles"))
        url = os.environ["MAGNITE_URL"]
        headers = {'Content-type': 'application/json'}
        tests = open(self.fixPath)
        resultsFileName = "test_Results_{}".format(datetime.datetime.now())
        testResults = os.path.join(self.ROOTDIR, "fixtures/testResults/{}".format(resultsFileName))
        file = open(testResults, "w+")
        for test in tests:
            time.sleep(1)
            fileNames = "bidRequest_{0}".format(test.strip())
            with open(os.path.join(fileLoc, fileNames)) as f:
                print("running test for {0}".format(fileNames))
                v = ""
                for line in f.readlines():
                    v += line.replace('\n', '').replace(' ', '')
                x = requests.post(url=url, data=v, headers=headers)
                file.write(fileNames + " : " + str(x) + " : " + str(x.status_code) + " : " + str(x.text) + "\n")
                print(x)
                print(x.status_code)
                print(x.text)

    def settingUp(self):
        fixPath = os.path.join(self.fixPath)
        testCases = open(fixPath)
        testcase = [test for test in testCases.read().split('\n')]
        main("").teardown()
        if len(self.testcases) >= 1:
            test = [test for test in testcase if test in self.testcases]
            for tests in test:
                print("running test for {}".format(tests))
                data = dataInsertion(tests)
                # main(main).teardown()
                main(tests).setVariablesFromJsonFile()
                main(tests).updateDatafileFromDriver()
                main(tests).teardownDbData()
                main(tests).insertDataIntoTables()
                main(tests).runDatainserts(data)
        else:
            test = [test for test in testcase]
            for tests in test:
                if len(tests) > 0:
                    print("running test for {}".format(tests))
                    data = dataInsertion(tests)
                    main(tests).setVariablesFromJsonFile()
                    main(tests).updateDatafileFromDriver()
                    main(tests).teardownDbData()
                    main(tests).insertDataIntoTables()
                    main(tests).runDatainserts(data)


if __name__ == '__main__':
    runnerRunner().refreshSecurityToken()
    if "yes" in runnerRunner().load.lower():
        load_dotenv()
        runnerRunner().settingUp()
        process_reload()
        time.sleep(5)
    runnerRunner().APIRunner()
