import os

def globals(self):
    # test = testCase
    returnObj ={}
    global ROOTDIR
    ROOTDIR = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
    global  fixtures
    fixtures = os.path.join(ROOTDIR, "fixtures")
    global  main
    main = os.path.join(ROOTDIR, "main")
    global resources
    resources = os.path.join(ROOTDIR, "resources")
    returnObj["ROOTDIR"] = ROOTDIR
    returnObj["fixtures"] = fixtures
    returnObj["main"] = main
    return returnObj



# if __name__=="__main()__":



