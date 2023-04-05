import os
import sys
import unittest

from magnite.main.main import main


class MyTestCase(unittest.TestCase):

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

    def RunTests(self):





    def test_something(self):
        self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    unittest.main()
