Feature: Magnite End to end tests

  Scenario Outline: Validate Magnite End to End tests
    Given The testdata setup for the tests is complete
    When <testcases> are run using the automation
    Then generate the <response> as response code
    And the nurl sent back with the response is <NURL>


 Examples: PositiveValidations
   | testcases              | response | NURL            |
   | BidRequestIpValidation | 200      | www.testUrl.com |

