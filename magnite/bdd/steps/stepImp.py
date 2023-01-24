from behave import *



@given('The testdata setup for the tests is complete')
def step_impl(context):
	assert 1 == 1


@when("{testcases} are run using the automation")
def step_impl(context, testcases):
	"""
	:type context: behave.runner.Context
	:type testcases: str
	"""
	# raise NotImplementedError(u'STEP: When <testcases> are run using the automation')


@then("generate the {response} as response code")
def step_impl(context, response):
	"""
	:type context: behave.runner.Context
	:type response: str
	"""
	# raise NotImplementedError(u'STEP: Then generate the <response> as response code')


@then("the nurl sent back with the response is {NURL}")
def step_impl(context, NURL):
	"""
	:type context: behave.runner.Context
	:type NURL: str
	"""
	# raise NotImplementedError(u'STEP: And the nurl sent back with the response is <NURL>')