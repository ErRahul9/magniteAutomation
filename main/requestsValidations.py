import requests

# headers = {'Content-type': 'application/json'}
# url = "http://bidder.coredev.west2.steelhouse.com/magnite/bidder"
# vals = "MagniteAutomation/magnite/fixtures/bidRequest.txt"
# x = requests.post(url=url, data=vals, headers= headers)
# print(x.text)



v = ""
with open('/Users/rahulparashar/PycharmProjects/MagniteAutomation/magnite/fixtures/bidRequest.txt') as f:
    for line in f.readlines():
         v += line.replace('\n','').replace(' ','')
print(v)
headers = {'Content-type': 'application/json'}
url = "http://bidder.coredev.west2.steelhouse.com/magnite/bidder"
x = requests.post(url=url, data=v, headers= headers)
print(x)
print(x.status_code)
print(x.text)

# x = requests.get("https://steelhousesbx.api.beeswax.com/rest/list_item")
# print(x.status_code)
# # requests.get("https://steelhousesbx.api.beeswax.com/rest/list_item")
#
#
#
# requests.post("https://steelhousesbx.api.beeswax.com/rest/authenticate",data={
#     "email": "eng@steelhouse.com",
#     "password": "Zgg7ExPtRWGE243",
#     "keep_logged_in": true
# })






'''
requests.p

POST 
Body
{
    "email": "eng@steelhouse.com", 
    "password": "Zgg7ExPtRWGE243", 
    "keep_logged_in": true
}



'''