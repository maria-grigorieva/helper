# import argparse1
#
# a = "xxx"
# b = []
# c = (1,0)
#
# print isinstance(a,dict)
# print isinstance(b,dict)
# print isinstance(c,dict)
#
# if not (isinstance(a,dict) and isinstance(b,dict) and isinstance(c,dict)):
#     print "FALSE"
#
# if type(a) != type(b) != type(c) != dict:
#     print "FALSE"


import re

str = "Hello world from the opposition"

g = re.search('world', str)
print g.group()

match = {}
hashtags_list = ["particlegun", "egamma", "electron", "mc16campaign", "singleparticle"]
values = ["singleparticle"]

for i in hashtags_list:
    if i in values:
        print "Yes!"

match['SingleParticle'] = len([x for x in hashtags_list if x in values])

print match

handler = open('ElasticSearch/output_0.json')
handler.close()

print handler.closed

