'''
Using python's ElasticSearch client
https://elasticsearch-py.readthedocs.io/en/master/

'''
from elasticsearch import Elasticsearch
from datetime import datetime
import pprint

# connect to ES
es = Elasticsearch(['localhost:9200'], http_auth=('elastic', 'changeme'))

# requests
res = es.get(index="hashtag", doc_type="summary", id="AVyrX2apXrSoZACjSPbL")
pprint.pprint(res)