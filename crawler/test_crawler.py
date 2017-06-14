#!/usr/bin/python
from BigpandaCrawler import BigpandaCrawler
from elasticsearch import Elasticsearch

def main():
    # by default we connect to localhost:9200 with user = elastic and password = changeme
    es = Elasticsearch(['localhost:9200'], http_auth=('elastic', 'changeme'))
    crawler = BigpandaCrawler()
    data = crawler.getData()
    print data
    # json_data = crawler.getJSON()
    # print json_data

    documents = crawler.docGenerator()

    for doc in documents:
        print doc
        res = es.index(index="hashtag", doc_type='summary', body=doc)
        print(res['created'])

    # curl -u elastic -p changeme -XGET 'localhost:9200/hashtag/_search?q=hashtag:Ttbar&pretty'

if  __name__ =='__main__':
    main()