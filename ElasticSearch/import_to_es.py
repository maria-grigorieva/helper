from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConflictError
import pprint
import sys
from fileCatalogProcessing import FileCatalog
from crawler.BigpandaCrawler import BigpandaCrawler

def main():
    es = Elasticsearch(['localhost:9200'], http_auth=('elastic', 'changeme'))
    index_altas_documents("dkb-atlas-documents", "cds-glance", es)
    index_monitor_summary("dkb-summary", "campaign", es)
    #removeIndex("hashtag", es)
    #removeIndex("atlas_documents", es)

def index_altas_documents(index, doc_type, es):
    catalog = FileCatalog()
    json_data = catalog.files2json()
    for item in json_data:
        doc_id = item.get('dkbID')
        try:
            res = es.index(index=index, doc_type=doc_type, body=item, id=doc_id, op_type='create')
            # pprint.pprint(res)
        except ConflictError as e:
            print('Document already exists')

def index_monitor_summary(index, doc_type, es):
    crawler = BigpandaCrawler()
    data = crawler.getData()
    documents = crawler.docGenerator()

    for doc in documents:
        print doc
        try:
            res = es.index(index=index, doc_type=doc_type, body=doc)
        except ConflictError as e:
            print('Document already exists')


def removeIndex(index, es):
    try:
        res = es.indices.delete(index=index, ignore=[400, 404])
    except:
        print('Index could not be deleted')


def putDocIntoIndex(doc, index, doc_type, es, doc_content):
    try:
        res = es.index(index=index, doc_type=doc_type, body=doc_content, op_type='create')
    except ConflictError as e:
        print('Document already exists')

if  __name__ =='__main__':
    main()

