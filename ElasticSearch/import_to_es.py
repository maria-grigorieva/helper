from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConflictError
import pprint
import sys
from fileCatalogProcessing import FileCatalog

def main():
    # connect to ES
    es = Elasticsearch(['localhost:9200'], http_auth=('elastic', 'changeme'))
    es_index = "atlas_documents"
    es_doc_type = "cds_glance"
    catalog = FileCatalog()
    json_data = catalog.files2json()

    for item in json_data:
        es_doc_id = item.get('dkbID')
        print es_doc_id
        try:
            res = es.index(index=es_index, doc_type=es_doc_type, body=item, id=es_doc_id, op_type='create')
            pprint.pprint(res)
        except ConflictError as e:
            print('Document already exists')

if  __name__ =='__main__':
    main()

