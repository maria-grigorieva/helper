from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConflictError
import pprint
import sys
import json
from FilesProcessing import FileCatalog
from crawler.BigpandaCrawler import BigpandaCrawler
import argparse
import datetime

def main():
    args = parsingArguments()
    if (args.type):
        global MODE
        MODE = args.type
    if (args.input):
        global INPUT
        INPUT = args.input
    if (args.host):
        global HOST
        HOST = args.host
    if (args.port):
        global PORT
        PORT = args.port
    if (args.user and args.pwd):
        global USER
        USER = args.user
        global PWD
        PWD = args.pwd
    if (args.user and args.pwd):
        es = Elasticsearch([HOST+':'+PORT], http_auth=(USER, PWD))
    else:
        es = Elasticsearch([HOST+':'+PORT])

    # writeToFile('mapping.json', prettyJSON(es.indices.get_mapping("atlas-documents", "cds-glance"), 4))

    # es = Elasticsearch(['localhost:9200'], http_auth=('elastic', 'changeme'))
    #es.indices.delete("atlas-documents")
    #user_mapping = readJSONFromFile('mapping_.json', 'dkb-atlas-documents')
    #print user_mapping
    #es.indices.create(index="atlas-documents", body=user_mapping)
    # print prettyJSON(es.indices.get_mapping("atlas-documents"), 4)
    index_altas_documents("atlas-documents", "cds-glance", es, 1)
    # removeIndex("dkb-atlas-documents", es)
    #print json.dumps(es.indices.get_mapping("dkb-atlas-documents", "cds-glance"), indent=4, sort_keys=True)
    # index_monitor_summary("dkb-summary", "campaign", es)
    # removeIndex("dkb-atlas-documents", es)
    # removeIndex("atlas_documents", es)
    # print es.indices.exists(['atlas_documents'])

def prettyJSON(json_str, indent=4):
    return json.dumps(json_str, indent=indent, sort_keys=True)

def readJSONFromFile(filename, field):
    with open(filename, 'r') as file_content:
        data = json.load(file_content)
        try:
            return data[field]
        except KeyError as e:
            print "KeyError " + e.message

def writeToFile(filename, content):
    try:
        file_handle = open(filename, 'w')
    except IOError as e:
        print 'cannot open', filename
        raise
    file_handle.write(content)
    file_handle.close()


def parsingArguments():
    parser = argparse.ArgumentParser(description='Process command line arguments.')
    parser.add_argument('--host', help='ElasticSearch host')
    parser.add_argument('--port', help='ElasticSearch port')
    parser.add_argument('--user', help='ElasticSearch user')
    parser.add_argument('--pwd', help='ElasticSearch password')
    parser.add_argument('--type', help='type of input data: (file(f) | dir(d) | stream(s) | url(u))')
    parser.add_argument('--input', help='input data')
    # parser.add_argument('--o', help='output directory')
    return parser.parse_args()

def create_index(index, es):
    es.indices.create(index=index, ignore=400)

def index_altas_documents(index, doc_type, es, num):
    """
    Indexing ATLAS Documents metadata in the ElasticSearch
    :param index: name of ES index
    :param doc_type: document type of the ES index
    :param es: ElasticSearch instance
    :return:
    """
    catalog = FileCatalog(INPUT)
    json_data = catalog.getFileJSON()
    operation_timestamp = datetime.datetime.now()

    for index, item in enumerate(json_data):
        doc_id = item.get('dkbID')
        try:
            res = es.index(
                index=index,
                doc_type=doc_type,
                body=item,
                id=doc_id,
                op_type="create",
                timestamp=operation_timestamp)

            pprint.pprint(res)
            if index == num:
                break
        except ConflictError as e:
            print('Document already exists')

def index_monitor_summary(index, doc_type, es):
    """
    Indexing BigPanDA monitor summary from URL in the ElasticSearch
    :param index: name of ES index
    :param doc_type: document type of ES index
    :param es: ElasticSearch instance
    :return:
    """
    crawler = BigpandaCrawler(INPUT)
    data = crawler.getData()
    documents = crawler.docGenerator()
    operation_timestamp = datetime.datetime.now()

    for doc in documents:
        print doc
        try:
            res = es.index(index=index,
                           doc_type=doc_type,
                           body=doc,
                           timestamp=operation_timestamp)
        except ConflictError as e:
            print('Document already exists')


def removeIndex(index, es):
    try:
        res = es.indices.delete(index=index, ignore=[400, 404])
        print('Index ' + index + 'has beend removed from ElasticSearch')
    except:
        print('Index could not be deleted')


def putDocIntoIndex(doc, index, doc_type, es, doc_content):
    try:
        res = es.index(index=index, doc_type=doc_type, body=doc_content, op_type='create')
    except ConflictError as e:
        print('Document already exists')

if  __name__ =='__main__':
    main()

