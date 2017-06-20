from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConflictError
import pprint
import sys
from FilesProcessing import FileCatalog
from crawler.BigpandaCrawler import BigpandaCrawler
import argparse


def main():
    args = parsingArguments()
    global HOST
    HOST = args.host
    global PORT
    PORT = args.port
    if args.user:
        global USER
        USER = args.user
    if args.pwd:
        global PWD
        PWD = args.pwd
    if (args.user and args.pwd):
        es = Elasticsearch([HOST+':'+PORT], http_auth=(USER, PWD))
    else:
        es = Elasticsearch([HOST+':'+PORT])
    print es
    print es.cluster.health()

def create_index(index, es):
    es.indices.create(index=index, ignore=400)

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


def parsingArguments():
    parser = argparse.ArgumentParser(description='Process command line arguments.')
    parser.add_argument('--host', help='ElasticSearch host')
    parser.add_argument('--port', help='ElasticSearch port')
    parser.add_argument('--user', help='ElasticSearch user')
    parser.add_argument('--pwd', help='ElasticSearch password')
    return parser.parse_args()


if  __name__ =='__main__':
    main()