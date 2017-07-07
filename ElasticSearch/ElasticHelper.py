from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConflictError
import pprint
import sys
import json
from FilesProcessing import InputData
from crawler.BigpandaCrawler import BigpandaCrawler
import argparse
import ConfigParser
import datetime
from DocumentProcessing import FileConnector
import DButils

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
    print es.cluster.health()
    # handler = open('mc16_mapping.json')
    # mapping =  handler.read()
    # print mapping
    # removeIndex('mc16', es)
    # es.indices.create(index='mc16',
    #                   body=mapping)
    #
    # print es.indices.get_mapping(index='mc16')



    # Config = ConfigParser.ConfigParser()
    # Config.read("../settings.cfg")
    # dsn = Config.get("oracle", "dsn")
    # print dsn
    # conn, cursor = DButils.connectDEFT_DSN(dsn)
    #
    # handler = open('../SQLRequests/mc16_campaign.sql')
    # result = DButils.ResultIter(conn, handler.read()[:-1], 1, True)
    #
    curr_tstamp = datetime.datetime.now()
    #
    # for i in result:
    #     json_body = json.dumps(i, ensure_ascii=False)
    #     print json_body
    #     # es.index(index='mc16',
    #     #          doc_type='event_summary',
    #     #          id=i['TASKID'],
    #     #          body=json_body,
    #     #          timestamp=curr_tstamp)
    #     break
    #
    json_str = {"STATUS": "finished", "DATASETNAME": "mc16_13TeV:mc16_13TeV.423000.ParticleGun_single_electron_egammaET.simul.HITS.e3566_s3007_tid09742134_00", "DESCRIPTION": "E/gamma Single particles with 21.0.16", "HASHTAG_LIST": "ParticleGun, egamma, electron, mc16campaign, singleParticle", "TASKID": 10728431, "CAMPAIGN": "MC16", "PR_ID": 11031, "PROCESSED_EVENTS": 39895000, "TIMESTAMP": "05-07-17 11:25:19", "START_TIME": "17-02-17 03:23:35", "DATASET_STATUS": "ready", "PHYS_CATEGORY": "SingleParticle", "PROJECT": "mc16_13TeV", "STEP_NAME": "Reco", "PHYS_GROUP": "EGAM", "ENERGY_GEV": 13000, "TASKNAME": "mc16_13TeV.423000.ParticleGun_single_electron_egammaET.recon.e3566_s3007_r9117", "SUBCAMPAIGN": "MC16a", "REQUESTED_EVENTS": 39975000, "END_TIME": "22-02-17 18:55:00"}
    es.index(index='mc16',
             doc_type='event_summary',
             id=json_str['TASKID'],
             body=json_str,
             timestamp=curr_tstamp)

    #index_altas_documents("atlas-documents", "cds-glance", es, 1)

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

def index_altas_documents(index, doc_type, doc_id, es, limit):
    """
    Indexing ATLAS Documents metadata in the ElasticSearch
    :param index: name of ES index
    :param doc_type: document type of the ES index
    :param es: ElasticSearch instance
    :param limit: number of records to process 
    :return:
    """
    catalog = InputData('d', INPUT)

    # set current time for indexing operation
    curr_tstamp = datetime.datetime.now()

    for index, item in enumerate(catalog.filesGenerator()):
        try:
            res = es.index(
                index=index,
                doc_type=doc_type,
                body=item,
                id=item.get(doc_id),
                op_type="create",
                timestamp=curr_tstamp)
            pprint.pprint(res)
            if index == limit:
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

#
# def index_json_file(json_file, index, doc_type, es):
#


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

