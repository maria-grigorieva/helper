from elasticsearch import Elasticsearch
from elasticsearch import ElasticsearchException
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
import time
import re
import operator
import os
from datetime import datetime
from elasticsearch import helpers

def main():
    args = parsingArguments()
    if (args.type):
        global MODE
        MODE = args.type
    if (args.input):
        global INPUT
        INPUT = args.input
    if (args.output):
        global OUTPUT
        OUTPUT = args.output
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
    if (args.type):
        global SIZE
        SIZE = args.size
    if (args.user and args.pwd):
        es = Elasticsearch([HOST+':'+PORT], http_auth=(USER, PWD))
    else:
        es = Elasticsearch([HOST+':'+PORT])

    Config = ConfigParser.ConfigParser()
    Config.read("../settings.cfg")
    global dsn
    dsn = Config.get("oracle", "dsn")
    # query_body = '''
    #     {
    #         "aggs": {
    #             "category": {
    #               "terms": {"field": "phys_category.keyword"},
    #               "aggs": {
    #                     "step": {
    #                         "terms": {"field": "step_name.keyword"},
    #                         "aggs": {
    #                           "requested": {
    #                                 "sum": {"field": "requested_events"}
    #                             },
    #                             "processed": {
    #                                 "sum": {"field": "processed_events"}
    #                             }
    #                         }
    #                     }
    #                 }
    #             }
    #         }
    #     }
    # '''
    # es.indices.clear_cache(index='mc16')
    start = time.time()
    indexing(es_instance=es,
             index_name='prodsys_local',
             doc_type='MC16',
             sql_file='../OracleProdSys2/mc16.sql',
             mapping_file='mapping.json',
             keyfield='taskid')
    # bulk_insert(es, 'prodsys_local', 'MC16', 'mapping.json', '../OracleProdSys2/mc16.sql')
    end = time.time()
    print(end - start)
    # pprint.pprint(res)
    # print es.cluster.health()
    # print es.cluster.state()
    #pprint.pprint(es.indices.get_mapping(index='mc16'))


def createIndex(index_name, mapping, es_instance):
    try:
        es_instance.indices.create(index=index_name, body=mapping)
    except ElasticsearchException.IndexAlreadyExistsException as e:
        es_instance.indices.delete(index=index_name)
        es_instance.indices.create(index=index_name, body=mapping)

def bulk_insert(es_instance, index_name, doc_type, mapping_file, sql_file, pagesize=100):
    mapping = open(mapping_file).read()
    es_instance.indices.delete(index=index_name)
    es_instance.indices.create(index=index_name, body=mapping)
    # create_index(index_name, mapping, es_instance)
    # mapping_index = es_instance.indices.put_mapping(index=index_name, doc_type=doc_type, body=mapping)
    # if create_index["acknowledged"] != True or mapping_index["acknowledged"] != True:
    #     print "Index creation failed..."
    conn, cursor = DButils.connectDEFT_DSN(dsn)
    handler = open(sql_file)
    result = DButils.ResultIter(conn, handler.read()[:-1], pagesize, True)
    actions = []
    for row in result:
        row["phys_category"] = get_category(row)
        actions.append(
            {
                "_index": index_name,
                "_type": doc_type,
                "_id": row.get('taskid'),
                "_source": row,
            })
    helpers.bulk(es_instance, actions)


def oracle2json(sql_file, output, arraysize=500):
    conn, cursor = DButils.connectDEFT_DSN(dsn)

    sql_handler = open(sql_file)
    result = DButils.OneByOneIter(conn, sql_handler.read()[:-1], True)

    counter = -1
    if not os.path.exists(output):
        os.makedirs(output)
    json_handler = open('%s/%s_%d.json' % (output, output, 0), 'wb')
    result_arr = []

    for i in result:
        i["phys_category"] = get_category(i.get("hashtag_list"), i.get("taskname"))
        json_body = json.dumps(i, ensure_ascii=False)

        if (counter%arraysize == 0):
            json_handler = open('%s_%d.json' % (output, counter), 'wb')
            json_handler.write('[')
        result_arr.append(json_body)
        json_handler.write(json_body)
        counter += 1
        if (counter % arraysize != 0):
            json_handler.write(',')
            json_handler.write('\n')
        else:
            json_handler.write(']')
            json_handler.close()
    if not json_handler.closed:
        json_handler.seek(-1, os.SEEK_END)
        json_handler.truncate()
        json_handler.write(']')
        json_handler.close()

def indexing(es_instance, index_name, doc_type, sql_file, remove_old=True, mapping_file=None, keyfield=None):
    """
        sample usage:
        - auth mapping
        indexing(es_instance=es,
             index_name='mc16',
             doc_type='event_summary',
             sql_file='../SQLRequests/mc16_campaign.sql',
             mapping_file=None,
             keyfield='taskid')
        - custom mapping
        indexing(es_instance=es,
             index_name='mc16',
             doc_type='event_summary',
             sql_file='../SQLRequests/mc16_campaign.sql',
             mapping_file='mc16_mapping.json',
             keyfield='taskid')
    """
    # remove old index
    if remove_old:
        removeIndex(index_name, es_instance)
        # recreate index
        if mapping_file is not None:
            if isinstance(mapping_file, str):
                handler = open(mapping_file)
                mapping = handler.read()
                es_instance.indices.create(index=index_name,
                                           body=mapping)
        else:
            es_instance.indices.create(index=index_name)
    conn, cursor = DButils.connectDEFT_DSN(dsn)
    handler = open(sql_file)
    result = DButils.ResultIter(conn, handler.read()[:-1], 100, True)

    # set current timestamp
    curr_tstamp = datetime.now()
    id_counter = 0
    # file_handle = open("temp.txt", 'w')

    for i in result:
        i["phys_category"] = get_category(i)

        json_body = json.dumps(i, ensure_ascii=False)

        try:
            res = es_instance.index(index=index_name,
                           doc_type=doc_type,
                           id=i[keyfield] if keyfield is not None else id_counter+1,
                           body=json_body,
                           timestamp=curr_tstamp)
            pprint.pprint(res)
        except ElasticsearchException as e:
            print json_body
            print e
    # file_handle.close()

def get_category(row):
    """
    Each task can be associated with a number of Physics Categories.
    1) search category in hashtags list
    2) if not found in hashtags, then search category in phys_short field of tasknames
    :param row
    :return:
    """
    hashtags = row.get('hashtag_list')
    taskname = row.get('taskname')
    PHYS_CATEGORIES_MAP = {'BPhysics':['charmonium','jpsi','bs','bd','bminus','bplus','charm','bottom','bottomonium','b0'],
                            'BTag':['btagging'],
                            'Diboson':['diboson','zz', 'ww', 'wz', 'wwbb', 'wwll'],
                            'DrellYan':['drellyan', 'dy'],
                            'Exotic':['exotic', 'monojet', 'blackhole', 'technicolor', 'randallsundrum',
                            'wprime', 'zprime', 'magneticmonopole', 'extradimensions', 'warpeded',
                            'randallsundrum', 'contactinteraction','seesaw'],
                            'GammaJets':['photon', 'diphoton'],
                            'Higgs':['whiggs', 'zhiggs', 'mh125', 'higgs', 'vbf', 'smhiggs', 'bsmhiggs', 'chargedhiggs'],
                            'Minbias':['minbias'],
                            'Multijet':['dijet', 'multijet', 'qcd'],
                            'Performance':['performance'],
                            'SingleParticle':['singleparticle'],
                            'SingleTop':['singletop'],
                            'SUSY':['bino', 'susy', 'pmssm', 'leptosusy', 'rpv','mssm'],
                            'Triboson':['triplegaugecoupling', 'triboson', 'zzw', 'www'],
                            'TTbar':['ttbar'],
                            'TTbarX':['ttw','ttz','ttv','ttvv','4top','ttww'],
                            'Upgrade':['upgrad'],
                            'Wjets':['w'],
                            'Zjets':['z']}
    match = {}
    categories = []
    for phys_category in PHYS_CATEGORIES_MAP:
        current_map = [x.strip(' ').lower() for x in PHYS_CATEGORIES_MAP[phys_category]]
        match[phys_category] = len([x for x in hashtags.lower().split(',') if x.strip(' ') in current_map])
    categories = [cat for cat in match if match[cat] > 0]
    if len(categories) > 0:
        return categories
    else:
        phys_short = taskname.split('.')[2].lower()
        if re.search('singletop', phys_short) is not None: categories.append("SingleTop")
        if re.search('ttbar', phys_short) is not None: categories.append("TTbar")
        if re.search('jets', phys_short) is not None: categories.append("Multijet")
        if re.search('h125', phys_short) is not None: categories.append("Higgs")
        if re.search('ttbb', phys_short) is not None: categories.append("TTbarX")
        if re.search('ttgamma', phys_short) is not None: categories.append("TTbarX")
        if re.search('_tt_', phys_short) is not None: categories.append("TTbar")
        if re.search('upsilon', phys_short) is not None: categories.append("BPhysics")
        if re.search('tanb', phys_short) is not None: categories.append("SUSY")
        if re.search('4topci', phys_short) is not None: categories.append("Exotic")
        if re.search('xhh', phys_short) is not None: categories.append("Higgs")
        if re.search('3top', phys_short) is not None: categories.append("TTbarX")
        if re.search('_wt', phys_short) is not None: categories.append("SingleTop")
        if re.search('_wwbb', phys_short) is not None: categories.append("SingleTop")
        if re.search('_wenu_', phys_short) is not None: categories.append("Wjets")
        return categories
    return "Uncategorized"


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
    parser.add_argument('--output', help='Output directory')
    parser.add_argument('--size', help='number of lines, processed at a time')
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

