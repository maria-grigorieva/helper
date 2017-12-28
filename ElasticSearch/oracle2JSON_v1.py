#!/bin/env python
import json
import argparse
import ConfigParser
import re
import DButils
import sys

try:
    import cx_Oracle
except:
    print "****ERROR : DButils. Cannot import cx_Oracle"
    pass

def connectDEFT_DSN(dsn):
    connect = cx_Oracle.connect(dsn)
    cursor = connect.cursor()

    return connect, cursor

def main():
    """
    --input <SQL file> --size <page size> --config <config file> [--offset <datetime>]
    :return:
    """
    args = parsingArguments()
    if (args.input):
        input = args.input
    if (args.size):
        size = args.size
    else:
        size = 100
    Config = ConfigParser.ConfigParser()
    try:
        Config.read(args.config)
    except IOError:
        sys.stderr.write('Could not read config file %s' % args.config)

    global dsn
    dsn = Config.get("oracle", "dsn")
    conn, cursor = connectDEFT_DSN(dsn)

    file_handler = open("prodsys2ES.json", "w")

    try:
        sql_handler = open(input)
        query = sql_handler.read().rstrip().rstrip(';') % ("01-01-2016 00:00:00", "02-01-2016 00:00:00")
        print query
       # query = query_sub(query, args)
        result = DButils.ResultIter(conn, query, size, True)
        json_arr = []
        for row in result:
            json_arr.append(json.dumps(row))
        json_string = '[' + ','.join(json_arr) + ']'
        file_handler.write(json_string);
            # row['phys_category'] = get_category(row)
            # file_handler.write(json.dumps(row) + '\n')
            # sys.stdout.write(json.dumps(row) + '\n')

    except IOError:
        sys.stderr.write('File open error. No such file.')

    try:
        sql_handler = open(input)
        query = sql_handler.read().rstrip().rstrip(';') % ("01-01-2016 00:00:00", "02-01-2016 00:00:00")
        print query
       # query = query_sub(query, args)
        result = DButils.ResultIter(conn, query, size, True)
        json_arr = []
        for row in result:
            json_arr.append(json.dumps(row))
        json_string = '[' + ','.join(json_arr) + ']'
        file_handler.write(json_string);
            # row['phys_category'] = get_category(row)
            # file_handler.write(json.dumps(row) + '\n')
            # sys.stdout.write(json.dumps(row) + '\n')

    except IOError:
        sys.stderr.write('File open error. No such file.')

def query_sub(query, args):
    """ Substitute %%VARIABLE%% in query with args.variable. """
    query_var = re.compile('%%[A-Z_0-9]*%%')
    return re.sub(
        query_var,
        lambda m: vars(args).get(m.group(0).strip('%').lower(), m.group(0)),
        query
    )

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
    PHYS_CATEGORIES_MAP = {
        'BPhysics': ['charmonium', 'jpsi', 'bs', 'bd', 'bminus', 'bplus',
                     'charm', 'bottom', 'bottomonium', 'b0'],
        'BTag': ['btagging'],
        'Diboson': ['diboson', 'zz', 'ww', 'wz', 'wwbb', 'wwll'],
        'DrellYan': ['drellyan', 'dy'],
        'Exotic': ['exotic', 'monojet', 'blackhole', 'technicolor',
                   'randallsundrum', 'wprime', 'zprime', 'magneticmonopole',
                   'extradimensions', 'warpeded', 'randallsundrum',
                   'contactinteraction', 'seesaw'],
        'GammaJets': ['photon', 'diphoton'],
        'Higgs': ['whiggs', 'zhiggs', 'mh125', 'higgs', 'vbf', 'smhiggs',
                  'bsmhiggs', 'chargedhiggs'],
        'Minbias': ['minbias'],
        'Multijet': ['dijet', 'multijet', 'qcd'],
        'Performance': ['performance'],
        'SingleParticle': ['singleparticle'],
        'SingleTop': ['singletop'],
        'SUSY': ['bino', 'susy', 'pmssm', 'leptosusy', 'rpv', 'mssm'],
        'Triboson': ['triplegaugecoupling', 'triboson', 'zzw', 'www'],
        'TTbar': ['ttbar'],
        'TTbarX': ['ttw', 'ttz', 'ttv', 'ttvv', '4top', 'ttww'],
        'Upgrade': ['upgrad'],
        'Wjets': ['w'],
        'Zjets': ['z']}
    match = {}
    categories = []
    for phys_category in PHYS_CATEGORIES_MAP:
        current_map = [x.strip(' ').lower() for x in PHYS_CATEGORIES_MAP[phys_category]]
        if hashtags is not None:
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

def parsingArguments():
    parser = argparse.ArgumentParser(description='Process command line arguments.')
    parser.add_argument('--input', help='SQL file path')
    parser.add_argument('--config', help='Configuration file path')
    parser.add_argument('--size', help='Number of lines, processed at a time')
    parser.add_argument('--offset', help='Offset datetime (dd-mm-yyyy hh24:mi:ss)')
    return parser.parse_args()

if  __name__ == '__main__':
    main()
