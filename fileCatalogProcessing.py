#!/usr/bin/python
import os
import argparse
import json

class FileCatalog:
    'Common base class for all catalogs of files'

    def __init__(self):
        self.args = self.parsingArguments()
        self.dir_name = self.args.i

    def parsingArguments(self):
        parser = argparse.ArgumentParser(description='Process command line arguments.')
        parser.add_argument('--i', help='input directiry')
        parser.add_argument('--o', help='output directory')
        return parser.parse_args()

    def files2string(self):
        return ' '.join(os.listdir(self.dir_name))

    def files2json(self):
        for filename in os.listdir(self.dir_name):
            file_handle = open(self.dir_name + '/' + filename, 'r')
            print filename
            yield json.load(file_handle)

    def parameterSearch(self, data, params_list):
        for item in params_list:
            if isinstance(data, list):
                for x in data:
                    if isinstance(x, dict):
                        for k,v in x.items():
                            if k == item:
                                data = v
            elif isinstance(data, (str, unicode)):
                data = data
            else:
                data = data.get(item)
                if data is not None:
                    if len(params_list) == 1:
                        return data
                    else:
                        params_list.pop(0)
                        data = self.parameterSearch(data, params_list)
            return data

    def searchParameterByType(self, data, params_list, type):
        parameter = self.parameterSearch(data, params_list)
        if isinstance(parameter, type):
            return parameter

    def searchString(self, data, lookup):
        for key, value in data.items():
            for v in value:
                if lookup in v:
                    return key


def main():
    catalog = FileCatalog()
    #print catalog.files2string()
    json_content = catalog.files2json()
    #print next(json_content)
    #print catalog.parameterSearch(next(json_content), params_list)
    for item in json_content:
        params_list = ['CDS', 'primary_report_number']
        print catalog.parameterSearch(item, params_list)
        # print catalog.searchParameterByType(item, params_list, list)
        #print catalog.searchString(item, 'STDM-2010-01')



if  __name__ =='__main__':
    main()