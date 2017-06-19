#!/usr/bin/python
import os
import argparse
import json

class FileCatalog:
    'Common base class for all catalogs of files'

    def __init__(self, dir_name):
        # self.args = self.parsingArguments()
        # self.dir_name = self.args.i
        self.dir_name = dir_name

    def getFilesList(self):
        return ' '.join(os.listdir(self.dir_name))

    def getFileJSON(self):
        for filename in os.listdir(self.dir_name):
            file_handle = open(self.dir_name + '/' + filename, 'r')
            print filename
            yield json.load(file_handle)

    def getJSONParameter(self, data, params_list):
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

    def getParamByType(self, data, params_list, type):
        parameter = self.getJSONParameter(data, params_list)
        if isinstance(parameter, type):
            return parameter
        else:
            return False

    def searchString(self, data, lookup):
        for key, value in data.items():
            for v in value:
                if lookup in v:
                    return key
