#!/usr/bin/python
import os
import argparse
import json

class InputData:
    'Common base class for file/dir inputs'

    def __init__(self, input):
        if os.path.isdir(input):
            self.processor = CatalogProcessing(input)
        elif os.path.isfile(input):
            self.processor = FileProcessing(input)

    def getFilesList(self):
        return ' '.join(os.listdir(self.dir_name))

    def filesGenerator(self):
        for filename in os.listdir(self.dir_name):
            file_handle = open(self.dir_name + '/' + filename, 'r')
            print filename
            yield json.load(file_handle)

    def json_item_generator(self, json_file):
        with open(json_file, 'r') as file_content:
            data = json.load(file_content)
            if isinstance(data, list):
                for item in data:
                    yield item
            else:
                yield data

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


class CatalogProcessing():

    def __init__(self, input):
        self.input = input

    def get_files_list(self):
        return ' '.join(os.listdir(self.input))

    def files_generator(self):
        for filename in os.listdir(self.input):
            file_handle = open(self.input + '/' + filename, 'r')
            print filename
            yield json.load(file_handle)

class FileProcessing(InputData):

    def __init__(self, input):
        self.input = input


