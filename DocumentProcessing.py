#!/usr/bin/python
import json
import csv

class FileConnector(object):

    @staticmethod
    def factory(path):

        if path.endswith('json'):
            return JSONConnector(path)
        elif path.endswith('csv'):
            return CSVConnector(path)
        else:
            return TextConnector(path)

class TextConnector(FileConnector):

    def __init__(self, path):

        self.data = dict()

        with open(self.path, mode='r') as f:
            self.data = f.read()

        print self.data

class CSVConnector(FileConnector):

    def __init__(self, path):

        self.path = path
        self.open()

    def open(self):

        self.file_handler = open(self.path, 'rb')

    def generator(self):

        self.reader = csv.reader(self.file_handler, delimiter=',', quotechar='"')
        for row in self.reader:
            yield ', '.join(row)


class JSONConnector(FileConnector):

    def __init__(self, path):

        self.path = path
        self.open()

    def open(self):

        with open(self.path, mode='r') as f:
            self.data = json.load(f)

    def generator(self):

        if isinstance(self.data, list):
            for item in self.data:
                yield item
        else:
            yield self.data
