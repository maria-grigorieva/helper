import urllib3
import json
import argparse

class BigpandaCrawler:

    def __init__(self):
        self.args = self.parsingArguments()
        self.URL = self.args.url
        # PoolManager instance to make requests
        # This object handles all of the details of connection pooling
        try:
            self.http = urllib3.PoolManager()
            try:
                self.resp = self.http.request('GET', self.URL)
            except urllib3.exceptions.NewConnectionError as e:
                print('Connection failed.')
        except:
            print('Cannot create PoolManager instance')

    def parsingArguments(self):
        parser = argparse.ArgumentParser(description='Process command line arguments.')
        parser.add_argument('--url', help='URL of source page')
        return parser.parse_args()

    def getStatus(self):
        return self.resp.status

    def getData(self):
        return self.resp.data

    def getHeaders(self):
        return self.resp.headers

    def getJSON(self):
        try:
            return json.loads(self.getData())
        except ValueError, e:
            print('This page doesn\'t return JSON')

    def docGenerator(self):
        for doc in self.getJSON():
            yield doc


