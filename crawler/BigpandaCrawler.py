'''
Class for web-scraping of BigpandaMonitor
(implemented for the DKB)
'''
import urllib3
import json
import argparse

class BigpandaCrawler:

    def __init__(self, url):
        self.URL = url
        self.http, self.resp = self.setConnectionInstance()

    def setConnectionInstance(self):
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

    def getJSON(self):
        try:
            return json.loads(self.resp.data)
        except ValueError, e:
            print('This page doesn\'t return JSON')

    def docGenerator(self):
        for doc in self.getJSON():
            yield doc


