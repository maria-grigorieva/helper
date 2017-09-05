'''
Author: Maria Grigorieva
maria.grigorieva@cern.ch

Execute on atlswing@aipanda070.cern.ch

input metadata:
  - list of papers & supporting documents [JSON] from GLANCE API

output metadata:
  1) CDS Records in MARC format
  2) MARC is converted in JSON

'''

from invenio_client import InvenioConnector
from invenio_client.contrib.cds import CDSInvenioConnector
import json
from urlparse import urlparse
import sys, getopt

def main(argv):
   login = ''
   password = ''
   try:
      opts, args = getopt.getopt(argv, "hl:p:",["login=","password="])
   except getopt.GetoptError:
      print '-l <login> -p <password>'
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print '-l <login> -p <password>'
         sys.exit()
      elif opt in ("-l", "--login"):
         login = arg
      elif opt in ("-p", "--password"):
         password = arg

   cds = CDSInvenioConnector(login, password)
   results = cds.search(cc="CERN People", of="xm")
   file = open("results.json", "w")
   file.write(results)
   file.close()
   print "done!"


if __name__ == "__main__":
   main(sys.argv[1:])
