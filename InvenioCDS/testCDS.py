from invenio_client import InvenioConnector
from invenio_client.contrib.cds import CDSInvenioConnector
import json
import sys, getopt
import uuid
import os
from pymarc import MARCReader

def usage():
  msg='''
USAGE
  ./getCDSPapers.py <options>

OPTIONS
  -l, --login     LOGIN   CERN account login
  -p, --password  PASSWD  CERN account password
  -h, --help              Show this message and exit
'''
  sys.stderr.write(msg)

def search_paper(cds):
   results = cds.search(cc="ATLAS",
                        aas=1,
                        m1="p",
                        p1="CERN-PH-EP-2015-191",
                        f1="reportnumber",
                        m2="a",
                        op2="a",
                        p2="ARTICLE, ATLAS_Papers",
                        f2="collection",
                        ot=['088','100','856','245','653'])
   print results
   for record in results:
      for internal_note in record["8564_"]:
         print internal_note["u"][0]

def main(argv):
   login = ''
   password = ''

   try:
      opts, args = getopt.getopt(argv, "hl:p:m:",["login=","password=","mode="])
   except getopt.GetoptError:
      usage()
      sys.exit(2)


   for opt, arg in opts:
      if opt == '-h':
         usage()
         sys.exit()
      elif opt in ("-l", "--login"):
         login = arg
      elif opt in ("-p", "--password"):
         password = arg

   cds = CDSInvenioConnector(login, password)

   results=search_paper(cds)


if __name__ == "__main__":
   main(sys.argv[1:])
