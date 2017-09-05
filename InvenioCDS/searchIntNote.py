from invenio_client import InvenioConnector
from invenio_client.contrib.cds import CDSInvenioConnector
import json
from urlparse import urlparse

cds = CDSInvenioConnector("user", "pwd")

with open('list_of_papers.json') as data_file:
   data = json.load(data_file)

for item in data:
   print item["id"]
   if "supporting_notes" in item:
      for note in item["supporting_notes"]:
         url = note["url"].replace('\\','')
         print url
         parsed = urlparse(url)
         recid = parsed.path.split('/')[-1]
         results = cds.get_record(recid)
         file = open('CDS_INTERNAL_NOTES/'+recid+'.json', 'w+')
         file.write(str(results))
         file.close()
         print "done!"
