from invenio_client import InvenioConnector
from invenio_client.contrib.cds import CDSInvenioConnector
import json

cds = CDSInvenioConnector("user", "pwd")

with open('list_of_papers.json') as data_file:
   data = json.load(data_file)

for item in data:
   print item["id"]
   results = cds.search(cc="ATLAS", p=item["full_title"], of="recjson")
   file = open('CDS_JSON/'+item["id"]+'.json', 'w+')
   file.write(results)
   file.close()
   print "done!"