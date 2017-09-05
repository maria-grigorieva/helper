from invenio_client import InvenioConnector
from invenio_client.contrib.cds import CDSInvenioConnector
import json
from urlparse import urlparse

cds = CDSInvenioConnector("user", "pwd")
results = cds.search(recid=1373726)
print results
