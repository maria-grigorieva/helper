from DocumentProcessing import FileConnector
import argparse
import re
import sys
import os
from FilesProcessing import CatalogProcessing


def main():
    dirname = sys.argv[1]

    dir = CatalogProcessing(dirname)

    for item in dir.files_generator():
        if len(item["supporting_notes"]) > 0:
            for i in item["supporting_notes"]:
                cds = i["CDS"]
                files = cds["files"]
                if len(files) >= 2:
                    for f in files:
                        print f

    # document = FileConnector.factory(filename)
    #
    # print document.__class__
    #
    # for item in document.generator():
    #     print item

if  __name__ =='__main__':
    main()