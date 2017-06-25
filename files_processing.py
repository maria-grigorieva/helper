from DocumentProcessing import FileConnector
import argparse
import re
import sys
import os


def main():
    filename = sys.argv[1]

    document = FileConnector.factory(filename)

    print document.__class__

    for item in document.generator():
        print item

if  __name__ =='__main__':
    main()