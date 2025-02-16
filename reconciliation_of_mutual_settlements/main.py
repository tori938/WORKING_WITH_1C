import os

from create_reconciliation_statements import download_document
from send_statements import send_document

step = os.environ['step']

def main():
    if step == 'download statements':
        download_document()
    elif step == 'send statements':
        send_document()


if __name__ == '__main__':
    main()