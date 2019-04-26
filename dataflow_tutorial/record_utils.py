import json
from io import StringIO
import csv
from google.cloud import bigquery
import logging
import time


def parse_records(raw_lines, filename):
    """Generator that yields raw records."""
    logging.getLogger().setLevel(logging.WARNING)
    loan = None
    read_success = False
    read_fails = 0
    while not read_success:
        try:
            table_date = raw_lines.read().decode("utf-8").replace("\ufeff", "")
            read_success = True
        except:
            read_fails += 1
            logging.warning('Failed to read file:'+filename+'\nHappened '+str(read_fails)+' times. Retrying.')
            time.sleep(3*read_fails)
            
    table_data = StringIO(table_date)
    reader = csv.reader(table_data, delimiter=",")
    header_codes = None
    for raw_line in reader:
        if not header_codes:
            header_codes = raw_line
            continue

        record = {}
        for i,var_code in enumerate(header_codes):
            var_value = raw_line[i]
            record[var_code] = var_value

        if len(record.keys()) == 0:
            continue

        yield record
        record = None
