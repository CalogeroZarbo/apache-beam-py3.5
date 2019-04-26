from .record_utils import *

import json
import logging
import pprint

import apache_beam as beam
import tensorflow as tf
import tensorflow_transform as tft

from apache_beam.io import filebasedsource
from tensorflow_transform.tf_metadata import dataset_schema


class ParseRecords(sfilebasedsource.FileBasedSource):
    def __init__(self, file_pattern):
        super(ParseRecords, self).__init__(file_pattern, splittable=False)

    def in_range(self, range_tracker, position):
        """This helper method tries to set the position to where the file left off.
        Since this is a non-splittable source, we have to call
        `set_current_position`, but if it tries to set the position to a split
        point, we still have to call `try_claim`, otherwise an error will be raised.
        """
        try:
            return range_tracker.set_current_position(position)
        except ValueError:
            return range_tracker.try_claim(position)

    def read_records(self, filename, range_tracker):
        """This yields a dictionary with the sections of the file that we're
        interested in. The `range_tracker` allows us to mark the position where
        possibly another worker left, so we make sure to start from there.
        """
        print('Reading file', filename)
        with self.open_file(filename) as f:
            f.seek(range_tracker.start_position() or 0)
            while self.in_range(range_tracker, f.tell()):
                for json_loan in parse_records(f, filename):  
                    yield json_loan
