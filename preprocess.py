#!/usr/bin/env python

import argparse
import dill as pickle
import os
import random
import tempfile

import dataflow_tutorial

import apache_beam as beam
import tensorflow as tf
import tensorflow_transform.beam.impl as beam_impl

from apache_beam.io import tfrecordio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from tensorflow_transform.beam.tft_beam_io import transform_fn_io
from tensorflow_transform.coders import example_proto_coder
from tensorflow_transform.tf_metadata import dataset_metadata
from tensorflow_transform.tf_metadata import dataset_schema


def run(
        files_pattern,
        table_name,
        table_schema,
        feature_scaling=None,
        eval_percent=20.0,
        beam_options=None,
        work_dir=None):

    """Runs the whole preprocessing step.
    This runs the feature extraction from CSV files in the given Google Storage entrypoint and upload them in a BigQuery Table.
    """
    tft_temp_dir = os.path.join(work_dir, 'tft-temp')

    transform_fn_dir = os.path.join(work_dir, transform_fn_io.TRANSFORM_FN_DIR)
    if tf.gfile.Exists(transform_fn_dir):
        tf.gfile.DeleteRecursively(transform_fn_dir)

    # Build and run a Beam Pipeline
    with beam.Pipeline(options=beam_options) as p, \
            beam_impl.Context(temp_dir=tft_temp_dir):

        # Extract records from sources
        dataset = p | 'Read Record' >> beam.io.Read(dataflow_tutorial.ParseRecords(files_pattern))

        # Write the Table on BigQuery
        dataset | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            table_name,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY)


if __name__ == '__main__':
    """Main function"""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '--work-dir',
        type=str,
        default=os.path.join(
            tempfile.gettempdir(), 'localdata'),
        help='Directory for staging and working files. '
             'This can be a Google Cloud Storage path.')

    args, pipeline_args = parser.parse_known_args()

    beam_options = PipelineOptions(pipeline_args)
    beam_options.view_as(SetupOptions).save_main_session = True

    data_files_pattern = os.path.join(args.work_dir, '*.csv')
    
    run(
        data_files_pattern,
        table_name = dataflow_tutorial.table_name,
        table_schema = dataflow_tutorial.table_schema,
        beam_options=beam_options,
        work_dir=args.work_dir)
