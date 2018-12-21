"""
This function return the word count of a text passed as input.

Call this function using Python 2.7.

Requirements in wordcount_requirements.txt

Run function : python -m wordcount.py --input YOUR_INPUT_FILE --output OUTPUT_FILE_PREFIX
"""

from __future__ import absolute_import

import argparse
import re

#from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

PROJECT = '[YOUR_PROJECT_ID]'
BUCKET  = '[YOUR_BUCKET_ID]'
REGION  = '[YOUR_REGION]'

def run(argv=None):
    '''Main entry point; defines and runs the wordcount pipeline.'''
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--input',
                        dest='input',
                        default='gs://dataflow-samples/shakespeare/kinglear.txt',
                        help='Input file to process.')
    
    parser.add_argument('--output',
                        dest = 'output_prefix',
                        default = 'gs://{0}/output_word_count'.format(BUCKET),
                        help='Output prefix for the file to write results to.')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
   
    pipeline_args.extend([
        '--runner=DataflowRunner', # or DirectRunner if you want to run the job locally
        '--project={0}'.format(PROJECT),
        '--staging_location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/temp/'.format(BUCKET),
        '--job_name=examplejob',
        '--region={0}'.format(REGION)
    ])
    
    pipeline_options = PipelineOptions(pipeline_args) 
    pipeline_options.view_as(SetupOptions).save_main_session = True
    
    def format_result(word_count):
            (word, count) = word_count
            return '%s: %s' % (word, count)
    
    # p = beam.Pipeline(options=pipeline_options)
    with beam.Pipeline(options=pipeline_options) as p:
        (p  | ReadFromText(known_args.input)
            | 'Split' >> (beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x)).with_output_types(unicode))
            | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
            | 'GroupAndSum' >> beam.CombinePerKey(sum)
            | 'Format' >> beam.Map(format_result)
            | 'Write' >> WriteToText(known_args.output_prefix)
        )
    
    # p.run() #if PCollections is defined outside 'with' statement
    
if __name__ == '__main__':
    run()
