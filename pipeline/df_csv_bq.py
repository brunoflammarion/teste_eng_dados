# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse
import logging
import re
from datetime import datetime, timedelta
from pytz import timezone
import pytz
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class FormatSchema(beam.DoFn): 

  def process(self,element):
    
    element = element.split(";")    
    return [{'ID_MARCA': element[0]
    ,'MARCA': element[1]
    ,'ID_LINHA': element[2]
    ,'LINHA': element[3]
    ,'DATA_VENDA': element[4][6:10].strip()+"-"+element[4][3:5].strip()+"-"+element[4][0:2].strip()
    ,'QTD_VENDA': element[5]
    }]

def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to read. This can be a local file or '
        'a file in a Google Storage Bucket.',
        default='gs://testeboticatios/upload/Base_2017_1.csv')

    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='teste_boticatio.base_boticario')
    known_args, pipeline_args = parser.parse_known_args(argv)

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))
    schema='ID_MARCA:INTEGER,MARCA:STRING,ID_LINHA:INTEGER,LINHA:STRING,DATA_VENDA:DATE,QTD_VENDA:INTEGER'
    #additional_bq_parameters = {
    #    'timePartitioning': {'type': 'DAY', 'field': 'DATA_VENDA'},
    #    'clustering': {'fields': ['DATA_VENDA']}
    #}
    (p
     | 'Read from a File' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
     | 'Format' >> beam.ParDo(FormatSchema())
     | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
             known_args.output,
             schema=schema,
             #additional_bq_parameters=additional_bq_parameters,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             ##write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
             )
    )
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()