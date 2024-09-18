import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import json

class ParseJson(beam.DoFn):
    def process(self, element):
        # Parse the JSON string into a dictionary
        record = json.loads(element)
        yield record

def run(argv=None):
    # Set up the pipeline options
    pipeline_options = PipelineOptions()

    # Get parameters from the pipeline options
    gcloud_options = pipeline_options.view_as(GoogleCloudOptions)
    gcloud_options.project = pipeline_options.view_as(GoogleCloudOptions).project
    gcloud_options.staging_location = pipeline_options.view_as(GoogleCloudOptions).staging_location
    gcloud_options.temp_location = pipeline_options.view_as(GoogleCloudOptions).temp_location
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    pipeline_options.view_as(StandardOptions).streaming = False  # Set to True if you want streaming

    # Create ValueProviders for input and output parameters
    input_file = pipeline_options.view_as(PipelineOptions).input
    output_table = pipeline_options.view_as(PipelineOptions).output

    # Create the Apache Beam pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'ReadFromGCS' >> beam.io.ReadFromText(input_file)
            | 'ParseJson' >> beam.ParDo(ParseJson())
            | 'WriteToBigQuery' >> WriteToBigQuery(
                output_table,
                schema='SCHEMA_AUTODETECT',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    # Define custom pipeline options
    class CustomOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_value_provider_argument(
                '--input',
                type=str,
                help='The Google Cloud Storage location of the input files.')
            parser.add_value_provider_argument(
                '--output',
                type=str,
                help='The BigQuery table where the data will be written.')

    # Pass custom options to the pipeline
    run()
