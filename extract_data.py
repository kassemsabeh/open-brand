"""Extracts Brands Data From Amazon Product Metadata"""
import os
from typing import Any, Iterator, Mapping, Sequence, Tuple, Union
import json
import copy

import apache_beam as beam
from absl import app, flags



_INPUT_AMAZON_PRODUCT_METADATA_JSON_FILENAME = flags.DEFINE_string(
    'input_amazon_product_metadata_json_filename',
    default=None,
    help='The input JSON file name for the Amazon Product Metadata.',
    required=True)

_INPUT_TAGS_FILENAME = flags.DEFINE_string(
    'input_tags_filename',
    default=None,
    help='The input list file name for brand tag labels.',
    required=True)

_OUTPUT_DIR_NAME = flags.DEFINE_string(
    'output_dir_name',
    default=None,
    help='The output directory to save files.',
    required=True)

_OUTPUT_JSON_FILENAME = flags.DEFINE_string(
    'output_json_filename',
    default=None,
    help='The output filename after cleaning and processing data.',
    required=True)

_JsonObject = Mapping[str, Union[str, Sequence[Any]]]

_ASIN = 'asin'
_CATEGORY = 'main_cat'
_TITLE = 'title'
_BRAND = 'brand'
_TAG = 'tag'

class AggregateTags(beam.DoFn):
  """Aggregate Amazon Metadata with Tags"""

  def process(self, json_example: _JsonObject,
              labels_by_id: Mapping[str, _JsonObject], *args,
              **kwargs) -> Iterator[Tuple[_JsonObject, _JsonObject]]:
    labels = labels_by_id.get(json_example.get('asin', ''))
    if not labels:
      return
    yield json_example, copy.deepcopy(labels)

class CleanData(beam.DoFn):
    """Clean Amazon Metadata"""
    
    def process(self, element: Tuple[_JsonObject, _JsonObject], *args, **kwargs) -> Iterator[_JsonObject]:
      json_example, labels = element
      yield {
        "asin": json_example[_ASIN],
        "category": json_example[_CATEGORY],
        "description": json_example[_TITLE],
        "brand_name": json_example[_BRAND],
        "tag": labels[_TAG]
      }

def pipeline(my_pipeline):
    labels = (
        my_pipeline
        | 'ReadLabels' >> beam.io.textio.ReadFromText(
            _INPUT_TAGS_FILENAME.value)
        | 'Load_Labels' >> beam.Map(json.loads)
        | 'GetASIN' >> beam.Map(lambda x: (x[_ASIN], x))
        )
    
    lines = (
        my_pipeline
        | 'ReadData' >> beam.io.textio.ReadFromText(
            _INPUT_AMAZON_PRODUCT_METADATA_JSON_FILENAME.value)
        | 'Load_Data' >> beam.Map(json.loads)
        | 'JoinWithLabels' >> beam.ParDo(
          AggregateTags(), labels_by_id=beam.pvalue.AsDict(labels))
        | 'CleanData' >> beam.ParDo(CleanData())
        | 'GroupData' >> beam.GroupBy(lambda x: x[_ASIN])
        | 'RemoveDuplicate' >> beam.Map(lambda x: list(x[1])[0])

        )
    _ = (
        lines
        | 'JSONDumps' >> beam.Map(json.dumps)
        | 'WriteToJSONLine' >> beam.io.WriteToText(
           _OUTPUT_JSON_FILENAME.value,
            shard_name_template='',
      ))

def main(unused_argv: Sequence[str]) -> None:
    if not os.path.exists(_OUTPUT_DIR_NAME.value):
        os.mkdir(_OUTPUT_DIR_NAME.value)
    with beam.Pipeline() as pip:
        pipeline(pip) 

if __name__ == '__main__':
    app.run(main)
    

