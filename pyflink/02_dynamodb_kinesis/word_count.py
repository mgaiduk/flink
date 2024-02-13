import argparse
import logging
import os
import sys

from pyflink.common import Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import (FileSink, OutputFileConfig, RollingPolicy)
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common import WatermarkStrategy

from pyflink.datastream.functions import SinkFunction
from pyflink.datastream.functions import ProcessFunction, RuntimeContext
import boto3


# class DynamoDBSink(SinkFunction):
#     def __init__(self, table_name="WordCounts", region_name='us-east-1', endpoint_url='http://localhost:8000'):
#         self.table_name = table_name
#         self.region_name = region_name
#         self.endpoint_url = endpoint_url

#     def open(self, runtime_context):
#         self.dynamodb = boto3.resource('dynamodb', region_name=self.region_name, endpoint_url=self.endpoint_url)
#         self.table = self.dynamodb.Table(self.table_name)

#     def invoke(self, value, context):
#         item = {
#             'word': value[0],  # Primary key
#             'count': value[1],
#         }
#         self.table.put_item(Item=item)


class DynamoDBWriter(ProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url='http://localhost:8000')
        self.table = self.dynamodb.Table('WordCounts')

    def process_element(self, value, ctx):
        item = {
            'word': value[0],  # Primary key
            'count': value[1],
        }
        self.table.put_item(Item=item)

def main(args):
    kafka_source = FlinkKafkaConsumer(
        topics='input',
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': 'localhost:19092', 'group.id': 'test_group'})
    
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()

    env = StreamExecutionEnvironment.get_execution_environment()
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    env.add_jars("file:///" + CURRENT_DIR + "/connectors/flink-sql-connector-kafka_2.11-1.14.4.jar")
    ds = env.add_source(kafka_source).assign_timestamps_and_watermarks(watermark_strategy)

    def split(line):
        yield from line.split()

    ds = ds.flat_map(split)
    # add bogus "1" counter for future reduce
    ds = ds.map(lambda word: (word, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()]))
    # group by word
    ds = ds.key_by(lambda pair: pair[0])
    # window
    ds = ds.window(TumblingEventTimeWindows.of(Time.seconds(5)))
    # reduce
    ds = ds.reduce(lambda pair1, pair2: (pair1[0], pair1[1] + pair2[1]), output_type=Types.TUPLE([Types.STRING(), Types.LONG()]))
    # Apply the process function to write to DynamoDB
    ds.process(DynamoDBWriter())

    # sink to printsink just in case
    ds.print()
    
    # submit for execution
    env.execute()






if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        help='Output file to write results to.')

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    main(known_args)