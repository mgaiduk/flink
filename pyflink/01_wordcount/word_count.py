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

     # define the sink
    if args.output is not None:
        ds.sink_to(
            sink=FileSink.for_row_format(
                base_path=args.output,
                encoder=Encoder.simple_string_encoder())
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix("prefix")
                .with_part_suffix(".ext")
                .build())
            .with_rolling_policy(RollingPolicy.default_rolling_policy())
            .build()
        )
    else:
        print("Printing result to stdout. Use --output to specify output path.")
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