This tutorial shows how to set up a basic WordCount example in pyflink, reading data from kafka and writing to console or file.
## Installing
Pyflink supports only certain versions of python and other libs. Currently, max supported python version is 3.8. Which is why it is recommended to use `conda` (anaconda or miniconda) to set up a proper clean venv and carefully handle all dependencies.
```bash
# create fresh venv with python3.8
conda create -n pyflink pip python=3.8
conda activate pyflink
# install latest flink version. Tested on 1.8.1.
pip install apache-flink
```
Because development of flink programs involves passing around the executable between your local machine and the cluster, make sure that flink version you are using in your dependencies, installed libraries, local and remote cluster are the same:  
- Choose the same version (here, 1.8.1) when downloading local flink from [here](https://flink.apache.org/downloads/)
- Choose the same flink version for side-party flink connectors, like kafka
- Make sure you always launch pyflink jobs from the same `pyflink` virtual environment
- When deploying in the cloud, make sure that the cloud has `flink` of the same version   

If at any point flink versions and python versions used differ between local env and the cluster, you might get unexplained pickling errors or something even more cryptic.    

Next, download Kafka connector from [this page](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/table/downloads/) and keep it handy. Unlike jvm-based programs that download dependencies automatically from Maven repository, with python programs, you will have to download the `.jar` yourself and pass it along with the code when running a job.  

## Where to find information
The official [flink docs](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/python/datastream_tutorial/) have some information and code examples for pyflink with datastream api - we are using this api in this tutorial. Various parts of this doc tell about important aspects, for example, [submitting a job](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/cli/#submitting-pyflink-jobs).   

[Here](https://nightlies.apache.org/flink/flink-docs-master/api/python/reference/pyflink.datastream/api/pyflink.datastream.connectors.kafka.FlinkKafkaConsumer.html) is the reference with argument help for every specific function.    

Official code [examples](https://github.com/apache/flink/tree/master/flink-python/pyflink/examples)

3d party code [examples](https://github.com/aws-samples/pyflink-getting-started).   

## Writing wordcount code in pyflink
Imports and parsing command line arguments:   
```python
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
```
If we provide `--output` option, Pyflink will aggregate job outputs and transfer them from cluster back to the local machine. Alternatively, it will use `PrintSink` that prints the output to the stdout on the corresponding `taskexecutor`. You will have to get to cluster logs in Flink UI in order to read that.   

Initializing kafka source:   
```python
def main(args):
    env = StreamExecutionEnvironment.get_execution_environment()
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    env.add_jars("file:///" + CURRENT_DIR + "/connectors/flink-sql-connector-kafka_2.11-1.14.4.jar")
    
    kafka_source = FlinkKafkaConsumer(
        topics='input',
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': 'localhost:19092', 'group.id': 'test_group'})
    
    ds = env.add_source(kafka_source)
```
We define our workflow by creating an `execution environment`. As mentioned before, kafka connector `.jar` is necessary to read data from Kafka, and we have to provide the jar to our script externally. 

We provide kafka server address, topic name, and initialize a `ds` datastream. Datastream is the object on which we then define map and reduce transforms to perform actual wordcounting logic:   
```python
    def split(line):
        yield from line.split()

    ds = ds.flat_map(split)
    # add bogus "1" counter for future reduce
    ds = ds.map(lambda word: (word, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()]))
    # group by word
    ds = ds.key_by(lambda pair: pair[0])
    # reduce
    ds = ds.reduce(lambda pair1, pair2: (pair1[0], pair1[1] + pair2[1]), output_type=Types.TUPLE([Types.STRING(), Types.LONG()]))
```
Actual map and reduce transforms can be defined quite easily either with lambda functions, Python functions or classes with `process` methods in case we need some extra functionality.   

We provide `output_type` to specify how to serialize results in between pipeline steps. Different parts of the workflow can be executed by different workers, especially after `key_by` that reshuffles the data, sending entries with the same key to the same worker. If `output_type` is not provided, it will be `PICKLED_BYTE_ARRAY` by default - arbitrary python object will be pickled to transfer to another worker.   

To perform the actual wordcounting logic, we do the following:   
- We split a line into words - this is a map operation. This means that for every one message in the input kafka topic, we generate 0, 1 or many output messages
- We add a bogus `1` counter to every word to make it easier to aggregate later
- We group entries by `word`, sending all entries with the same word to the same worker
- We perform `reduce` operation. It takes in a binary operator that tells it how to perform "sum" over two objects, yielding another object of the same class. Flink will apply this operation in some order to all input messages, outputting 1 message per each input message, with the total word count for that word so far.   

Note that without windows, `reduce` operation uses `global state`: it saves current total wordcount for every word encountered so far. The job can potentially run for months and years, and encounter millions of unique "words". Because of this, it is better to have checkointing mechanism to save this state in case of worker failures, and ttl mechanism to prevent it from growing indefinitely. Unfortunately, there is no way to specify `ttl` to this simple version of reduce function; we'll have to rewrite it to a `process` function with custom state handling. Because of this, it is better to introduce windowing into the pipeline, so that state is only kept for the duration of the window:   
```python
watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()

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
```
In order to use windowed reduce, we also need to add watermarking strategy. Without it, the job will not fail, but watermarks will not be generated, and the window will never be closed. This looks like a job that is stuck forever, and problems like these are quite hard to debug.   

We use `WatermarkStrategy.for_monotonous_timestamps` watermark strategy. It assumes that all timestamps in input events will be monotonously increasing, and discards any event that violates that assumption. In general case, you should carefully consider your timestamps guarantee, and choose proper watermark strategy as a trade-off between overall pipeline latency and percentage of events discarded. For example, we could use `.forBoundedOutOfOrderness(Duration.ofSeconds(5))` watermark strategy; it will allow events that are at most 5 seconds late to make it through the pipeline; this will also increase the latency of the pipeline by 5 seconds. In our case, we are going to use kafka timestamps for watermarking, and they are indeed guaranteed to be monotonous.   

Using windowed reduce introduces two major changes to the pipeline:   
- The state is now `local`, and keeps track of word counts within current window only. This means that the output of the pipeline is no longer `total word count since the beginning of times`, but simply `word count for this word in the past 5 seconds`.     
- The number of output events is drastically reduced. The pipeline used to generate 1 output event for every word in the input event. Now, it will only output 1 event per unique word per 5 seconds.   

Finally, we sink our data either to local file or to debug log on the cluster:   
```python
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
```
We call `env.execute()` in the end to actually start the execution.   

## Setting up kafka and flink cluster
I am using RedPanda as a kafka client. To set up a topic, simply copy the [docker-compose.yml](./docker-compose.yml) file, and run `docker compose up`.   

Then, install rpk cli tool: [link](https://docs.redpanda.com/current/get-started/rpk-install/), and use it to create a kafka topic and push some messages into it:   
```bash
# create a topic named "input"
rpk -X brokers=localhost:19092 topic create input
# list topics to validate that it was created succesfully
rpk -X brokers=localhost:19092 topic list
# start publishing messages
# reads from stdin
rpk -X brokers=localhost:19092 topic produce input
```

To test our job, we also need some sort of cluster. We can launch one in the cloud or locally; to launch one locally, download flink from [here](https://flink.apache.org/downloads/), making sure that flink version matches pyflink version. Unpack the archive and put it somewhere handy, like `~/bin`. Then launch local cluster:   
```bash
~/bin/flink-1.18.1/bin/start-cluster.sh
```
## Launching the job
```bash
~/bin/flink-1.18.1/bin/flink run --python word_count.py --jarfile connectors/flink-sql-connector-kafka_2.11-1.14.4.jar -pyexec ~/miniconda3/envs/pyflink/bin/python3
```
We use `flink run` to launch the job, similar to how that was done in kotlin. We provide `.jar` files with external dependencies, like a Kafka connector; we also pass in an exact python executable that we used to install the libraries; failure to do so might cause unexplainable errors. Remember to also run this command under the same `venv` you used to set everything up initially.   

Running the command will start flink job on the cluster. It will be accessible in WebUI. One difference of the process as compared to java/kotlin is that interrupting the command will cancel the job. Launch it with `--detached` arg to disable this behaviour.