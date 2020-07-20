import argparse
import logging
import os
import re

from io import BytesIO
from tempfile import TemporaryFile
from multiprocessing.pool import ThreadPool
import boto3
import botocore
import time
from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

import re

from collections import Counter

from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

from bs4 import BeautifulSoup

LOGGING_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'

class JupyterCCSparkJob(object):
    """
    A simple Spark job definition to process Common Crawl data
    """

    name = 'CCSparkJob'

    output_schema = StructType([
        StructField("key", StringType(), True),
        StructField("val", LongType(), True)
    ])

    # description of input and output shown in --help
    input_descr = "Path to file listing input paths"
    output_descr = "Name of output table (saved in spark.sql.warehouse.dir)"

    warc_parse_http_header = True

    args = None
    records_processed = None
    warc_input_processed = None
    warc_input_failed = None
    log_level = 'INFO'
    logging.basicConfig(level=log_level, format=LOGGING_FORMAT)

    num_input_partitions = 40
    num_output_partitions = 10
    input_path = "/Users/alexxue/Desktop/commoncrawltest/cc-pyspark/input/subset_news_warc.txt"
    output_path = "delete_this"

    def parse_arguments(self):
        """ Returns the parsed arguments from the command line """

        description = self.name
        if self.__doc__ is not None:
            description += " - "
            description += self.__doc__
        arg_parser = argparse.ArgumentParser(prog=self.name, description=description,
                                             conflict_handler='resolve')

        arg_parser.add_argument("--input", default=self.input_path, help=self.input_descr)
        arg_parser.add_argument("--output", default=self.output_path, help=self.output_descr)

        arg_parser.add_argument("--num_input_partitions", type=int,
                                default=self.num_input_partitions,
                                help="Number of input splits/partitions")
        arg_parser.add_argument("--num_output_partitions", type=int,
                                default=self.num_output_partitions,
                                help="Number of output partitions")
        arg_parser.add_argument("--output_format", default="parquet",
                                help="Output format: parquet (default),"
                                     " orc, json, csv")
        arg_parser.add_argument("--output_compression", default="gzip",
                                help="Output compression codec: None,"
                                     " gzip/zlib (default), snappy, lzo, etc.")
        arg_parser.add_argument("--output_option", action='append', default=[],
                                help="Additional output option pair"
                                     " to set (format-specific) output options, e.g.,"
                                     " `header=true` to add a header line to CSV files."
                                     " Option name and value are split at `=` and"
                                     " multiple options can be set by passing"
                                     " `--output_option <name>=<value>` multiple times")

        arg_parser.add_argument("--local_temp_dir", default=None,
                                help="Local temporary directory, used to"
                                     " buffer content from S3")

        arg_parser.add_argument("--log_level", default=self.log_level,
                                help="Logging level")
        arg_parser.add_argument("--spark-profiler", action='store_true',
                                help="Enable PySpark profiler and log"
                                     " profiling metrics if job has finished,"
                                     " cf. spark.python.profile")

        self.add_arguments(arg_parser)
        args = arg_parser.parse_args(args=[])
        if not self.validate_arguments(args):
            raise Exception("Arguments not valid")
        self.init_logging(args.log_level)
        # print(args.input)
        return args

    def add_arguments(self, parser):
        pass

    def validate_arguments(self, args):
        if "orc" == args.output_format and "gzip" == args.output_compression:
            # gzip for Parquet, zlib for ORC
            args.output_compression = "zlib"
        return True

    def get_output_options(self):
        return {x[0]: x[1] for x in map(lambda x: x.split('=', 1),
                                        self.args.output_option)}

    def init_logging(self, level=None):
        if level is None:
            level = self.log_level
        else:
            self.log_level = level
        logging.basicConfig(level=level, format=LOGGING_FORMAT)

    def init_accumulators(self, sc):
        self.records_processed = sc.accumulator(0)
        self.warc_input_processed = sc.accumulator(0)
        self.warc_input_failed = sc.accumulator(0)

    def get_logger(self, spark_context=None):
        """Get logger from SparkContext or (if None) from logging module"""
        if spark_context is None:
            return logging.getLogger(self.name)
        return spark_context._jvm.org.apache.log4j.LogManager \
            .getLogger(self.name)

    def run(self):
        self.args = self.parse_arguments()

        conf = SparkConf()

        if self.args.spark_profiler:
            conf = conf.set("spark.python.profile", "true")

        sc = SparkContext.getOrCreate(  # appName=self.name,

            conf=conf)

        sqlc = SQLContext(sparkContext=sc)

        self.init_accumulators(sc)

        self.run_job(sc, sqlc)

        if self.args.spark_profiler:
            sc.show_profiles()

        sc.stop()

    def log_aggregator(self, sc, agg, descr):
        self.get_logger(sc).info(descr.format(agg.value))

    def log_aggregators(self, sc):
        self.log_aggregator(sc, self.warc_input_processed,
                            'WARC/WAT/WET input files processed = {}')
        self.log_aggregator(sc, self.warc_input_failed,
                            'WARC/WAT/WET input files failed = {}')
        self.log_aggregator(sc, self.records_processed,
                            'WARC/WAT/WET records processed = {}')

    @staticmethod
    def reduce_by_key_func(a, b):
        return a + b

    def run_job(self, sc, sqlc):

        input_data = sc.textFile(self.args.input,
                                 minPartitions=self.args.num_input_partitions)
        print("going to output process warcs")
        output = input_data.mapPartitionsWithIndex(self.process_warcs)
        sqlc.createDataFrame(output, schema=self.output_schema) \
            .coalesce(self.args.num_output_partitions) \
            .write \
            .format(self.args.output_format) \
            .option("compression", self.args.output_compression) \
            .options(**self.get_output_options()) \
            .saveAsTable(self.args.output)

        self.log_aggregators(sc)

    def process_warcs(self, id_, iterator):
        s3pattern = re.compile('^s3://([^/]+)/(.+)')
        base_dir = "/user/"  # os.path.abspath(os.path.dirname(__file__))

        # S3 client (not thread-safe, initialize outside parallelized loop)
        no_sign_request = botocore.client.Config(
            signature_version=botocore.UNSIGNED)
        s3client = boto3.client('s3', config=no_sign_request)
        start = time.time()
        #  spark.time()

        for uri in iterator:
            self.warc_input_processed.add(1)
            if uri.startswith('s3://'):
                self.get_logger().info('Reading from S3 {}'.format(uri))
                s3match = s3pattern.match(uri)
                if s3match is None:
                    self.get_logger().error("Invalid S3 URI: " + uri)
                    continue
                bucketname = s3match.group(1)
                path = s3match.group(2)
                warctemp = TemporaryFile(mode='w+b',
                                         dir=self.args.local_temp_dir)
                try:
                    s3client.download_fileobj(bucketname, path, warctemp)
                except botocore.client.ClientError as exception:
                    self.get_logger().error(
                        'Failed to download {}: {}'.format(uri, exception))
                    self.warc_input_failed.add(1)
                    warctemp.close()
                    continue
                warctemp.seek(0)
                stream = warctemp
            elif uri.startswith('hdfs://'):
                self.get_logger().error("HDFS input not implemented: " + uri)
                continue
            else:
                self.get_logger().info('Reading local stream {}'.format(uri))
                if uri.startswith('file:'):
                    uri = uri[5:]
                uri = os.path.join(base_dir, uri)
                try:
                    stream = open(uri, 'rb')
                except IOError as exception:
                    self.get_logger().error(
                        'Failed to open {}: {}'.format(uri, exception))
                    self.warc_input_failed.add(1)
                    continue

            no_parse = (not self.warc_parse_http_header)
            end = time.time()
            print("ended with : ", end - start)
            try:
                archive_iterator = ArchiveIterator(stream,
                                                   no_record_parse=no_parse)

              #  pool = ThreadPool(5)
              #  a = pool.map(lambda x: x, self.iterate_records(uri,archive_iterator) )
              #  yield from a
                for res in self.iterate_records(uri, archive_iterator):
                    yield res
            except ArchiveLoadFailed as exception:
                self.warc_input_failed.add(1)
                self.get_logger().error(
                    'Invalid WARC: {} - {}'.format(uri, exception))
            finally:
                stream.close()

    def process_record(self, record):
        raise NotImplementedError('Processing record needs to be customized')

    def iterate_records(self, _warc_uri, archive_iterator):
        """Iterate over all WARC records. This method can be customized
           and allows to access also values from ArchiveIterator, namely
           WARC record offset and length."""
        ok = 0


        for record in archive_iterator:
            for res in self.process_record(record):
                ok += 1
                if ok >= 10:
                    break
                yield res
            if ok >= 10:
                break

            self.records_processed.add(1)
            # WARC record offset and length should be read after the record
            # has been processed, otherwise the record content is consumed
            # while offset and length are determined:
            #  warc_record_offset = archive_iterator.get_record_offset()
            #  warc_record_length = archive_iterator.get_record_length()

    @staticmethod
    def is_wet_text_record(record):
        """Return true if WARC record is a WET text/plain record"""
        return (record.rec_type == 'conversion' and
                record.content_type == 'text/plain')

    @staticmethod
    def is_wat_json_record(record):
        """Return true if WARC record is a WAT record"""
        return (record.rec_type == 'metadata' and
                record.content_type == 'application/json')

    @staticmethod
    def is_html(record):
        """Return true if (detected) MIME type of a record is HTML"""
        html_types = ['text/html', 'application/xhtml+xml']
        if (('WARC-Identified-Payload-Type' in record.rec_headers) and
                (record.rec_headers['WARC-Identified-Payload-Type'] in
                 html_types)):
            return True
        for html_type in html_types:
            if html_type in record.content_type:
                return True
        return False


class StringMatchCountJob(JupyterCCSparkJob):
    """ Word count (frequency list) from texts in Common Crawl WET files"""

    name = "StringMatchCount"

    output_schema = StructType([
        StructField("title", StringType(), True),
        StructField("body", StringType(), True),
        StructField("url", StringType(), True),
        StructField("record_date", StringType(), True),
        StructField("content_length", StringType(), True),
        StructField("warc_ip", StringType(), True),
        StructField("warc_truncated", StringType(), True),

        StructField("server_name", StringType(), True),
        StructField("vary", StringType(), True),
        StructField("cache_control", StringType(), True),
        StructField("content_type", StringType(), True),
        StructField("http_date", StringType(), True),
        StructField("http_date_expires", StringType(), True),
        StructField("html_meta", ArrayType(StringType()), True),
        StructField("html_hyperlink", ArrayType(StringType()), True),
        StructField("html_image", ArrayType(StringType()), True)
    ]

    )

    # simple Unicode-aware tokenization
    # (not suitable for CJK languages)

    word_pattern = re.compile('\w+', re.UNICODE)

    def process_record(self, record):
        if record.rec_type == 'response':
            # record headers

            record_url = record.rec_headers.get_header('WARC-Target-URI', None)
            record_date = record.rec_headers.get_header('WARC-Date', None)
            record_content_length = record.rec_headers.get_header('Content-Length', None)
            record_ip = record.rec_headers.get_header('WARC-IP-Address', None)
            record_truncated = record.rec_headers.get_header('WARC-Truncated', None)

            # http headers

            http_server_name = record.http_headers.get_header('Server', None)
            http_vary = record.http_headers.get_header('Vary', None)
            http_cache_control = record.http_headers.get_header('Cache-Control', None)
            http_content_type = record.http_headers.get_header('Content-Type', None)
            http_date = record.http_headers.get_header('Date', None)
            http_expires = record.http_headers.get_header('Expires', None)

            data = record.content_stream().read()

            soup = BeautifulSoup(data, 'lxml')

            title = soup.find('title').string if soup.find('title') else None
            if not title:
                return
            paragraphs = soup.find_all(['p', 'b'])

            whitelist = ['meta', 'a', 'img']
            html_meta = []
            html_hyperlink = []
            html_image = []
            for t in soup.find_all(whitelist):
                if t.name == 'meta':
                    html_meta.append(str(t))
                elif t.name == 'a':
                    html_hyperlink.append(str(t))
                elif t.name == 'img':
                    html_image.append(str(t))

            body = ""
            for paragraph in paragraphs:
                if not paragraph.has_attr('class'):
                    body += paragraph.getText() + " "

            string_title = str(title)

            yield string_title, body, record_url, record_date, record_content_length, record_ip, \
                  record_truncated, http_server_name, http_vary, http_cache_control, http_content_type, \
                  http_date, http_expires, html_meta, html_hyperlink, html_image
        else:
            return


# dbutils.fs.rm('/user/hive/warehouse/subsetnews8', True)
job = StringMatchCountJob()
job.run()