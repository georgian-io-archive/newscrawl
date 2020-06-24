


import argparse
import logging
import os
import re

from io import BytesIO
from tempfile import TemporaryFile

import boto3
import botocore

from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
import argparse
import spacy


parser = argparse.ArgumentParser()
parser.add_argument("query", help="the key word you want to look at")
parser.add_argument('--ner', dest='ner', action='store_true')
parser.add_argument('--no-ner', dest='ner', action='store_false')
parser.set_defaults(ner=True)
args = parser.parse_args()
conf = SparkConf()
sc = SparkContext(
            appName="asdfasf",
            conf=conf)
sqlc = SQLContext(sparkContext=sc)
df = sqlc.read.parquet("spark-warehouse/stringmatchfulltest")
nlp = spacy.load("en_core_web_sm")


def performNER(title, body):
    doc = nlp(title)
    for ent in doc.ents:
        if ent.label_ == "ORG" and ent.text.lower() == args.query.lower():
            print("org Found:", ent.text)
            return True
    return False

performNERudf = udf(performNER, BooleanType())
df_cols = df.columns
if args.ner:
    for row in df.filter(performNERudf("title", "body")).collect():
        print(row.title)
else:
    for row in df.filter(df.title.contains(args.query)).collect():
        print(row.title)

