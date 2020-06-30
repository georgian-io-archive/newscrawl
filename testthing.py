


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
import pandas as pd


conf = SparkConf()
sc = SparkContext(
            appName="asdfasf",
            conf=conf)
sqlc = SQLContext(sparkContext=sc)
df = sqlc.read.parquet("spark-warehouse/stringmatchfulltest")
nlp = spacy.load("en_core_web_sm")
columns = ['title','body','url','record_date', 'content_length',
                                 'warc_ip', 'warc_truncated', 'server_name', 'vary',
                                 'cache_control', 'content_type', 'http_date', 'http_date_expires',
                                 'html_meta', 'html_hyperlink', 'html_image']
rows = []


parser = argparse.ArgumentParser()
parser.add_argument("query", help="the key word you want to look at")
parser.add_argument('--ner', dest='ner', action='store_true')
parser.add_argument('--no-ner', dest='ner', action='store_false')
parser.add_argument('--csv', dest='csv', action='store_true')
parser.add_argument('--no-csv', dest='csv', action='store_false')
parser.set_defaults(ner=True)
parser.set_defaults(csv=False)
args = parser.parse_args()





def performNER(title, body):
    doc = nlp(title)
    for ent in doc.ents:
        if ent.label_ == "ORG" and ent.text.lower() in args.query.lower():
            print("org Found:", ent.text)
            return True
    return False

class CompanyQuery():

    def __init__(self):
        pass

    def performQuery(self):

        performNERudf = udf(performNER, BooleanType())

        if args.ner:
            for row in df.filter(performNERudf("title", "body")).collect():
                print(row.title)
                if args.csv:
                    rows.append([i for i in row])
        else:
            for row in df.filter(df.title.contains(args.query)).collect():
                print(row.title)
                if args.csv:
                    rows.append([i for i in row])

    def saveData(self):
        if args.csv:
            panda_df = pd.DataFrame(rows,columns=columns)
          #  print(panda_df)
            panda_df.to_csv('panda_dataframe.csv')
temp = CompanyQuery()
temp.performQuery()
temp.saveData()
