


import argparse
import logging
import os
import re

from io import BytesIO
from tempfile import TemporaryFile

import boto3
import botocore
#from pyspark.python.pyspark.shell import spark

from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
import argparse
import spacy
import pandas as pd
from tqdm import tqdm

conf = SparkConf()
sc = SparkContext(
            appName="asdfasf",
            conf=conf)
sqlc = SQLContext(sparkContext=sc)
df = sqlc.read.parquet("spark-warehouse/stringmatchfulltest")
nlp = spacy.load("en_core_web_sm")
columns =  ['title','body','url','record_date', 'content_length',
            'warc_ip', 'warc_truncated', 'server_name', 'vary',
            'cache_control', 'content_type', 'http_date', 'http_date_expires',
            'html_meta', 'html_hyperlink', 'html_image', 'domain_match', 'title_match_string', 'body_match_string',
            'title_match_token', 'body_match_token', 'title_ner', 'body_ner' , 'matched_company']
rows = []

parser = argparse.ArgumentParser()
#parser.add_argument("query", help="the key word you want to look at")
parser.add_argument('--ner', dest='ner', action='store_true')
parser.add_argument('--no-ner', dest='ner', action='store_false')
parser.add_argument('--csv', dest='csv', action='store_true')
parser.add_argument('--no-csv', dest='csv', action='store_false')
parser.set_defaults(ner=True)
parser.set_defaults(csv=False)
args = parser.parse_args()


#submarkets = sqlc.read.format('csv').options(header='true', inferSchema='true').load('submarkets.csv')

list_of_domains = []
list_of_companies = []

with open("seeds.txt", "r") as f:
    for line in f:
        stripped_line = line.split()
        list_of_domains.append(stripped_line[0])

list_of_companies.append("Apple")
list_of_companies.append("Microsoft")

def perform_NER_title(title, query):
    doc = nlp(title)
    for ent in doc.ents:
        if ent.label_ == "ORG" and ent.text.lower() in query.lower():
            return True
    return False

def perform_NER_body(body, query):
    doc = nlp(body)
    for ent in doc.ents:
        if ent.label_ == "ORG" and ent.text.lower() in query.lower():
            return True
    return False

def domain_match(url):
    for n in list_of_domains:
        if n in url:
            return True
    return False

def token_match_title(title, query):
    title_tokens = title.split()
    for n in title_tokens:
        if n == query:
            return True
    return False

def token_match_body(body, query):
    body_tokens = body.split()
    for n in body_tokens:
        if n == query:
            return True
    return False

def string_match_title(title, query):
    if query in title:
        return True
    return False

def string_match_body(body, query):

    if query in body:
        return True
    return False

def performFilter(title, body, query):
    if string_match_body(body, query) or string_match_title(title, query): #body and title string match supercedes all
        return True
    return False

def company_match(query):
    return query


class CompanyQuery():

    def __init__(self):
        pass

    def performQuery(self):

        filter_udf = udf(performFilter, BooleanType())
        domain_match_udf = udf(domain_match, BooleanType())
        title_match_token_udf = udf(token_match_title, BooleanType())
        body_match_token_udf = udf(token_match_body, BooleanType())

        title_match_string_udf = udf(string_match_title, BooleanType())
        body_match_string_udf = udf(string_match_body, BooleanType())

        ner_title_udf = udf(perform_NER_title, BooleanType())
        ner_body_udf = udf(perform_NER_body, BooleanType())
        company_match_udf = udf(company_match, StringType())
        master_df = None
        lsts = []
        for n in list_of_companies:

            filtered_df = df.filter(filter_udf("title","body", lit(n) ))

            df_one = filtered_df.withColumn("domain_match", domain_match_udf("url"))
            df_one = df_one.withColumn("title_match_string", title_match_string_udf("title", lit(n)))
            df_one = df_one.withColumn("body_match_string", body_match_string_udf("body", lit(n)))
            df_two = df_one.withColumn("title_match_token", title_match_token_udf("title", lit(n)))
            df_three = df_two.withColumn("body_match_token", body_match_token_udf("body", lit(n)))
            df_four = df_three.withColumn("title_ner", ner_title_udf("title", lit(n)))
            df_five = df_four.withColumn("body_ner", ner_body_udf("body", lit(n)))
            df_last = df_five.withColumn("company_match", company_match_udf(lit(n)))

            if not master_df:
                master_df = df_last
            else:
                master_df = master_df.union(df_last)

        if args.csv:
            master_df.toPandas().to_csv('panda_dataframe.csv')
        else:
            for row in df.collect():
                print(row)




    def saveData(self):
        if args.csv:

            panda_df = pd.DataFrame(rows,columns=columns)
            panda_df.to_csv('panda_dataframe.csv')

temp = CompanyQuery()
temp.performQuery()
#temp.saveData()
