import re

from collections import Counter

from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

from sparkcc import CCSparkJob
#from databricks_sparkcc import JupyterCCSparkJob
from bs4 import BeautifulSoup
import htmlmin


class StringMatchCountJob(CCSparkJob):
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



            yield string_title, body, record_url, record_date, record_content_length, record_ip,\
                  record_truncated, http_server_name, http_vary, http_cache_control, http_content_type, \
                  http_date, http_expires, html_meta, html_hyperlink, html_image
        else:
            return

job = StringMatchCountJob()
job.run()
