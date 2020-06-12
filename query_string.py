import re

from collections import Counter

from pyspark.sql.types import StructType, StructField, StringType, LongType

from sparkcc import CCSparkJob

from bs4 import BeautifulSoup


class StringMatchCountJob(CCSparkJob):
    """ Word count (frequency list) from texts in Common Crawl WET files"""

    name = "StringMatchCount"


    output_schema = StructType([
        StructField("key", StringType(), True),

        StructField("val", StructType([
            StructField("body", StringType(), True),
            StructField("url", StringType(), True)]), True)
    ])

    # simple Unicode-aware tokenization
    # (not suitable for CJK languages)

    word_pattern = re.compile('\w+', re.UNICODE)


    def add_arguments(self, parser):
        parser.add_argument("--match_query", default=None,
                            help="match title/body with some string")
        parser.add_argument("--match_on", default=None,
                            help="choose to match on title or body")

    def process_record(self, record):
        if record.rec_type == 'response':
            url = record.rec_headers.get_header('WARC-Target-URI')
            data = record.content_stream().read()
            soup = BeautifulSoup(data,'lxml')

            title = soup.find('title').string if soup.find('title') else None
            if not title:
                return
            paragraphs = soup.find_all('p')
            body = ""
            for paragraph in paragraphs:
                if not paragraph.has_attr('class'):
                    body += paragraph.getText() + " "

            string_title = str(title)

            yield string_title, (body, url)
        else:
            return




if __name__ == '__main__':
    job = StringMatchCountJob()
    job.run()

