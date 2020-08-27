Producing news articles in the form of dataframes, given a set of company names.
The goal of this project is to create a pipeline that can produce news articles from an arbitrary time period, for an arbitrary set of companies.
The data will be sourced from News Crawl. The notebook can be seen as 2 different parts. Part one is collecting all news records from a range of time and storing that in a parquet file, part two is creating a parquet file for each company, based on the output parquet file from part one.
 
# Setup

Commands are already ordered so that they should be run chronologically. Load `sparkcc.ipynb` into databricks.

# Compatability and requirements

Databricks natively has Spark installed. Ensure that the cluster has ability to read/write to S3 if you plan to use it.

# Required data

There are a few files that must be set up properly in order to run the notebook properly. Samples will be provided. Make sure to edit the respective variables in the notebook if you plan to use a different file/path.

## Processing WARC file variables
* `seed_path` : List of domains that act as a whitelist. Only news articles that exist on a whitelisted domain will be saved in the resulting file. Example seed is found in `seeds.txt`. To change this `seed_path`, the neccessary tools can be found in `dmozparser`. `content.rdf.u8` and `structure.rdf.u8.gz` must be downloaded from https://curlz.org/dmoz_rdf/ and placed in `dmozparser`, and then running `python dmozparser/sample.py` will produce a new seed text file in `dmozparser/seeds.txt`
* `input_path`: List of WARC file paths. These Common Crawl S3 Bucket paths will be loaded and processed. Examples found in `./input`
* `output_path`: Name of the output file that you will be created.

Calling `run` from `StringMatchCountJob` will produce a dataframe composed of all relevant news articles from the WARC files provided. An example dataframe (in the form of a csv file) 

## Producing company specific dataframes
* `seed_path` : List of domains that act as a whitelist. Only news articles that exist on a whitelisted domain will be saved in the resulting file. Example seed is found in `seeds.txt`.
* `company_feed`: By default, a CSV that stores names of the companies that you will be getting news articles for. With a different CSV format, or even file type the code can be easily rewritten. The key point is to ensure that the `list_of_companies` list is populated.
* `source_dataframe_path`: The input dataframe that you will be querying on (this should be the same as `output_path`).
* `output_path`: The directory that all of the company dataframes will be saved into.

# Quickstart

Loading the `sparkcc.ipynb` file into Databricks, as well as the neccessary files outlined above into DBFS is enough for it to run.
The current `sparkcc.ipynb` is ready to run, so starting each command sequentially will get the data for the companies found in `all_labelled_data.csv` for the current month. 

# Time and Space approximations

* Processing of N different WARC Files (where N is less than or equal to the number of workers running on your databricks cluster) should take a 1 - 4 hours
* Each company's specific dataframe size will vary based on how many times it appears in news articles. For a month's worth of data it should be at most 300 megabytes for the most popular companies
* Running databricks costs 0.86 dollars per hour for 1 machine of the i3.xlarge CPU.

Brief calculations: https://docs.google.com/document/d/1YR5ye1jIai9dHu7gzosU_iJqW5mNrgVtfXEwjGWAM0c/edit

# Next Step / Improvements

Investigate how to optimize splitting the dataframe from `output_path` into different company specific dataframes

# Note on Databricks

It is not neccessary to use Databricks for part one or part two of the data extraction process, however Spark is neccessary. More info on Databricks and Spark may be found: https://docs.google.com/document/d/1fCWwr-LnOuXHLsxdC4lwftraWVlGaNJj8Pg4xf58mNU/edit?ts=5efba57b#heading=h.s8sa0qj2g0yn

Existing datasets: https://docs.google.com/spreadsheets/d/16Z-1yfYS6kjieMYnUcJ8hr7eiZDPGmirjunP-c6qgNk/edit#gid=0
