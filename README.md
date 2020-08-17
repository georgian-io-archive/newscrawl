Producing news articles in the form of dataframes, given a set of company names.

## Setup

Commands are already ordered so that they should be run chronologically. Load `sparkcc.ipynb` into databricks.

## Compatability and requirements

Databricks natively has Spark installed. Ensure that the cluster has ability to read/write to S3 if you plan to use it.

## Required data

There are a few files that must be set up properly in order to run the notebook properly. Samples will be provided. Make sure to edit the respective variables in the notebook if you plan to use a different file/path.

# Processing WARC file variables
* `seed_path` : List of domains that act as a whitelist. Only news articles that exist on a whitelisted domain will be saved in the resulting file. Example seed is found in `seeds.txt`. 
* `input_path`: List of WARC file paths. These Common Crawl S3 Bucket paths will be loaded and processed. Examples found in `./input`
* `output_path`: Name of the output file that you will be created.

# Producing company specific dataframes
* `seed_path` : List of domains that act as a whitelist. Only news articles that exist on a whitelisted domain will be saved in the resulting file. Example seed is found in `seeds.txt`.
* `company_feed`: By default, a CSV that stores names of the companies that you will be getting news articles for. With a different CSV format, or even file type the code can be easily rewritten. The key point is to ensure that the `list_of_companies` list is populated.
* `source_dataframe_path`: The input dataframe that you will be querying on (this should be the same as `output_path`).
* `output_path`: The directory that all of the company dataframes will be saved into.
