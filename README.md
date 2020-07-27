Running job on WARC files to produce parquet of a certain time span of crawled data:

- Populate some text file (putting it in input directory), specifying which crawls one would like to take records from
- replace input_path and output_path parameters in cmd 3 of sparkcc.ipynb
- Run on databricks

Processing 1 parquet file into multiple company parquet files
- Gather a list of companies, storing that as a list (this will be list_of_companies in command 6)
- Modify the output path (tmp_path variable)
- Run on databricks
