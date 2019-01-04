# tl-tooling
A collection of helpful scripts and tools.

## easyBigQueryTable
Create a `dataset` and a `table` in BigQuery, with given table `schema`. If specified, fill the table with `data`. 
```console
$ python tableSetup.py --help
usage: tableSetup.py [-h] [-c CREDS] [-s] dataset table schema [data]

positional arguments:
  dataset               Specifies the dataset name.
  table                 Specifies the table name.
  schema                Specify the table schema (field names) in json format.
  data                  Path to data in json format.

optional arguments:
  -h, --help            show this help message and exit
  -c CREDS, --creds CREDS
                        Specify the path to BigQuery authentication file if
                        not stored in environment variable
                        GOOGLE_APPLICATION_CREDENTIALS.
  -s, --silent          No console output of current progress.
```

Note: Tested for `Python >= 3.7`. Example:
```shell
$ python tableSetup.py finance historical_data example_schema.json example_data.json
```
