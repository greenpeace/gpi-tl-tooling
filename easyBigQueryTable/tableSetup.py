"""A script to ease the interaction with BiqQuery to create tables and
datasets."""

import sys
import json
import logging
import argparse

from math import ceil

from google.cloud import bigquery
from google.cloud.bigquery import Table, Dataset, SchemaField

# arparse
parser = argparse.ArgumentParser()
parser.add_argument("dataset", type=str,
                    help="Specifies the dataset name.")
parser.add_argument("table", type=str,
                    help="Specifies the table name.")
parser.add_argument("schema", type=str,
                    help="Specify the table schema (field names) in json "
                         "format.")
parser.add_argument("data", type=str, nargs="?",
                    help="Path to data in json format.")
parser.add_argument("-c", "--creds", type=str,
                    help="Specify the path to BigQuery authentication file "
                         "if not stored in environment variable "
                         "GOOGLE_APPLICATION_CREDENTIALS.")
parser.add_argument("-s", "--silent", action="store_true",
                    help="No console output of current progress.")
args = parser.parse_args()

# constants
MAX_PAYLOAD = 10485760  # in bytes -> https://cloud.google.com/bigquery/quotas


# class definition of whole table schemata ------------------------------------
class Schema:
    """Helper class to create BigQuery table Schemas from a json file.

    Parameters
    ----------
    config : list
        `config` is the Python representation of a read json object. Its
        entries are dictionaries with (key, value)-pairs.

    Attributes
    ----------
    schema : list
        Upon initialisation, `schema` contains the processed json configuration
        as list of bigquery.SchemaField elements.

    """

    __slots__ = ["schema"]

    def __init__(self, *, config: list):
        """Initialize a new `Schema` instance from a JSON."""
        self.schema = []

        # TODO: check for nested fields + handling
        for sf in config:
            self.schema.append(SchemaField(**sf))


# helper function to determine size of data -----------------------------------
# credits to https://goshippo.com/blog/measure-real-size-any-python-object/
def get_size(obj, seen=None):
    """Recursively finds size of objects.

    Parameters
    ----------
    obj : dict-like, array-like
        `obj` is any Python object
    seen : set, None
        `seen` is needed for recursion to handle self-referential objects.

    Returns
    -------
    int
        Returns the aggregated size of `obj` in bytes.

    """
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes,
                                                           bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size


# logger setup ----------------------------------------------------------------
# create logger instance
logger = logging.getLogger("tableSetup")
logger.setLevel(logging.DEBUG)  # general logging level

if args.silent:
    logger.setLevel(logging.CRITICAL)

ch = logging.StreamHandler()  # channel -> console
ch.setLevel(logging.INFO)

# create formatter
formatter = logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t"
                              "%(message)s")
ch.setFormatter(formatter)

# add handlers to logger
logger.addHandler(ch)

# init ------------------------------------------------------------------------
ds = args.dataset
t = args.table

# table schema
with open(args.schema, "r") as j:
    jsonSchema = json.load(j)

schema = Schema(config=jsonSchema).schema  # list of SchemaField entriess

# credentials for client instance
if args.creds is None:
    client = bigquery.Client()
else:
    client = bigquery.Client(credentials=args.creds)

# references
ds_ref = client.dataset(ds)
t_ref = ds_ref.table(t)


try:
    dataset = client.get_dataset(ds_ref)
    logger.info("Found Dataset %s.", repr(ds))
    try:
        table = client.get_table(t_ref)
        logger.info("Found Table %s.", repr(t))

    except:  # TODO find teh right exception for this
        logger.info("Creating Table %s.", repr(t))
        table = Table(table_ref, schema=schema)
        table = client.create_table(table)

except:  # TODO find the right exception for this
    # create the dataset
    logger.info("Creating Dataset %s.", repr(ds))
    dataset = Dataset(ds_ref)
    dataset.location = "EU"
    dataset = client.create_dataset(dataset)
    # create a table
    logger.info("Creating Table %s.", repr(t))
    table = Table(t_ref, schema=schema)
    table = client.create_table(table)

# data ------------------------------------------------------------------------
if args.data is not None:
    logger.info("Loading data now...")
    with open(args.data, 'r') as f:
        data = json.load(f)

    size = get_size(data)  # determine size
    if size < MAX_PAYLOAD:
        logger.info("Data smaller than payload, directly inserting into "
                    "table.")
        client.insert_rows_json(t_ref, data)

    else:
        rows = len(data)
        chunks = ceil(1.1 * size/MAX_PAYLOAD)  # 10% chunks more for safety
        logger.info("Splitting data into %s chunks.", chunks)
        step = ceil(len(data)/chunks)  # get stepsize to iterate over chunks
        for i in range(chunks):
            logger.info("Working on chunk %s/%s...", i+1, chunks)
            end = (i+1) * step
            if end >= rows:  # last chunk, open ended slice
                client.insert_rows_json(t_ref, data[slice(i*step, None)])
            else:
                client.insert_rows_json(t_ref, data[slice(i*step, end)])
