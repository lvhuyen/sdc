from __future__ import print_function

# This is an AWS Lambda function triggered by CRON events
# to clean up logging data indices (or any other type of index with a defined retention
# period). It does this by comparing the creation date against today's date, and if the
# difference is more than the retention period in days, the index is deleted.

# https://discuss.elastic.co/t/delete-indices-from-aws-lambda-based-on-creation-date/142333

import os
import json
import datetime
import boto3
from botocore.vendored import requests

# Get the endpoint from the env (set in the Lambda)
esEndPoint = os.environ["ES_ENDPOINT"]
# This string tells the ES api to get the indices and creation dates, return JSON, and sort by date
retrieveString = "/_cat/indices?h=index,creation.date.string&format=json&s=creation.date"
# Get the indices to exclude from deletion
indices_list_json = os.environ['INDICES_LIST']

# Convert a datetime string into a datetime.
def convertDate(s):
    return datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%fZ')

# Gets the current indices and their creation dates
def retrieveIndicesAndDates():
    try:
        theResult = requests.get(esEndPoint+retrieveString)
    except Exception as e:
        print("Unable to retrieve list of indices with creation dates.")
        print(e)
        exit(3)
    return theResult.content

def lambda_handler(event, context):
    # Load the list of indices
    theIndices = json.loads(retrieveIndicesAndDates())
    # For date comparison
    today = datetime.datetime.now()
    # Walk through the list of indices prefix to delete
    indices_list = json.loads(indices_list_json)
    deleted = []
    for idx_prefix, retention_days in indices_list.items():
        # Walk through the list
        for entry in theIndices:
            # Ignore the index that has the Kibana config
            if not entry["index"].startswith(idx_prefix):
                continue
            # Compare the creation date with today
            diff = today - convertDate(entry["creation.date.string"])
            # If the index was created more than retention_days ago, blow it away
            if diff.days > retention_days:
                print ("Deleting index %s" %(entry["index"]))
                theresult = requests.delete(esEndPoint+'/'+entry["index"])
                theresult.raise_for_status()
                deleted += [entry["index"]]
    print ("Job completed successfully with %s indices deleted." %len(deleted))
    return deleted