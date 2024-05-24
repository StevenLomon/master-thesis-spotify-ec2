import json, requests, re, time, random, os, datetime
import boto3
import pandas as pd
from datetime import datetime
from botocore.exceptions import ClientError

def get_secret():

    secret_name = "Spotify/Client_secrets"
    region_name = "eu-north-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name='eu-north-1'
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = json.loads(get_secret_value_response['SecretString'])

    return secret

client_secrets = get_secret()
CLIENT_ID = client_secrets['CLIENT_ID']
CLIENT_SECRET = client_secrets['CLIENT_SECRET']
print(CLIENT_ID)
print(CLIENT_SECRET)
