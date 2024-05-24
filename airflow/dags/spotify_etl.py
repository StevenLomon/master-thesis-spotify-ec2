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

AUTH_URL = 'https://accounts.spotify.com/api/token'

auth_response = requests.post(AUTH_URL, {
    'grant_type': 'client_credentials',
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
})

# convert the response to JSON
auth_response_data = auth_response.json()

# save the access token
access_token = auth_response_data['access_token']

headers = {
    'Authorization': 'Bearer {token}'.format(token=access_token)
}

BASE_URL = 'https://api.spotify.com/v1/'

# get the 50 most popular featured playlists in Sweden right now
r = requests.get(BASE_URL + 'browse/featured-playlists',
                 headers=headers,
                 params={'q': 'top%20%20tracks', 'type': 'playlist',
                         'market': 'SE', 'limit': 50})
d = r.json()

# RAW DATA FOR THE 50 PLAYLISTS
pd.DataFrame(d).to_csv("spotify_playlists_raw.csv", index=False)