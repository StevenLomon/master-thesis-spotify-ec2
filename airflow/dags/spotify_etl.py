import json, requests, re, time, random, os, datetime
import boto3
import pandas as pd
from datetime import datetime
from botocore.exceptions import ClientError
from functions import get_secret, get_headers

client_secrets = get_secret()
CLIENT_ID = client_secrets['CLIENT_ID']
CLIENT_SECRET = client_secrets['CLIENT_SECRET']
headers = get_headers()
BASE_URL = 'https://api.spotify.com/v1/'

def extract_raw_playlist_data():
    # get the 50 most popular featured playlists in Sweden right now
    r = requests.get(BASE_URL + 'browse/featured-playlists',
                    headers=headers,
                    params={'q': 'top%20%20tracks', 'type': 'playlist',
                            'market': 'SE', 'limit': 50})
    d = r.json()

    # RAW DATA FOR THE 50 PLAYLISTS
    df = pd.DataFrame(d['playlists']['items'])
    return df