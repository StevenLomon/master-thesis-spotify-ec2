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
    # get raw data for the 50 songs in the Spotify Global Top 50 playlist
    q = requests.get(BASE_URL + 'search',
                    headers=headers,
                    params={'q': 'global%2520top%252050', 'type': 'playlist',
                            'market': 'SE', 'limit': 1})
    d = q.json()
    global_top_50_id = d['playlists']['items'][0]['id']

    r = requests.get(BASE_URL + f'playlists/{global_top_50_id}',
                    headers=headers,
                    params={'market': 'SE'})
    d = r.json()
    return d

def transform_raw_playlist_data(raw_playlist_df):
    df = raw_playlist_df
    df['id'] = df['track'].apply(lambda x: x['id'])
    df['name'] = df['track'].apply(lambda x: x['name'])
    df['popularity'] = df['track'].apply(lambda x: x['popularity'])
    df['explicit'] = df['track'].apply(lambda x: x['explicit'])
    df['duration_ms'] = df['track'].apply(lambda x: x['duration_ms'])
    df['external_url'] = df['track'].apply(lambda x: x['external_urls']['spotify'])
    df['artist'] = df['track'].apply(lambda x: x['artists'][0]['name'])
    df['artist_id'] = df['track'].apply(lambda x: x['artists'][0]['id'])
    df['album'] = df['track'].apply(lambda x: x['album']['name'])
    df['album_id'] = df['track'].apply(lambda x: x['album']['id'])
    df['album_release_date'] = df['track'].apply(lambda x: x['album']['release_date'])
    df_cleaned = df.drop(columns=['added_at', 'is_local', 'primary_color', 'added_by', 'track', 'video_thumbnail'])
    return df_cleaned