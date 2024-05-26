import json, requests, re, time, random, os, datetime
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

def transform_raw_playlist_data(raw_playlist_data):
    # The following didn't work for some reason????
    # # Helper function to safely extract values from the track dictionary
    # def get_track_value(track, key):
    #     if isinstance(track, dict):
    #         return track.get(key, None)
    #     return None
    
    # # Extract values with safety checks
    # raw_playlist_df['id'] = raw_playlist_df['track'].apply(lambda x: get_track_value(x, 'id'))
    # raw_playlist_df['name'] = raw_playlist_df['track'].apply(lambda x: get_track_value(x, 'name'))
    # raw_playlist_df['popularity'] = raw_playlist_df['track'].apply(lambda x: get_track_value(x, 'popularity'))
    # raw_playlist_df['explicit'] = raw_playlist_df['track'].apply(lambda x: get_track_value(x, 'explicit'))
    # raw_playlist_df['duration_ms'] = raw_playlist_df['track'].apply(lambda x: get_track_value(x, 'duration_ms'))
    # raw_playlist_df['external_url'] = raw_playlist_df['track'].apply(lambda x: get_track_value(x.get('external_urls', {}), 'spotify') if isinstance(x, dict) else None)
    # raw_playlist_df['artist'] = raw_playlist_df['track'].apply(lambda x: x['artists'][0]['name'] if isinstance(x, dict) and 'artists' in x and len(x['artists']) > 0 else None)
    # raw_playlist_df['artist_id'] = raw_playlist_df['track'].apply(lambda x: x['artists'][0]['id'] if isinstance(x, dict) and 'artists' in x and len(x['artists']) > 0 else None)
    # raw_playlist_df['album'] = raw_playlist_df['track'].apply(lambda x: get_track_value(x.get('album', {}), 'name') if isinstance(x, dict) else None)
    # raw_playlist_df['album_id'] = raw_playlist_df['track'].apply(lambda x: get_track_value(x.get('album', {}), 'id') if isinstance(x, dict) else None)
    # raw_playlist_df['album_release_date'] = raw_playlist_df['track'].apply(lambda x: get_track_value(x.get('album', {}), 'release_date') if isinstance(x, dict) else None)

    # # Drop the original nested columns
    # df_cleaned = raw_playlist_df.drop(columns=['added_at', 'is_local', 'primary_color', 'added_by', 'track', 'video_thumbnail'])

    # return df_cleaned

    refined_playlist_tracks = []
    for item in raw_playlist_data['tracks']['items']:
        track_data = item['track']
        refined_playlist_tracks.append({'id': track_data['id'], 'name': track_data['name'],
                                'popularity': track_data['popularity'], 'explicit': track_data['explicit'],
                                'duration_ms': track_data['duration_ms'], 'external_url': track_data['external_urls']['spotify'],
                                'artist': track_data['artists'][0]['name'], 'artist_id': track_data['artists'][0]['id'],
                                'album': track_data['album']['name'], 'album_id': track_data['album']['id'],
                                'album_release_date': track_data['album']['release_date'],})
    
    df = pd.json_normalize(refined_playlist_tracks)
    return df