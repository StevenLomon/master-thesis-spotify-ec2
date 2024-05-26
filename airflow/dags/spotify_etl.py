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
    global_top_50_id

    r = requests.get(BASE_URL + f'playlists/{global_top_50_id}',
                    headers=headers,
                    params={'market': 'SE'})
    d = r.json()
    return d

def transform_raw_playlist_data(raw_playlist_data):
    refined_playlists = []
    for playlist in raw_playlist_data['playlists']['items']:
        refined_playlists.append({'id': playlist['id'], 'name': playlist['name'],
                                'description': playlist['description'],
                                'owner': playlist['owner']['display_name'],
                                'tracks': playlist['tracks']['total']})
    df = pd.json_normalize(refined_playlists)

    # Filter out rows where 'sleep' or 'noise' or 'peaceful' appears in the 'name' column (case-insensitive)
    pattern = r'sleep|noise|peaceful'
    df = df[~df['name'].str.contains(pattern, case=False)]

    return df

def extract_track_info(track_info, playlist_id, playlist_name, headers, session):
    artist_id = track_info['artists'][0]['id']
    album_id = track_info['album']['id']

    endpoints = {
        'track_info': f'tracks/{track_info["id"]}',
        'artist_info': f'artists/{artist_id}',
        'album_info': f'albums/{album_id}',
        'audio_features': f'audio-features/{track_info["id"]}'
    }

    responses = {}
    for key, endpoint in endpoints.items():
        r = session.get(BASE_URL + endpoint, params={'market': 'SE'} if 'track_info' in key else {})
        if r.status_code == 429:
            print("Rate limit hit")
            return None
        if r.status_code == 200:
            responses[key] = r.json()
        else:
            responses[key] = {}
            print(f"Error: {r.status_code} - {r.text}")

    return {
        'id': track_info['id'],
        'artist id': artist_id,
        'album id': album_id,
        'playlist id': playlist_id,
        'name': track_info['name'],
        'popularity': track_info['popularity'],
        'genres': responses['artist_info'].get('genres', []),  # Extract genres from the artist info
        'artist': track_info['artists'][0]['name'],
        'artist popularity': responses['artist_info'].get('popularity'),
        'album': responses['album_info'].get('name'),
        'album release date': responses['album_info'].get('release_date'),
        'album popularity': responses['album_info'].get('popularity'),
        'playlist': playlist_name,
        'danceability': responses['audio_features'].get('danceability'),
        'energy level': responses['audio_features'].get('energy'),
        'instrumentalness': responses['audio_features'].get('instrumentalness'),
        'liveness': responses['audio_features'].get('liveness'),
        'loudness': responses['audio_features'].get('loudness'),
        'speechiness': responses['audio_features'].get('speechiness'),
        'tempo': responses['audio_features'].get('tempo'),
        'duration_ms': responses['audio_features'].get('duration_ms'),
        'time signature': responses['audio_features'].get('time_signature')
    }

def get_refined_tracks(df, headers, num_rows=None, resume_from=None, save_interval=2, save_path=f'/home/ubuntu/refined_tracks.json'):
    refined_tracks = []

    try:
        refined_tracks = pd.read_json(save_path).to_dict('records')
        processed_playlists = set(track['playlist id'] for track in refined_tracks)
        print("Resuming from saved progress.")
    except (FileNotFoundError, ValueError) as e:
        print("Save file not found or empty. Starting from scratch.")
        processed_playlists = set()

    resume_from = resume_from or 0

    with requests.Session() as session:
        session.headers.update(headers)

        end_index = resume_from + (num_rows or len(df) - resume_from)
        for index, row in df.iloc[resume_from:end_index].iterrows():
            playlist_id = row['id']
            playlist_name = row['name']
            if playlist_id in processed_playlists:
                print(f"Skipping already processed playlist: {playlist_id}")
                continue

            print(f"Processing playlist {index}: {playlist_id}")

            r1 = session.get(BASE_URL + f'playlists/{playlist_id}/tracks', params={'market': 'SE'})
            if r1.status_code == 429:
                print("Rate limit hit")
                pd.DataFrame(refined_tracks).to_json(save_path, orient='records')
                return pd.json_normalize(refined_tracks)

            if r1.status_code == 200:
                d = r1.json()
                for track in d.get('items', []):
                    track_info = track.get('track')
                    if track_info:
                        refined_track = extract_track_info(track_info, playlist_id, playlist_name, headers, session)
                        if refined_track:
                            refined_tracks.append(refined_track)
            else:
                print(f"Error: {r1.status_code} - {r1.text}")

            # Save intermediate results
            if (index + 1) % save_interval == 0:
                pd.DataFrame(refined_tracks).to_json(save_path, orient='records')
                print(f"Saved progress after processing {index + 1} playlists")

    # Final save
    pd.DataFrame(refined_tracks).to_json(save_path, orient='records')
    return pd.json_normalize(refined_tracks)

def transform_data_final(clean_playlist_data):
    df = get_refined_tracks(clean_playlist_data, headers, num_rows=None)

    # Group by the unique track ID
    df_aggregated = df.groupby('id').agg({
        'artist id': 'first', # We can safely use 'first' since 'id' guarantees uniqueness
        'album id': 'first',
        'name': 'first',
        'popularity': 'first',
        'genres': 'first',
        'artist': 'first',
        'artist popularity': 'first',
        'album': 'first',
        'album release date': 'first',
        'album popularity': 'first',
        'playlist': lambda x:list(set(x)),  # Compile all playlist names into a list
        'danceability': 'mean',  # Average track features if there's variation
        'energy level': 'mean',
        'instrumentalness': 'mean',
        'liveness': 'mean',
        'loudness': 'mean',
        'speechiness': 'mean',
        'tempo': 'mean',
        'duration_ms': 'mean',
        'time signature': 'mean'
    }).reset_index()

    # Rename the aggregated playlist column for clarity
    df_aggregated.rename(columns={'playlist': 'playlist sources'}, inplace=True)

    # Column with occurance count
    df_aggregated['playlist occurrences'] = df_aggregated['playlist sources'].apply(len)

    return df_aggregated