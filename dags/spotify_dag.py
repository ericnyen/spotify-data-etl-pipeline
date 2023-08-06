from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import pandas as pd
import json
import boto3

def fetch_web_api(endpoint, method, body=None):
    url = f'https://api.spotify.com/{endpoint}'
    headers = {
        'Authorization': f'Bearer {TOKEN}'
    }

    if method == 'GET':
        response = requests.get(url, headers=headers)
    elif method == 'POST':
        headers['Content-Type'] = 'application/json'
        response = requests.post(url, headers=headers, data=json.dumps(body))
    else:
        raise ValueError(f'Unsupported HTTP method: {method}')

    response.raise_for_status()
    return response.json()


def get_top_tracks():
    endpoint = 'v1/me/top/tracks?time_range=short_term&limit=5'
    response = fetch_web_api(endpoint, 'GET')
    return response['items']


def get_recommendations(top_tracks_ids):
    endpoint = f'v1/recommendations?limit=5&seed_tracks={",".join(top_tracks_ids)}'
    response = fetch_web_api(endpoint, 'GET')
    return response['tracks']


def save_tracks_to_csv(tracks, filename):
    track_data = []
    for track in tracks:
        name = track['name']
        artists = ', '.join([artist['name'] for artist in track['artists']])
        track_data.append({'Name': name, 'Artists': artists})

    df = pd.DataFrame(track_data)
    s3_client = boto3.client('s3')
    bucket_name = 'spotify-data-airflow-bucket-2'
    s3_key = filename

    csv_buffer = df.to_csv(index=False)

    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer)

default_args = {
    'owner': 'ericnyen',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2),
}

with DAG(
    dag_id='aws_spotify_data_pipeline_v2',
    default_args=default_args,
    start_date = datetime(2023, 7, 18),
    schedule_interval='@daily',
    catchup = False
) as dag:
    def fetch_and_save_top_tracks():
        top_tracks = get_top_tracks()
        save_tracks_to_csv(top_tracks, 'top_tracks.csv')
        top_tracks_df = pd.DataFrame(top_tracks)
        print(top_tracks_df)

    def fetch_and_save_recommended_tracks():
        top_tracks = get_top_tracks()
        top_tracks_ids = [track['id'] for track in top_tracks]
        recommended_tracks = get_recommendations(top_tracks_ids)
        save_tracks_to_csv(recommended_tracks, 'recommended_tracks.csv')
        recommended_tracks_df = pd.DataFrame(recommended_tracks)
        print(recommended_tracks_df)

    fetch_and_save_top_tracks_task = PythonOperator(
        task_id='fetch_and_save_top_tracks',
        python_callable=fetch_and_save_top_tracks
    )

    fetch_and_save_recommended_tracks_task = PythonOperator(
        task_id='fetch_and_save_recommended_tracks',
        python_callable=fetch_and_save_recommended_tracks
    )

    fetch_and_save_top_tracks_task >> fetch_and_save_recommended_tracks_task
