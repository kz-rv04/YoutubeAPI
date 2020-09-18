import os
import sys
import time
import requests
import pandas as pd
from datetime import datetime as dt

from pprint import pprint
from authorization import get_api_key

API_KEY = get_api_key()


def get_video_categories():
    url = "https://www.googleapis.com/youtube/v3/videoCategories"
    params = {
        "key": API_KEY,
        "part": 'snippet',
        "regionCode": 'JP',
    }
    res = requests.get(url, params=params).json()
    return res

# queryやカテゴリ名で生放送を検索します
def search_livestreaming(query="", category_id=10, order='viewCount', pageToken=''):
    '''
    Args:
        API_KEY: YoutubeDataAPI v3 API key 
        video_id: str or list of video_id 
    returns:
        list of liveStreamingDetails
    '''
    
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "key": API_KEY,
        "part": 'snippet',
        "eventType": 'live',
        "type": 'video',
        "maxResults": 500,
        "regionCode": 'JP',
        "language": 'ja',
        "q": query,
        "order": order,
        "videoCategoryId": category_id,
        'pageToken': pageToken
    }
    res = requests.get(url, params=params).json()
    return res
