import os
import sys
import time
import requests
import pandas as pd
from datetime import datetime as dt

from pprint import pprint
from authorization import get_api_key

# video_idもしくはそのリストから配信時間やliveChatID,同時接続数などの情報を取得する
def get_livestreaming_details(API_KEY: str, video_id: str):
    '''
    Args:
        API_KEY: YoutubeDataAPI v3 API key 
        video_id: str or list of video_id 
    returns:
        list of liveStreamingDetails
    '''
    
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "key": API_KEY,
        "part": 'liveStreamingDetails',
        "id": video_id
    }
    res = requests.get(url, params=params).json()
    
    return res

# コメントデータから文字列を取得する
def get_live_chat(API_KEY, liveChatId, nextPageToken=None):
    url = "https://www.googleapis.com/youtube/v3/liveChat/messages"
    params = {
        "key": API_KEY,
        "liveChatId": liveChatId,
        "part": "authorDetails,snippet"
    }
    if nextPageToken:
        params['pageToken'] = nextPageToken
    res = requests.get(url, params=params).json()
    return res

# livechatのリストから必要なデータを取得する
def get_comment_data(livechat):
    comments = list()
    for c in livechat:
        row = dict()
        comment = c['snippet']
        author = c['authorDetails']
        # チャット日時　年～秒までを取得します
        row['publishedAt'] = dt.strptime(comment['publishedAt'], "%Y-%m-%dT%H:%M:%S.%fZ").strftime('%Y-%m-%d %H:%M:%S')        
        row['displayName'] = author['displayName']
        row['displayMessage'] = comment['displayMessage']
        row['isChatModerator'] = author['isChatModerator']
        comments.append(row)
    return comments

# # livechatのリストからコメントを全件取得する
def get_livechat_all(API_KEY, savepath, video_id, interval=60):
    # 保存先のファイルを初期化
    path = os.path.join(savepath, video_id+'.tsv')
    print('save path', savepath)
    print('interval', interval)
    columns = ['publishedAt', 'displayName', 'displayMessage', 'isChatModerator']
    df = pd.DataFrame([], columns=columns)
    df.to_csv(path, index=False, sep='\t')
    
    total = 0
    try:
        # ライブチャットIDを取得（配信中のみ）
        detail = get_livestreaming_details(API_KEY, video_id)
        activelivechatid = detail['items'][0]['liveStreamingDetails']['activeLiveChatId']
        nextPageToken = None
        while True:
            # ライブ配信のチャットを取得
            livechat = get_live_chat(API_KEY, activelivechatid, nextPageToken)
            comments = get_comment_data(livechat['items'])
            # 取得したコメントをファイルに保存
            df = pd.DataFrame(comments, columns=columns)
            df.to_csv(path, mode='a', index=False, header=False, sep='\t')
            
            total+= len(comments)
            print('{0} {1}: {2} comments, total {3}'.format(dt.now(), video_id, len(comments), total))
            
            nextPageToken = livechat['nextPageToken']
            time.sleep(interval)
    # 配信が終了すると終わり
    except KeyError:
        print('end')
        return
    return
   
# 一定時間間隔で同時接続数を取得します
def get_concurrent_viewers(API_KEY, savepath, video_id, interval=60):
    # 保存先のファイルを初期化
    path = os.path.join(savepath, video_id+'.tsv')
    print('save path', savepath)
    print('interval', interval)
    df = pd.DataFrame([], columns=['timestamp', 'concurrentViewers'])
    df.to_csv(path, index=False, sep='\t')
    
    # live終了もしくはプログラム強制終了までinterval秒間隔で取得し続ける
    while True:
        try:
            detail = get_livestreaming_details(API_KEY, video_id)
            row = [dt.now().strftime('%Y-%m-%d %H:%M:%S'),
                   detail['items'][0]['liveStreamingDetails']['concurrentViewers']
                  ]

            print(*row)
            df = pd.DataFrame([row], columns=['timestamp', 'concurrentViewers'])
            df.to_csv(path, mode='a', index=False, header=False, sep='\t')
        # 配信が終了するなどしてデータが取得できなくなったら終了
        except KeyError:
            print('end')
            return
        time.sleep(interval)

if __name__ == "__main__":
    args = sys.argv
    if len(args) > 1:
        video_id = args[1]
    API_KEY = get_api_key()
    
    get_concurrent_viewers(API_KEY, savepath='../data/concurrent_viewers/', video_id=video_id, interval=60)

    get_livechat_all(API_KEY, savepath='../data/comments/', video_id=video_id, interval=60)