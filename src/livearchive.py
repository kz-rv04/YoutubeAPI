import json
import codecs
import os
import sys
import time
import re
import bs4
from datetime import datetime as dt

import pandas as pd

import htmlgetter

CONTINUATION_URL_FORMAT = "https://www.youtube.com/live_chat_replay?continuation={continuation}"


# htmlファイルから目的のjsonファイルを取得する
def get_json(html):
    soup = bs4.BeautifulSoup(html, "lxml")

    json_dict = None
    for script in soup.find_all("script"):
        if 'window["ytInitialData"]' in str(script):
            #print(script.string)
            json_line = re.findall(r"window\[\"ytInitialData\"\] = (.*);", script.string)[0]
            #print(json_line)
            json_dict = json.loads(json_line)
    return json_dict

# 最初の動画のURLからcontinuationを引っ張ってくる
def get_initial_continuation(url):
    html = htmlgetter.get_html(url)

    json_dict = get_json(html)

    continuation = json_dict['contents']['twoColumnWatchNextResults']['conversationBar']['liveChatRenderer']['continuations'][0]['reloadContinuationData']['continuation']
#     print('InitialContinuation : ', continuation)
    return continuation

# htmlから抽出したjson形式の辞書からcontinuationの値を抜き出す
def get_continuation(json_dict):
    try:
        continuation = json_dict['continuationContents']['liveChatContinuation']['continuations'][0]['liveChatReplayContinuationData']['continuation']
#         print("NextContinuation: ", continuation)
    except KeyError:
        continuation = ""
        print("Continuation NotFound")
    return continuation

# コメントデータから文字列を取得する
def get_chat_text(actions):
    lines = []
    for item in actions:
        # ユーザーによるコメント要素のみ取得する
        try:
            line = dict()
            # ユーザー名やテキスト、アイコンなどのデータが入っている
            comment_data = item['replayChatItemAction']['actions'][0]['addChatItemAction']['item']['liveChatTextMessageRenderer']
            line['publishedAt'] = comment_data['timestampText']['simpleText']
            line['displayName'] = comment_data['authorName']['simpleText']
            line['displayMessage'] = comment_data['message']['runs'][0]['text']
            
            lines.append(line)
#             print(line)
        except KeyError:
            continue
    # 最後の行のコメントデータが次のcontinuationの最初のコメントデータを一致するため切り捨て
    if len(lines) > 1:
        del lines[len(lines) - 1]
    return lines

# 与えられたcontinuationから順次コメントを取得する
def get_livechat_replay(video_id, savepath='./', interval=1):
    if not os.path.exists(savepath):
        os.makedirs(savepath)
    path = os.path.join(savepath, video_id + '.tsv')
    
    # 保存するファイルを作成
    columns = ['publishedAt', 'displayName', 'displayMessage']
    df = pd.DataFrame([], columns=columns)
    df.to_csv(path, index=False, sep='\t')
    
    total = 0
    
    try:        
        url = "https://www.youtube.com/watch?v="+video_id
        # 生放送の録画ページから最初のcontinuationを取得する
        continuation = get_initial_continuation(url)
        while continuation:
            url = CONTINUATION_URL_FORMAT.format(continuation=continuation)
            html = htmlgetter.get_html(url)
            # コメントが格納されているjsonファイルをhtmlから抜き出し
            json_dict = get_json(html)
            #print(json_dict)
            # key:actions中に各ユーザーのコメントが格納されている
            actions = json_dict["continuationContents"]["liveChatContinuation"]["actions"]
            # 複数件のコメントをlist形式で取得
            comments = get_chat_text(actions)

            # 取得したコメントをファイルに保存
            df = pd.DataFrame(comments, columns=columns)
            df.to_csv(path, mode='a', index=False, header=False, sep='\t')

            total+= len(comments)
            print('{0} {1}: {2} comments, total {3}'.format(dt.now(), video_id, len(comments), total))
            # 次のcontinuationを取得する
            continuation = get_continuation(json_dict)
            
            time.sleep(interval)
    except KeyError:
        print('end')
        return

if __name__ == "__main__":
    args = sys.argv
    if len(args) > 1:
        video_id = args[1]

    get_livechat_replay(video_id=video_id, savepath="../data/comments")