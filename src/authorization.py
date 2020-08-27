import json

def get_api_key():
    with open('../secret.json', mode="r") as f:
        keys = json.load(f)
    return keys['API_KEY']