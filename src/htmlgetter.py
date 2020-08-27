import urllib.request
import urllib.error
import codecs

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0",
}

# urlからhtmlのソースを取得する
def get_html(url):
    req = urllib.request.Request(url, None, headers=headers)

    try:
        request = urllib.request.urlopen(req)
    except:
        print("urlopen error")
        get_html(url)

    html = request.read()
    decoded = codecs.decode(html,encoding="utf-8", errors='strict')

    return decoded