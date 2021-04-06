import json
import os

import requests

BSE_NEWS_HOOK = os.getenv("BSE_NEWS_HOOK")
bearer_token = os.getenv("TWITTER_BEARER_TOKEN")
headers = {"Authorization": f"Bearer {bearer_token}"}
TWITTER_STREAM_API = {
    "RULES": "https://api.twitter.com/2/tweets/search/stream/rules",
    "STREAM": "https://api.twitter.com/2/tweets/search/stream"
}


def get_rules():
    response = requests.get(TWITTER_STREAM_API["RULES"], headers=headers)
    if response.status_code != 200:
        raise Exception(f"Cannot get rules (HTTP {response.status_code}): {response.text}")
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules=get_rules()):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(TWITTER_STREAM_API["RULES"], headers=headers, json=payload)
    if response.status_code != 200:
        raise Exception(f"Cannot delete rules (HTTP {response.status_code}): {response.text}")
    print(json.dumps(response.json()))


def set_rules():
    bse_rules = [{"value": "from:BSE_news", "tag": "bse news"}]
    payload = {"add": bse_rules}
    response = requests.post(TWITTER_STREAM_API["RULES"], headers=headers, json=payload)
    if response.status_code != 201:
        raise Exception(f"Cannot add rules (HTTP {response.status_code}): {response.text}")
    print(json.dumps(response.json()))


def get_stream():
    response = requests.get(TWITTER_STREAM_API["STREAM"], headers=headers, stream=True,
                            params={
                                "media.fields": "url",
                                "tweet.fields": "created_at,entities"
    })

    if response.status_code != 200:
        raise Exception(f"Cannot get stream (HTTP {response.status_code}): {response.text}")

    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            send_slack_msg(json_response.get('data'))


def get_payload(data):
    tweet_at = {
        "type": "section",
        "text": {"type": "mrkdwn", "text": f'*Tweeted At:* {data.get("created_at")}'},
    }
    tweet_body = {
        "type": "section",
        "text": {"type": "mrkdwn", "text": f'*Tweet:* {data.get("text")}\n------------------------------------------'},
    }
    payload = {"blocks": [tweet_at, tweet_body]}
    return json.dumps(payload)


def send_slack_msg(data):
    requests.post(
        url=BSE_NEWS_HOOK,
        headers={"Content-type": "application/json"},
        data=get_payload(data),
    )


def main():
    delete_all_rules()
    set_rules()
    get_stream()


if __name__ == "__main__":
    main()
