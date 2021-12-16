import requests
import json
import config


Bearer_Token = config.Bearer_Token
Twitter_Username = config.Twiiter_Username


def url_endpoint():
  tweet_fields = 'tweet.fields=public_metrics,created_at'
  url = f'https://api.twitter.com/2/tweets/search/recent?max_results=100&query=from:{Twitter_Username}&{tweet_fields}'

  return url

def bearer_oauth(r):
  """
  Method for bearer token authentication.
  """
  r.headers['Authorization'] = f'Bearer {Bearer_Token}'

  
  return r

def get_data(url):
  response = requests.request('GET', url, auth=bearer_oauth)
  if response.status_code != 200:
    raise Exception(f"Status Code: {response.status_code}, Error Message: {response.text}")
  return response.json()

def main():
  url = url_endpoint()
  json_response = get_data(url)
  json_data = json.dumps(json_response, indent=4, sort_keys=True)
  print(json_data)

main()

"""
Sample Output:

{
    "data": [
        {
            "created_at": "2021-12-16T02:13:14.000Z",
            "id": "1471302367454900230",
            "public_metrics": {
                "like_count": 1,
                "quote_count": 0,
                "reply_count": 0,
                "retweet_count": 0
            },
            "text": "I think am now more of a Python guy than JavaScript."
        },
        {
            "created_at": "2021-12-13T09:58:16.000Z",
            "id": "1470332232057425920",
            "public_metrics": {
                "like_count": 0,
                "quote_count": 0,
                "reply_count": 0,
                "retweet_count": 1
            },
            "text": "It's been a while I've worked on a project. I hope to get my rythm back soon."
        },
        {
            "created_at": "2021-12-12T14:41:45.000Z",
            "id": "1470041187147038727",
            "public_metrics": {
                "like_count": 1,
                "quote_count": 0,
                "reply_count": 0,
                "retweet_count": 0
            },
            "text": "@Ashot_ That's an unexplainable or mysterious love she has for you, if she loves you."
        },
        {
            "created_at": "2021-12-11T20:35:16.000Z",
            "id": "1469767762746744833",
            "public_metrics": {
                "like_count": 0,
                "quote_count": 0,
                "reply_count": 0,
                "retweet_count": 0
            },
            "text": "@MrPepz Thanks"
        },
        {
            "created_at": "2021-12-11T20:12:12.000Z",
            "id": "1469761957750321152",
            "public_metrics": {
                "like_count": 9,
                "quote_count": 0,
                "reply_count": 3,
                "retweet_count": 1
            },
            "text": "I got a Big Data role but still don't know what to expect as my first day approaches."
        },
        {
            "created_at": "2021-12-10T17:25:36.000Z",
            "id": "1469357644615925776",
            "public_metrics": {
                "like_count": 2,
                "quote_count": 0,
                "reply_count": 0,
                "retweet_count": 0
            },
            "text": "@jamesqquick It's OOP, especially using this keyword in JavaScript class."
        }
    ],
    "meta": {
        "newest_id": "1471302367454900230",
        "oldest_id": "1469357644615925776",
        "result_count": 6
    }
}
"""
