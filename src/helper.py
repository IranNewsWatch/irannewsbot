
import requests
from urllib.parse import urlparse
from GoogleNews import GoogleNews
import yaml
import pandas
import tweepy
from datetime import datetime
import pandas as pd

from os.path import isfile

import logging

logging.basicConfig(filename="app_python.log",
                    filemode='a',
                    format='%(asctime)s %(message)s', level=logging.INFO)


def skip_redirect(uri: str) -> str:
    """ Returns destination URI when given redirect URI."""
    return requests.get(uri).url

def guess_news_source(uri):   
    """Uses URL to extract website main domain and returns it as news source."""
    domain = urlparse(uri).netloc
    return domain.split('.')[-2]

def create_tweet_text(text: str, hashtags: str, url: str):
    """Aggregate tweet text and hashtags."""
    cutoff = 280 - len(hashtags) - 5 # for buffer lets leave 5 out
    return text[:cutoff] + "\n" + hashtags + "\n" + url

def get_keys(path="keys/twitterkeys.yaml"):
    with open(path, 'r') as key_file:
        return yaml.safe_load(key_file)

def get_client(keys_path):
    return tweepy.Client(**get_keys(path=keys_path))

def get_news(title = "Iran Protests"):
    googlenews = GoogleNews()
    googlenews.set_lang('en')
    #googlenews.set_time_range('12/01/2022','12/02/2022')
    googlenews = GoogleNews(period='4h')
    googlenews.set_encode('utf-8')
    googlenews.get_news('Iran Revolution')

    results = googlenews.results()
    if results is None:
        results
    else:
        results_dict_list = [] 
        for res in results:
            try:
                url = skip_redirect("https://" + res['link'])
                next_news = {
                        'title':res['title'],
                        'url': url,
                        'datetime': res['datetime'],
                        'retreived': datetime.now(),
                        'tweeted': 0,
                    }
                results_dict_list.append(next_news)
            except:
                logging.info(f"URL for {'https://' + res['link']} not retreived.")
        return pd.DataFrame(results_dict_list)

def recycle_data(data_new):
    yesterday = datetime.now() - pd.Timedelta("1D")
    return data_new[data_new['datetime']>=yesterday]

def update_data(data, filename):
    if isfile(filename):
        file_data = pd.read_csv(filename, parse_dates = ['datetime', 'retreived'])
        data_new = pd.concat([data.copy(), file_data], axis=0)
        
        #make sure tweeted column stays updated
        data_new['tweeted'] = data_new.groupby('url')['tweeted'].transform(lambda x: max(x))

        data_new = data_new.drop_duplicates(subset=['url'])
        data_new = recycle_data(data_new)
        data_new.to_csv(filename, index=False)
    else:
        data.to_csv(filename, index=False)
        data_new = data.copy()
    return data_new.sort_values(['datetime'], ascending=False)

    
def tweet(text, url, hashtags="#IranRevolution", key_folder=""):
    news_source = guess_news_source(url).upper()
    # hashtags = hashtags + " #" + news_source
    hashtags = "#"+news_source
    text = f"{text}\n{hashtags}\n{url}"
    client = get_client(keys_path=key_folder+"twitterkeys.yaml")
    client.create_tweet(text=text)
    return text