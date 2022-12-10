
import requests
from urllib.parse import urlparse
from tld import get_tld
from GoogleNews import GoogleNews
import yaml
import pandas
import tweepy
from datetime import datetime
import pandas as pd
from typing import Tuple, List, Dict
from os.path import isfile

import logging

logging.basicConfig(
  filename="app_python.log",
  filemode='a',
  format='%(asctime)s %(message)s',
  level=logging.INFO
)


def get_keys(path="keys/twitterkeys.yaml"):
  """Get keys for twitter api

  Args:
      path (str, optional): path to read the info. Defaults to "keys/twitterkeys.yaml".

  Returns:
      _type_: dict of secrets and keys.
  """
  with open(path, 'r') as key_file:
    return yaml.safe_load(key_file)


def get_news_filter(yaml_file:str = 'news_filter.yaml') -> Dict:
  """Return a dict which includes infor on bad news sources.

  Args:
    yaml_file (str, optional): file to read. Defaults to 'news_filter.yaml'.

  Returns:
    Dict: dict with domains and extention.
  """
  with open(yaml_file) as f:
    return yaml.safe_load(f)


def skip_redirect(url: str) -> str:
  """ Returns destination url when given redirect url.

  Args:
    url (str): Potentially redirecting news.

  Returns:
    str: The target url.
  """
  return requests.get(url).url


def get_url_parts(url: str) -> Tuple[str, str]:
  """Get a url and return domain and extension.

  Args:
    url (str): url string

  Returns:
    Tuple[str, str]: a tuple of domain and extemtion
  """
  tldobj = get_tld(url, as_object=True)
  return tldobj.domain, tldobj.extension
  
  
def isbadnews(url:str) -> bool:
  """Return true when news source might be unreliable.

  Args:
    url (str): string of news url or website address.

  Returns:
    bool: if news is bad or not.
  """
  domain, ext = get_url_parts(url)
  news_filter_dict = get_news_filter()

  return True if (
    len([s for s in news_filter_dict['shit_news_sources'] if s in domain]) > 0 or
    ext in news_filter_dict['shit_news_extension']
  ) else False


def process_results(results: List[Dict]) -> pd.DataFrame:
  """Process dicts of news and convert the result into pandas DataFrame.

  Args:
      results (List[Dict]): list of dictionary item each belong to one news.

  Returns:
      pd.DataFrame: news processed to rows in one dataframe.
  """

  logging.info("Processing news started.")

  if results is None:
    return results
  else:
    results_dict_list = [] 
    for res in results:
      try:
        url = skip_redirect("https://" + res['link'])
        news_source, _ = get_url_parts(url)
        bad_flag = isbadnews()
        next_news = {
            'title':res['title'],
            'url': url,
            'datetime': res['datetime'],
            'retreived': datetime.now(),
            'source': news_source,
            'shitnews': bad_flag,
            'tweeted': 0,
          }
        results_dict_list.append(next_news)
      except:
        # It might get pulled on the next interation
        logging.info(f"URL for {'https://' + res['link']} not retreived.")
    return pd.DataFrame(results_dict_list)


def get_news(title = "Iran Protests", period='4h') -> pd.DataFrame:
  """Search for the news title and return a dataframe with processed news.

  Args:
      title (str, optional): news search string. Defaults to "Iran Protests".
      period (str, optional): how far back in time to search. Defaults to '4h'.

  Returns:
      pd.DataFrame: a dataframe with news url, source and tile.
  """
  logging.info("Pulling news started.")
  googlenews = GoogleNews()
  googlenews.set_lang('en')
  googlenews = GoogleNews(period=period)
  googlenews.set_encode('utf-8')
  googlenews.get_news(title)
  results = googlenews.results()
  news_df = process_results(results)
  return news_df


def create_tweet_text(text: str, hashtags: str, url: str) -> str:
  """Aggregate tweet text and hashtags and makes sure it won't reach 280 chars.

  Args:
      text (str): tweet text
      hashtags (str): tweet hashtag(s)
      url (str): the url to post with the tweet text

  Returns:
      str: the tweets text
  """
  cutoff = 280 - len(hashtags) - 5 # for buffer lets leave 5 out
  return text[:cutoff] + "\n" + hashtags + "\n" + url


def get_client(keys_path) -> tweepy.Client:
  """Use keys to retreive a teepy clients.

  Args:
      keys_path (_type_): path to keys' file

  Returns:
      tweepy.Client: tweepy client object
  """
  return tweepy.Client(**get_keys(path=keys_path))


def recycle_data(data_df: pd.DataFrame, datetime_col = 'datetime') -> pd.DataFrame:
  """Gets a dataframe and drops older rows based on a date time columns and return the rest.

  Args:
      data_df (pd.DataFrame): the dataframe to clean up
      datetime_col (str, optional): name of datetime columns. Defaults to 'datetime'.

  Returns:
      pd.DataFrame: cleaned data.
  """
  days_ago = datetime.now() - pd.Timedelta("3D")
  return data_df[data_df[datetime_col]>=days_ago]




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