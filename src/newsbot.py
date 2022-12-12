
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



class NewsBot():
  def __init__(
    self,
    path_to_keys="keys/twitterkeys.yaml",
    path_to_filters='news_filter.yaml',
    path_to_news_data='news_data.csv',
    title="Iran Protests",
    period='4h'
    ) -> None:

    # create a logger
    logging.basicConfig(
        filename="NewsBot.log",
        filemode='a',
        format='%(asctime)s %(message)s',
        level=logging.INFO
    )
    self.logging = logging

    # file data params
    self.path_to_keys = path_to_keys
    self.path_to_filters = path_to_filters
    self.path_to_news_data = path_to_news_data

    # search params
    self.title=title
    self.period=period

    # recycing params
    self.datetime_col='datetime'
    self.days_to_keep='3D'
  
  #___________Woman_Life_Freedom____________#
  def _get_keys(self):
    """Get keys for twitter api"""
    with open(self.path_to_keys, 'r') as key_file:
      self.keys=yaml.safe_load(key_file)

  #___________Woman_Life_Freedom____________#
  def _get_news_filter(self) -> Dict:
    """Return a dict which includes infor on bad news sources."""
    with open(self.path_to_filters) as f:
      self.filters=yaml.safe_load(f)

  #___________Woman_Life_Freedom____________#
  def _create_client(self) -> tweepy.Client:
    """Use keys to retreive a teepy clients."""
    self.client=tweepy.Client(**self.keys)

  #___________Woman_Life_Freedom____________#
  @staticmethod
  def _skip_redirect(url: str) -> str:
    """ Returns destination url when given redirect url."""
    return requests.get(url).url
  
  #___________Woman_Life_Freedom____________#
  @staticmethod
  def _get_url_parts(url: str) -> Tuple[str, str]:
    """Get a url and return domain and extension."""
    tldobj = get_tld(url, as_object=True)
    return tldobj.domain, tldobj.extension
  
  #___________Woman_Life_Freedom____________#
  @staticmethod
  def _create_tweet_text(text:str, hashtag:str, url:str) -> str:
    """Aggregate tweet text and hashtag and makes sure it won't reach 280 chars."""
    cutoff = 280 - len(hashtag) - 5 # for buffer lets leave 5 out
    return text[:cutoff] + "\n" + hashtag + "\n" + url

  #___________Woman_Life_Freedom____________#
  def _isbadnews(self, url:str) -> bool:
    """Return true when news source might be unreliable."""
    domain, ext = self._get_url_parts(url)

    return True if (
      len([s for s in self.filters['shit_news_sources'] if s in domain]) > 0 or
      ext in self.filters['shit_news_extension']
    ) else False

  #___________Woman_Life_Freedom____________#
  @staticmethod
  def _recycle_data(data_df, datetime_col, days_to_keep) -> pd.DataFrame:
    """Gets a dataframe and drops older rows based on a date time columns and return the rest."""
    days_ago = datetime.now() - pd.Timedelta(days_to_keep)
    return data_df[data_df[datetime_col]>=days_ago]
  
  #___________Woman_Life_Freedom____________#
  def _get_news(self) -> pd.DataFrame:
    """Search for the news title and return a dataframe with processed news."""
    self.logging.info("Pulling news started.")
    googlenews = GoogleNews()
    googlenews.set_lang('en')
    googlenews = GoogleNews(period=self.period)
    googlenews.set_encode('utf-8')
    googlenews.get_news(self.title)
    self.results = googlenews.results()

  #___________Woman_Life_Freedom____________#
  def _process_results(self):
    """Process dicts of news and convert the result into pandas DataFrame."""
    self.logging.info("Processing news started.")
    results_dict_list = [] 
    for res in self.results:
      try:
        url = self._skip_redirect("https://" + res['link'])
      except:
        self.logging.info(f"URL for {'https://' + res['link']} not retreived.")
        continue
      news_source, _ = self._get_url_parts(url)
      bad_flag = self._isbadnews(url)
      title = res['title']
      if not ('iran' in title.lower()):
        continue
      next_news = {
          'title':title,
          'url': url,
          'datetime': res['datetime'],
          'retreived': datetime.now(),
          'source': news_source,
          'bad_flag': bad_flag,
          'tweeted': 0,
        }
      results_dict_list.append(next_news)
    self.results_df = pd.DataFrame(results_dict_list)

  #___________Woman_Life_Freedom____________#
  def _update_data(self):
    if isfile(self.path_to_news_data):
      file_data = pd.read_csv(self.path_to_news_data, parse_dates = ['datetime', 'retreived'])
      data_new = pd.concat([self.results_df.copy(), file_data], axis=0)
      
      #make sure tweeted column stays updated
      data_new['tweeted'] = data_new.groupby('url')['tweeted'].transform(lambda x: max(x))
      data_new = data_new.drop_duplicates(subset=['url'])
      data_new = self._recycle_data(data_new, self.datetime_col, self.days_to_keep)
      data_new.to_csv(self.path_to_news_data, index=False)
      self.results_df = data_new.sort_values(['datetime'], ascending=False)
    else:
      self.results_df.sort_values(['datetime'], ascending=False, inplace=True)
      self.results_df.to_csv(self.path_to_news_data, index=False)

  #___________Woman_Life_Freedom____________#
  def _pop_news(self):
    """Get the most recent news condition that it's not tweeted or badnews."""
    df = self.  results_df
    news_tile, news_url, news_source = df.query("tweeted == 0").query("bad_flag == 0")[
      ['title', 'url', 'source']
      ].head(1).values[0]
    df.loc[df['url'] == news_url, 'tweeted'] = 1
    return news_tile, news_url, news_source

  #___________Woman_Life_Freedom____________#
  def tweet(self, mock=False):
    news_tile, news_url, news_source = self._pop_news()
    news_text = self._create_tweet_text(
      text=news_tile,
      hashtag=news_source,
      url=news_url
    )

    if mock:
      return news_text
    else:
      self._create_client()
      self.client.create_tweet(text=news_text)
      self.logging.info(news_text)
      return news_text
  
  #___________Woman_Life_Freedom____________#
  def collect_news(self):
    self._get_keys()
    self._get_news_filter()
    self._get_news()
    self._process_results()
    self._update_data()
    tweet = self.tweet(mock=False)
    return tweet

  






  
  