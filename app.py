from src import helper
import logging

import os

logging.basicConfig(filename="app_python.log",
                    filemode='a',
                    format='%(asctime)s %(message)s', level=logging.INFO)
logging.info(f'Logging started from {os.getcwd()}.')


import tempfile
temp_folder = tempfile.gettempdir()

import os
home_folder = os.getenv("HOME")
data_log= temp_folder + '/news_log.csv'


logging.info("Reading news...")
news_df =  helper.get_news()
logging.info(f"Updating data in {data_log}")
data_new = helper.update_data(news_df, data_log)

#drop if no Iran in the title
logging.info("Applying filters on newslist.")
data_new.loc[~data_new['url'].fillna("").apply(lambda x: str(x).lower().replace(".","")).str.contains("iran"), 'tweeted'] = -1

news_tile, news_url = data_new.query("tweeted==0")[['title', 'url']].head(1).values[0]

data_new.loc[data_new['url'] == news_url, 'tweeted'] = 1

text = helper.tweet(
        text=news_tile,
        url=news_url,
        key_folder=home_folder + "/Documents/keys/",
    )

logging.info(f"TWEET: {text}")

_ = helper.update_data(data_new, data_log)
logging.info(f"Updated data. Done!")  


