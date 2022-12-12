from src.newsbot import NewsBot

newsbot = NewsBot(
    path_to_keys="keys/twitterkeys.yaml", 
    path_to_filters="src/news_filter.yaml",
    )

newsbot = NewsBot(
    path_to_keys="keys/twitterkeys.yaml",
    path_to_filters="src/news_filter.yaml",
    )

tweet_text = newsbot.collect_news()

print(tweet_text)

newsbot.tweet(mock=True)