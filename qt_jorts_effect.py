import pandas as pd
import tweepy

import datetime as dt
import pytz

jorts_tweets = pd.read_csv('./jorts_all_tweets.tsv', sep='\t')

qt_authors = jorts_tweets[jorts_tweets['quoted'] != 'None'][['quoted_author_id', 'created_at', 'quoted_handle']]
qt_authors = qt_authors[qt_authors['created_at'] != 'None']

##############IMPORTANT################
# qt_authors = qt_authors[qt_authors.index % 2 == 0] # zhen use this line
# qt_authors = qt_authors[qt_authors.index % 2 == 1] # mohit use this line
qt_authors['created_at_ts'] = pd.to_datetime(qt_authors['created_at'])
qt_authors = qt_authors.groupby('quoted_handle').min()
qt_authors['quoted_handle'] = qt_authors.index

bearer_token = 'YOUR BEARER TOKEN HERE'
auth = tweepy.OAuth2BearerHandler(bearer_token)
# some dreadful global scope to avoid passing `api` around to every function
api = tweepy.API(auth, wait_on_rate_limit=True)

def parse_cursor(c: int) -> dt.datetime:
    # https://popzazzle.blogspot.com/2019/11/how-to-find-out-when-someone-followed-you-on-twitter.html
    if c == -1:
        return dt.datetime.now()
    if c == 0:
        return dt.datetime(2006, 1, 1, 0, 0, 0 ,0)
    if c < -2:
        c = -c
    a = 90595920000000
    b = 1230427978203430000
    d = c - b
    e = d / a
    # to get d, we need to multiply by a.
    f = dt.datetime(2007, 3, 9, 7, 51, 0, 0)
    return f + dt.timedelta(days=e) # subtract f from the unix timestamp;
    # we now have a dt.timedelta of e days.

def dt_to_cursor(ts: dt.datetime) -> int:
    utc_ts = ts.astimezone(pytz.UTC)
    utc_ts_delta = utc_ts - dt.datetime(2007, 3, 9, 7, 51, 0, 0, pytz.UTC) # timedelta of e days
    utc_unix_days = utc_ts_delta.days
    a = 90595920000000
    b = 1230427978203430000
    d = utc_unix_days * a
    c = d + b

    return c

day_unix_nanoseconds = 24 * 60 * 60 * (10 ** 9)
def jorts_effect(author, screen_name, ts, api):
    cursor = dt_to_cursor(ts)
    before = cursor - (7 * day_unix_nanoseconds)
    after = cursor + (7 * day_unix_nanoseconds)
    # cursor_now = api.get_follower_ids(user_id=author, screen_name=screen_name, cursor=-1)
    cursor_before = api.get_follower_ids(user_id=author, screen_name=screen_name, cursor=before)
    cursor_cursor = api.get_follower_ids(user_id=author, screen_name=screen_name, cursor=cursor)
    cursor_after = api.get_follower_ids(user_id=author, screen_name=screen_name, cursor=after)
    return cursor_before, cursor_cursor, cursor_after
# cursor_now, cursor_before, cursor_cursor, cursor_after = jorts_effect(48921931, 'mauraloran', ts, api)

def find_list_alignment(past_list, future_list):
    idx = 0
    if len(past_list) == 0:
        if len(future_list) == 0:
            return 0
        else:
            return len(future_list)
    elif len(future_list) == 0:
        return 0
    while idx < len(future_list) and past_list[0] != future_list[idx]:
        idx += 1
    # each index increasing in future_list goes further into the past.
    # when we have gone far enough into future_list, we'll align with past_list.
    # the number of items we've traveled into future_list's past
    # is the number of items that have occurred after past_list's boundary
    # but before future_list's boundary.
    return idx

with open('jorts_qt_effect.csv', 'a') as f:
    for d in qt_authors.to_dict(orient="records"):
        try:
            cursor_before, cursor_cursor, cursor_after = jorts_effect(
                d['quoted_author_id'], d['quoted_handle'], d['created_at_ts'], api)
        except Exception as e:
            print(e)
            print(d['quoted_handle'])
            continue
        between_tweet_and_a_week_after = find_list_alignment(cursor_cursor[0], cursor_after[0])
        between_a_week_before_tweet_and_tweet = find_list_alignment(cursor_before[0], cursor_cursor[0])
        res = [
            d['quoted_author_id'],
            d['quoted_handle'],
            d['created_at'],
            between_tweet_and_a_week_after,
            between_a_week_before_tweet_and_tweet,
        ]
        print(','.join([str(r) for r in res]))
        f.write(','.join([str(r) for r in res]))
        f.write('\n')
