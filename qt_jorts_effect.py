import pandas as pd
import tweepy

import datetime as dt
import pytz


bearer_token = 'YOUR BEARER TOKEN HERE'
auth = tweepy.OAuth2BearerHandler(bearer_token)
# some dreadful global scope to avoid passing `api` around to every function
api = tweepy.API(auth, wait_on_rate_limit=True)

def parse_cursor(c: int) -> dt.datetime:
    "credits to stefan mccabe; turns a Javascript cursor from Twitter into a Python unix timestamp"
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
    "turns a Python unix timestamp into a Javascript Twitter cursor"
    utc_ts = ts.astimezone(pytz.UTC)
    utc_ts_delta = utc_ts - dt.datetime(2007, 3, 9, 7, 51, 0, 0, pytz.UTC) # timedelta of e days
    utc_unix_days = utc_ts_delta.days
    a = 90595920000000
    b = 1230427978203430000
    d = utc_unix_days * a
    c = d + b

    return c

day_unix_nanoseconds = 24 * 60 * 60 * (10 ** 9)
def pull_followers_week_before_after(author, screen_name, ts, api):
    """
    Get all users who followed an account a week before/after a timestamp ts.
    So this will be all users in the 2 week time period centered at ts.
    """
    cursor = dt_to_cursor(ts)
    before = cursor - (7 * day_unix_nanoseconds)
    after = cursor + (7 * day_unix_nanoseconds)
    # cursor_now = api.get_follower_ids(user_id=author, screen_name=screen_name, cursor=-1)
    cursor_before = api.get_follower_ids(user_id=author, screen_name=screen_name, cursor=before)
    cursor_cursor = api.get_follower_ids(user_id=author, screen_name=screen_name, cursor=cursor)
    cursor_after = api.get_follower_ids(user_id=author, screen_name=screen_name, cursor=after)
    return cursor_before, cursor_cursor, cursor_after

