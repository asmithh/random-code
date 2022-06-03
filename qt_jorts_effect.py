import pandas as pd
import tweepy

import datetime as dt
import pytz

# The following code is meant to illustrate how a user can get time-bounded lists of follow events
# using the Twitter V1 API. 
# Note that rate limits for the following API make it difficult for this technique to scale.

bearer_token = 'YOUR BEARER TOKEN HERE'
auth = tweepy.OAuth2BearerHandler(bearer_token)
# some dreadful global scope to avoid passing `api` around to every function
api = tweepy.API(auth, wait_on_rate_limit=True)

def parse_cursor(c: int) -> dt.datetime:
    # Credits to Stefan Mccabe; turns a Javascript cursor from Twitter into a Python unix timestamp 
    # returns a datetime object
    # our inspiration can be found at the link below (yes, really)
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
    # turns a Python unix timestamp (datetime object) into a Javascript Twitter cursor
    # Again, credit to Stefan Mccabe for writing the code that does this.
    utc_ts = ts.astimezone(pytz.UTC)
    utc_ts_delta = utc_ts - dt.datetime(2007, 3, 9, 7, 51, 0, 0, pytz.UTC) # timedelta of e days
    utc_unix_days = utc_ts_delta.days
    a = 90595920000000
    b = 1230427978203430000
    d = utc_unix_days * a
    c = d + b

    return c

def find_list_alignment(past_list, future_list):
    # Given two lists that have some overlap point, return the index at which future_list overlaps into past_list.
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

day_unix_nanoseconds = 24 * 60 * 60 * (10 ** 9)
def pull_followers_week_before_after(author: int, screen_name: str, ts: datetime.datetime, api:tweepy.API) -> tuple:
    """
    Get all users who followed an account a week before/after a timestamp ts.
    In order to do this, we need:
    - the users who followed after "before" (7 days before the TS)
    - the users who followed after the cursor (at the TS) -- we will use these to eliminate overlap 
    - the users who followed after "after" (7 days after the TS). 
    
    """
    cursor = dt_to_cursor(ts)
    before = cursor - (7 * day_unix_nanoseconds)
    after = cursor + (7 * day_unix_nanoseconds)
    # cursor_now = api.get_follower_ids(user_id=author, screen_name=screen_name, cursor=-1)
    cursor_before = api.get_follower_ids(user_id=author, screen_name=screen_name, cursor=before)
    cursor_cursor = api.get_follower_ids(user_id=author, screen_name=screen_name, cursor=cursor)
    cursor_after = api.get_follower_ids(user_id=author, screen_name=screen_name, cursor=after)

    between_tweet_and_a_week_after = find_list_alignment(cursor_cursor[0], cursor_after[0])
    between_a_week_before_tweet_and_tweet = find_list_alignment(cursor_before[0], cursor_cursor[0])

    return cursor_before[:between_a_week_before_tweet_and_tweet], cursor_cursor[:between_tweet_and_a_week_after]
    return cursor_before, cursor_cursor, cursor_after

