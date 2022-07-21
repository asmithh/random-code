import pandas as pd
import pickle
import tweepy
import time
import requests
import datetime as dt
import pytz

# when all the blocks are uncommented, this gets all Jorts' tweets,
# finds all the authors Jorts RT'ed or QT'ed, and gets all their followers gained in the 2 weeks following the Jorts RT.
# note that to get the before/after RT comparison, you'll have to do 3 collections on the amplified authors:
# 1 between t=0 and t=-2 weeks; 1 between t=+2 weeks and t=0, and 1 between t=+2 weeks and t=+2 weeks + epsilon.
# when you find the overlap between period A and period B, you know when the tweets that were pre-amplification STOP,
# (i.e. when the t=0 tweet is in the list of followers from -2 weeks to some unspecified time)
# so you know which tweets were pre-amplification. this is because the API scrolls BACKWARD in time. 
# when you have the overlap between period B and period C, you know when the tweets that were post-amplification STOP. 
# (i.e. when the t=2 weeks tweet is in the list of followers from t=0 to t=+2 weeks)
# with these markers, you can reconstruct all the followers 2 weeks before & after a disruptive event.
# NOTE THAT YOU WILL HAVE TO RUN THIS SCRIPT 3 TIMES TO GET THIS DATA
# CHANGING THE TS PARAMETER TO BE TS - 2 WEEKS and TS + 2 WEEKS FOR THE LAST 2 RUNS.
# collecting this data with 2 API keys took several days for one chunk, so I recommend running in parallel 
# with multiple bearer tokens if possible.

# get all of Jorts' tweets
jorts_tweets = pd.read_csv("jorts/jorts_all_tweets.tsv", sep="\t")

# original authors of tweets that Jorts RT'ed
rt_authors = jorts_tweets[jorts_tweets["retweeted"] != "none"][
    ["id", "retweeted_author_id", "created_at", "retweeted_handle"]
]
rt_authors = rt_authors[rt_authors["created_at"] != "None"]
rt_authors["created_at_ts"] = pd.to_datetime(rt_authors["created_at"])
rt_authors = rt_authors.groupby("retweeted_handle").min()
rt_authors["retweeted_handle"] = rt_authors.index

# original authors of tweets that Jorts QT'ed
qt_authors = jorts_tweets[jorts_tweets["quoted"] != "None"][
    ["id", "quoted_author_id", "created_at", "quoted_handle"]
]
qt_authors = qt_authors[qt_authors["created_at"] != "None"]
qt_authors["created_at_ts"] = pd.to_datetime(qt_authors["created_at"])
qt_authors = qt_authors.groupby("quoted_handle").min()
qt_authors["quoted_handle"] = qt_authors.index


# bearer_token = 'YOUR BEARER TOKEN HERE'


def parse_cursor(c: int) -> dt.datetime:
    # https://popzazzle.blogspot.com/2019/11/how-to-find-out-when-someone-followed-you-on-twitter.html
    if c == -1:
        return dt.datetime.now()
    if c == 0:
        return dt.datetime(2006, 1, 1, 0, 0, 0, 0)
    if c < -2:
        c = -c
    a = 90595920000000
    b = 1230427978203430000
    d = c - b
    e = d / a
    # to get d, we need to multiply by a.
    f = dt.datetime(2007, 3, 9, 7, 51, 0, 0)
    ts = (f + dt.timedelta(days=e)).astimezone(pytz.UTC)
    return ts  # subtract f from the unix timestamp;
    # we now have a dt.timedelta of e days.


def dt_to_cursor(ts: dt.datetime) -> int:
    """
    Does the opposite of parse_cursor (that is, transforms a dt into a Twitter API nanosecond cursor)
    """
    utc_ts = ts.astimezone(pytz.UTC)
    utc_ts_delta = utc_ts - dt.datetime(
        2007, 3, 9, 7, 51, 0, 0, pytz.UTC
    )  # timedelta of e days
    utc_unix_days = utc_ts_delta.days
    a = 90595920000000
    b = 1230427978203430000
    d = utc_unix_days * a
    c = d + b

    return c


day_unix_nanoseconds = 24 * 60 * 60 * (10**9)


def get_all_followers(screen_name):
    # gets all followers of a given screen_name 
    # partitioned by cursor so you'll have rough time bounds
    # on who followed when
    data = {}
    res_count = 0

    cursor = dt_to_cursor(dt.datetime.now())
    while True:
        try:
            r = requests.get(
                "https://api.twitter.com/1.1/followers/list.json?cursor={}&screen_name={}&skip_status=true&include_user_entities=false&count=200".format(
                    str(cursor), screen_name
                ),
                headers={"Authorization": "Bearer {}".format(bearer_token)},
            )
            res = r.json()
            next_cursor = res["next_cursor"]
            users = res["users"]
            data[cursor] = users
            if cursor == next_cursor:
                break
            cursor = next_cursor
            res_count += 1
            if res_count % 60 == 0:
                pickle.dump(
                    data,
                    open(
                        "{}_{}_follower_data_by_cursor.pkl".format(
                            str(res_count), screen_name
                        ),
                        "wb",
                    ),
                )
        except KeyError:
            # gnarly try/except to wait out the rate limit
            time.sleep(60 * 15)
    return data

# get all people who follow Jorts
# data = get_all_followers('JortsTheCat')
# pickle.dump(data, open('jorts_follower_data_by_cursor_all.pkl', 'wb'))


def followers_gotten_after_ts(screen_name, ts):
    """
    Get all the followers gotten after a particular timestamp
    and before that timestamp plus two weeks
    returns followers partitioned by cursor (the increments might be small
    for an account that got lots of followers in that time period and bigger
    for accounts with less following)
    """
    ts = ts.astimezone(pytz.UTC)
    cursor = dt_to_cursor(ts + dt.timedelta(weeks=2))
    print(screen_name)
    data = {}
    res_count = 0
    while parse_cursor(cursor).astimezone(pytz.UTC) > ts:
        print(res_count)
        try:
            r = requests.get(
                "https://api.twitter.com/1.1/followers/ids.json?cursor={}&screen_name={}&skip_status=true&include_user_entities=false&count=5000".format(
                    str(cursor), screen_name
                ),
                headers={"Authorization": "Bearer {}".format(bearer_token)},
            )
            res = r.json()
            if "next_cursor" in res:
                next_cursor = res["next_cursor"]
            users = res["ids"]
            data[cursor] = users
            if "next_cursor" not in res or cursor == next_cursor:
                break
            cursor = next_cursor
            res_count += 1
            if res_count % 60 == 0:
                pickle.dump(
                    data,
                    open(
                        "{}_{}_following_data_post_{}.pkl".format(
                            str(res_count),
                            screen_name,
                            dt.datetime.strftime(ts, "%Y%m%d"),
                        ),
                        "wb",
                    ),
                )
        except KeyError as e:
            print(res)
            print(e)
            if "error" in res and res["error"] == "Not authorized.":
                # give up - acct went private
                break
            elif (
                "errors" in res
                and len(res["errors"]) >= 1
                and "code" in res["errors"][0]
                and res["errors"][0]["code"] == 34
            ):
                # give up - acct was deleted
                print("oop")
                return data
            time.sleep(60 * 15)
    return data


# out of the people that follow Jorts at a particular time,
# how many of them then followed the person Jorts amplified?
# count_rt = 0
# use the line below if you have to stop the collection and restart it
# since the followers_gotten_after_ts function prints out the accounts it collects data for,
# you can use those screen names to index the authors you still have to collect data for.
# rt_authors = rt_authors[rt_authors['retweeted_handle'] > 'xunqianti']
# for handle, ts in zip(rt_authors['retweeted_handle'].tolist(), rt_authors['created_at_ts'].tolist()):
#     print(count_rt / len(rt_authors), ' rt')
#     count_rt += 1
#     try:
#         data = followers_gotten_after_ts(handle, ts)
#     except:
#         # don't write this to home dir - fills up fast.
#         pickle.dump(data, open('{}_following_data_post_{}_all.pkl'.format(handle, dt.datetime.strftime(ts, '%Y%m%d')), 'wb'))
#         break
#     print(handle)
#     pickle.dump(data, open('{}_following_data_post_{}_all.pkl'.format(handle, dt.datetime.strftime(ts, '%Y%m%d')), 'wb'))

count_qt = 0
qt_authors = qt_authors[qt_authors["quoted_handle"] > "purrrhica"]
for handle, ts in zip(
    qt_authors["quoted_handle"].tolist(), qt_authors["created_at_ts"].tolist()
):
    print(count_qt / len(qt_authors), " qt")
    count_qt += 1
    data = followers_gotten_after_ts(handle, ts)
    pickle.dump(
        data,
        open(
            "{}_following_data_post_{}_all_qt.pkl".format(
                handle, dt.datetime.strftime(ts, "%Y%m%d")
            ),
            "wb",
        ),
    )
