import pandas as pd

import datetime as dt
import multiprocessing
import os
import pickle
import pytz
import requests
import time


def load_user_ids():
    """
    Loads a list of BLM activists' Twitter handles (screen names)
    """
    ids = []
    with open("blm_activists.txt", "r") as f:
        for line in f.readlines():
            ids.append(line.strip())
    return list(set(ids))


def load_new_follower_ids():
    """
    For each BLM activist, get the users who followed them between 5/25/2020 and 30 days after.
    Return strings of each user ID
    """
    ids = []
    original_user_ids = load_user_ids()
    for u in original_user_ids:
        # activist_$screen_name_$date.pkl - holds activist screen name mapped to list of info about ppl who followed them.
        d = pickle.load(
            open(
                "/net/data/twitter-bounded-following/george_floyd_follows/activist_{}_{}_full.pkl".format(
                    u, "20200525"
                ),
                "rb",
            )
        )
        following_folded = [vv for vv in [v for v in d.values()][0].values()]
        following = []
        for f1 in following_folded:
            for f2 in f1:
                following.append(f2)
        for f in following:
            ids.append(f["id_str"])
    return ids


def load_tokens():
    """
    Loads ~10 token sets as tuples from Kenny's collection of credentials.
    NOTE: DO NOT USE THESE TO COLLECT TWEETS - THESE ARE BEING USED FOR TWEET COLLECTION DAILY
    (following/follower collection is ok)
    """
    tokens = []
    fnames = os.listdir("REDACTED")
    for file in fnames:
        if file[0] == ".":
            continue
        with open(
            "REDACTED" + file,
            "r",
            encoding="ascii",
        ) as f:
            lines = [line for line in f.readlines()]
            tup = [l.strip() for l in lines[0].split(",")[-2:]]
            tup = tuple(tup)
            tokens.append(tup)
    return list(tokens)


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
    return f + dt.timedelta(days=e)  # subtract f from the unix timestamp;
    # we now have a dt.timedelta of e days.


def dt_to_cursor(ts: dt.datetime) -> int:
    """
    Maps datetime object to Twitter API nanosecond cursor (may be a little approximate)
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


def get_new_follows_of_user(
    user_id_and_token,
    ts=pd.to_datetime("20200525"),
    prefix="followedactivist",
    api_type="friends",
):
    """
    Given a user ID and a bearer token, get all the new follows of that person between ts and 30 days previous.
    (Twitter follower API goes backwards in time)
    user_id_and_token is a tuple of (user_id, token_pair).
    ts is a datetime object (I've just been changing the default values or within-function subtractions
    because passing arguments with multiprocessing is messy.
    prefix is a string that tells us what kind of pickle file (i've been writing a ton to disk) it is
    followedactivist = belongs to a user that followed one activist; this is all of their follows in the time period
    api_type: can be "friends" or "followers" - friends is people you follow, and followers are people that follow you.
    """
    token_pair = user_id_and_token[1]
    user_id = user_id_and_token[0]
    print(token_pair, user_id)
    ts = ts.tz_localize("UTC")
    cursor = dt_to_cursor(ts)
    data = {}
    # we don't go all the way back into the past
    stop_date = ts - dt.timedelta(days=30)
    stop_cursor = dt_to_cursor(stop_date)
    res_count = 0
    while cursor > stop_cursor:
        print("got here")
        try:
            r = requests.get(
                "https://api.twitter.com/1.1/{}/list.json?cursor={}&user_id={}&skip_status=true&include_user_entities=true&count=200".format(
                    api_type, str(cursor), str(user_id)
                ),
                headers={"Authorization": "Bearer {}".format(token_pair)},
            )
            res = r.json()
            next_cursor = res["next_cursor"]
            users = res["users"]
            data[cursor] = users
            cursor = next_cursor
            res_count += 1
            # checking if we've gone too far back in time or done something wrong
            if "next_cursor" not in res or cursor == next_cursor:
                break
            if res_count % 5 == 0:
                # incremental if someone has a ton of following/follower events
                pickle.dump(
                    data,
                    open(
                        "/net/data/twitter-bounded-following/george_floyd_follows/{}_{}_{}_following_data_post_{}.pkl".format(
                            prefix,
                            str(res_count),
                            str(user_id),
                            dt.datetime.strftime(ts, "%Y%m%d"),
                        ),
                        "wb",
                    ),
                )
        except KeyError as e:
            # if we don't have a next cursor or users item
            print(e)
            print(res)
            if "error" in res and res["error"] == "Not authorized.":
                break
                # not authorized = acct went private; we give up and return a blank entry
            elif (
                "errors" in res
                and len(res["errors"]) >= 1
                and "code" in res["errors"][0]
                and res["errors"][0]["code"] == 34
            ):
                # 34 means the page got deleted; not much we can do here either.
                pickle.dump(
                    {user_id: data},
                    open(
                        "/net/data/twitter-bounded-following/george_floyd_follows/{}_{}_{}_full.pkl".format(
                            prefix, str(user_id), dt.datetime.strftime(ts, "%Y%m%d")
                        ),
                        "wb",
                    ),
                )
                return
            time.sleep(60 * 15)
        except KeyboardInterrupt as e:
            print(e)
            pickle.dump(
                {user_id: data},
                open(
                    "/net/data/twitter-bounded-following/george_floyd_follows/{}_{}_{}_full.pkl".format(
                        prefix, str(user_id), dt.datetime.strftime(ts, "%Y%m%d")
                    ),
                    "wb",
                ),
            )
            # result_queue.put(data)
            return
    pickle.dump(
        {user_id: data},
        open(
            "/net/data/twitter-bounded-following/george_floyd_follows/{}_{}_{}_full.pkl".format(
                prefix, str(user_id), dt.datetime.strftime(ts, "%Y%m%d")
            ),
            "wb",
        ),
    )
    # note that this function just writes to disk and doesn't return data - this is intentional.
    # easier to pick up where we left off.
    return


if __name__ == "__main__":
    tokens = load_tokens()
    new_follower_ids = load_new_follower_ids()
    bearer_tokens = []
    user_ids_plus_tokens = []
    p = multiprocessing.Pool(8)
    for token in tokens:
        auth = (token[0], token[1])
        res = requests.post(
            "https://api.twitter.com/oauth2/token?grant_type=client_credentials",
            auth=auth,
        )
        bearer_token = res.json()["access_token"]
        bearer_tokens.append(bearer_token)
        # this generates our bearer tokens
    for i, user_id in enumerate(new_follower_ids):
        # each request we'll make gets a bearer token assigned and a user ID.
        # enqueueing and pulling from queues as tokens become available is a stretch goal for this
        # but the existing code works pretty well as is.
        user_ids_plus_tokens.append((user_id, bearer_tokens[i % len(bearer_tokens)]))
    p.map(get_new_follows_of_user, user_ids_plus_tokens)
    # notice we're not returning anything - just writing files to disk.
