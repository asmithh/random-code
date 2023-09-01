import pickle
import pywikibot

class WikiRevision(object):
    def __init__(
        self, text, comment, size, timestamp, user, previous_id=0, future_id=0
    ):
        self.text = text
        self.comment = comment
        self.size = size
        self.timestamp = timestamp
        self.user = user


# setting up pywikibot; you'll need to modify the page name and starttime/endtime based on your needs.
site = pywikibot.Site("en", "wikipedia")
page = pywikibot.Page(site, "Talk:Britney Spears")
starttime = pywikibot.Timestamp(year=2022, month=8, day=31)
endtime = pywikibot.Timestamp(year=2022, month=7, day=16)
revs = page.revisions(content=True, endtime=endtime, starttime=starttime) # this is your list of all the revisions of the page


revs_all = []
for idx, rev in enumerate(revs):
    try:
        current_rev = WikiRevision(
            rev.text,
            rev.comment,
            rev.size,
            rev.timestamp,
            rev.user,
            previous_id=idx,
            future_id=idx + 2,
        )
        # use this block if you want to write the full version files to .txt files. this will use a lot of space!!
        with open(f"./revisions/{rev.timestamp}_wiki_output.txt", "w") as f:
            f.write(current_rev.comment + "\n")
            f.write(current_rev.user + "\n")
            f.write(current_rev.text)
        revs_all.append(current_rev.__dict__)
    except Exception as e:
        # this should be rare!!!
        # this will print out the error; if you're getting a bunch of the same error that's probably bad.
        print('exception raised: ', e)
    print(rev.timestamp)

# dumping all our metadata -- this will be big!!
pickle.dump(revs_all, open("rev_metadata_wikipedia.pkl", "wb"))
