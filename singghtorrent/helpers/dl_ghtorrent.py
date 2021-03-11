import gzip
import json
import os
from calendar import Calendar
from datetime import date
from glob import glob
from multiprocessing.pool import Pool

import pandas as pd
import requests
import singghtorrent as sg
from tqdm import tqdm


def should_skip(date: str, stage: str = ""):
    """Check hierarchically if data is finished."""
    ext_path = sg.storage_external_root() / "ghtorrent/{}.json.gz".format(date)
    df_prc_path = sg.storage_interim_root() / "ghtorrent/{}-prc.parquet".format(date)
    df_cm_path = sg.storage_interim_root() / "ghtorrent/{}-cm.parquet".format(date)
    if os.path.exists(df_prc_path) and os.path.exists(df_cm_path):
        print("Already interimmed.")
        return True
    elif stage == "interim":
        return False
    if os.path.exists(ext_path):
        print("Already downloaded.")
        return True
    return False


def download_gh_event(date: str):
    """Download from ghtorrent.

    From: https://github.com/src-d/datasets/blob/master/ReviewComments/\
        PR_review_comments_generation.ipynb
    Date format in YYYY-MM-DD-hh
    Args:
        date (str): Date like 2021-01-01-0
    """
    url = "http://data.gharchive.org/{}.json.gz".format(date)
    saveurl = sg.storage_external_root() / "ghtorrent/{}.json.gz".format(date)
    if should_skip(date):
        return
    try:
        r = requests.get(url)
        with open(saveurl, "wb") as f:
            f.write(r.content)
    except Exception as e:
        print(e)
        download_gh_event(date)


def get_github_data(path: str) -> pd.DataFrame:
    """Get PR comments and commit messages from events."""
    COLUMNS = ["COMMENT_ID", "COMMIT_ID", "URL", "AUTHOR", "CREATED_AT", "BODY"]
    comments_list = []
    commits_list = []
    for line in tqdm(gzip.open(path).readlines()):
        event = json.loads(line)
        if event["type"] == "PullRequestReviewCommentEvent":
            comments_list.append(
                [
                    event["payload"]["comment"]["id"],
                    event["payload"]["comment"]["commit_id"],
                    event["payload"]["comment"]["html_url"],
                    event["payload"]["comment"]["user"]["login"],
                    event["payload"]["comment"]["created_at"],
                    event["payload"]["comment"]["body"],
                ]
            )
        if event["type"] == "PushEvent":
            commits_list += event["payload"]["commits"]
    pr_comments_df = pd.DataFrame(comments_list, columns=COLUMNS)
    commit_message_df = pd.DataFrame.from_records(commits_list).drop_duplicates(
        subset="sha"
    )
    if len(commit_message_df) > 0:
        commit_message_df = commit_message_df[["message", "url"]]
    return pr_comments_df, commit_message_df


def download_github_data(date: str):
    """Download and parse PR given YYYY-MM-DD-hh."""
    download_gh_event(date)
    if should_skip(date, "interim"):
        return
    ext_dl_path = sg.storage_external_root() / "ghtorrent/{}.json.gz".format(date)
    df_prc, df_cm = get_github_data(ext_dl_path)
    df_prc_path = sg.storage_interim_root() / "ghtorrent/{}-prc.parquet".format(date)
    df_prc.to_parquet(df_prc_path, index=0, compression="gzip")
    df_cm_path = sg.storage_interim_root() / "ghtorrent/{}-cm.parquet".format(date)
    df_cm.to_parquet(df_cm_path, index=0, compression="gzip")
    return


def delete_glob(globstr: str):
    """Delete files using glob."""
    for f in glob(globstr):
        os.remove(f)


def download_github_day(date: tuple):
    """Download by a full date (year, month, day)."""
    dates = generate_date_strs(date[0], date[1], date[2])
    date3 = "{}-{:02d}-{:02d}".format(date[0], date[1], date[2])
    proc_prc_path = (
        sg.storage_processed_root() / "pr_comments" / "{}-prc.parquet".format(date3)
    )
    cm_prc_path = (
        sg.storage_processed_root() / "commit_messages" / "{}-cm.parquet".format(date3)
    )
    if os.path.exists(proc_prc_path) and os.path.exists(cm_prc_path):
        delete_glob(str(sg.storage_interim_root() / "ghtorrent/{}-*".format(date3)))
        delete_glob(str(sg.storage_external_root() / "ghtorrent/{}-*".format(date3)))
        return "Already processed {}".format(date3)

    for d in dates:
        download_github_data(d)
        if d.split("-")[3] == "23":
            prc_paths = glob(
                str(sg.storage_interim_root() / "ghtorrent/{}-*-prc*".format(date3))
            )
            cm_paths = glob(
                str(sg.storage_interim_root() / "ghtorrent/{}-*-cm*".format(date3))
            )
            if len(prc_paths) == 24:
                df = pd.concat([pd.read_parquet(i) for i in prc_paths])
                df.to_parquet(proc_prc_path, index=0, compression="gzip")
            if len(cm_paths) == 24:
                df = pd.concat([pd.read_parquet(i) for i in cm_paths])
                df.to_parquet(cm_prc_path, index=0, compression="gzip")
    if os.path.exists(proc_prc_path) and os.path.exists(cm_prc_path):
        delete_glob(str(sg.storage_interim_root() / "ghtorrent/{}-*".format(date3)))
        delete_glob(str(sg.storage_external_root() / "ghtorrent/{}-*".format(date3)))
    print("Finished {}!".format(date))
    return


def generate_date_strs(year: int, month: int, day: int) -> str:
    """Automatically generate date strings."""
    return ["{}-{:02d}-{:02d}-{}".format(year, month, day, i) for i in range(24)]


def get_dates_for_year(year: int) -> list:
    """Return list of dates for given year."""
    early_stop = False
    dates = []
    today = date.today()
    now_year, now_month, now_day = today.year, today.month, today.day
    for m in range(1, 13):
        interim_dates = list(Calendar().itermonthdays3(year, m))
        interim_dates = [i for i in interim_dates if i[1] == m]
        processed_dates = []
        for d in interim_dates:
            if d[0] >= now_year and d[1] >= now_month and d[2] >= now_day:
                early_stop = True
                break
            processed_dates.append(d)
        dates += processed_dates
        if early_stop:
            return dates
    return dates


def download_pool_hours(year: str, month: str, day: str) -> pd.DataFrame:
    """Download data in parallel and return dataframe."""
    pool = Pool(4)
    dates = generate_date_strs(year, month, day)
    pr_comments_df = []
    commit_messages_df = []
    for result in pool.imap_unordered(download_github_data, dates):
        pr_comments_df.append(result[0])
        commit_messages_df.append(result[1])
    pool.close()
    pool.join()
    return pd.concat(pr_comments_df), pd.concat(commit_messages_df)
