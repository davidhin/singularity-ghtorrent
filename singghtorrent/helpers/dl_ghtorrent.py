import gzip
import json
import os
from calendar import Calendar
from datetime import date
from glob import glob

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
        print("Already interimmed {}.".format(date))
        return True
    elif stage == "interim":
        return False
    if os.path.exists(ext_path):
        print("Already downloaded {}.".format(date))
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
    r = requests.get(url)
    with open(saveurl, "wb") as f:
        f.write(r.content)


def get_github_data(path: str) -> tuple(pd.DataFrame, pd.DataFrame):
    """Get PR comments and commit messages from events.

    Args:
        path (str): Path as string, e.g. "/path/to/file"

    Returns:
        tuple(pd.DataFrame, pd.DataFrame): Dataframes representing
        commits and pull request comments.
    """
    COLUMNS = ["COMMENT_ID", "COMMIT_ID", "URL", "AUTHOR", "CREATED_AT", "BODY"]
    comments_list = []
    commits_list = []
    read_github_lines = gzip.open(path).readlines()
    for line in tqdm(read_github_lines):
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
    """Download and parse github data.

    Example:
        download_github_data("2021-1-1-0")
        download_github_data("2021-1-1-13")

    Args:
        date (str): Date in format "YYYY-MM-DD-h"
    """
    # Try and download
    while True:
        try:
            download_gh_event(date)
        except Exception as e:
            print(e)
            pass
        else:
            break

    # if should_skip(date, "interim"):
    #     return
    ext_dl_path = sg.storage_external_root() / "ghtorrent/{}.json.gz".format(date)

    # Try and get github data
    while True:
        try:
            df_prc, df_cm = get_github_data(ext_dl_path)
        except Exception as e:
            print(e)
            date4 = str(ext_dl_path).split("/")[-1].split(".")[0]
            delete_glob(str(sg.storage_external_root() / "ghtorrent/{}*".format(date4)))
            delete_glob(str(sg.storage_interim_root() / "ghtorrent/{}*".format(date4)))
            pass
        else:
            break

    # Save paths
    df_prc_path = sg.storage_interim_root() / "ghtorrent/{}-prc.parquet".format(date)
    df_cm_path = sg.storage_interim_root() / "ghtorrent/{}-cm.parquet".format(date)

    # Try and save to parquet
    while True:
        try:
            df_prc.to_parquet(df_prc_path, index=0, compression="gzip")
            df_cm.to_parquet(df_cm_path, index=0, compression="gzip")
            pd.read_parquet(df_prc_path)
            pd.read_parquet(df_cm_path)
        except Exception as e:
            print(e)
            pass
        else:
            break

    return


def delete_glob(globstr: str):
    """Delete files using glob.

    Args:
        globstr (str): "folder/file*"
    """
    for f in glob(globstr):
        os.remove(f)


def download_github_day(date: tuple[int, int, int]):
    """Download by a full date (year, month, day).

    Example:
        >>> download_github_day((2021, 1, 1))
        42%|████▏     | 73968/177742 [00:03<00:04, 20791.74it/s]

    Args:
        date (tuple[int, int, int]): Tuple representing year, month, day

    Returns:
        None: Outputs saved to storage/
    """
    # Format dates
    dates = generate_date_strs(date[0], date[1], date[2])
    date3 = "{}-{:02d}-{:02d}".format(date[0], date[1], date[2])

    # Generate paths
    spr = sg.storage_processed_root()
    sir = sg.storage_interim_root()
    proc_prc_path = spr / "pr_comments" / "{}-prc.parquet".format(date3)
    cm_prc_path = spr / "commit_messages" / "{}-cm.parquet".format(date3)

    # Skip if complete
    if os.path.exists(proc_prc_path) and os.path.exists(cm_prc_path):
        delete_glob(str(sg.storage_interim_root() / "ghtorrent/{}-*".format(date3)))
        delete_glob(str(sg.storage_external_root() / "ghtorrent/{}-*".format(date3)))
        return "Already processed {}".format(date3)

    # Download data for all hours in date
    for d in dates:
        download_github_data(d)
        if d.split("-")[3] == "23":
            prc_paths = glob(str(sir / "ghtorrent/{}-*-prc*".format(date3)))
            cm_paths = glob(str(sir / "ghtorrent/{}-*-cm*".format(date3)))
            if len(prc_paths) != 24 or len(cm_paths) != 24:
                print("Wrong number of files with {}. Restarting...".format(d))
                download_github_day(date)
                return
            df = pd.concat([pd.read_parquet(i) for i in prc_paths])
            df.to_parquet(proc_prc_path, index=0, compression="gzip")
            df = pd.concat([pd.read_parquet(i) for i in cm_paths])
            df.to_parquet(cm_prc_path, index=0, compression="gzip")

    # Delete unneeded external files
    if os.path.exists(proc_prc_path) and os.path.exists(cm_prc_path):
        delete_glob(str(sg.storage_interim_root() / "ghtorrent/{}-*".format(date3)))
        delete_glob(str(sg.storage_external_root() / "ghtorrent/{}-*".format(date3)))
    print("Finished {}!".format(date))
    return


def generate_date_strs(year: int, month: int, day: int) -> list:
    """Automatically generate date strings.

    Example:
        >>> generate_date_strs(2021, 1, 2)
        ['2021-01-02-0',
        '2021-01-02-1',
        '2021-01-02-2',
        '2021-01-02-3',...]

    Args:
        year (int): Year e.g. 2021
        month (int): Month e.g. 2
        day (int): Day e.g. 1

    Returns:
        list: list of dates as strings
    """
    return ["{}-{:02d}-{:02d}-{}".format(year, month, day, i) for i in range(24)]


def get_dates_for_year(year: int) -> list:
    """Get a list of dates given a year.

    Example:
        >>> get_dates_for_year(2021)
        [(2021, 1, 1),
        (2021, 1, 2),
        (2021, 1, 3),
        (2021, 1, 4), ...]

    Args:
        year (int): Year as integer, e.g. 2015

    Returns:
        list: List of years
    """
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
